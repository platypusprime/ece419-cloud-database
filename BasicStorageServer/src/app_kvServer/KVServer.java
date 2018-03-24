package app_kvServer;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import app_kvServer.cache.FifoCache;
import app_kvServer.cache.KVCache;
import app_kvServer.cache.LfuCache;
import app_kvServer.cache.LruCache;
import app_kvServer.migration.MigrationManager;
import app_kvServer.persistence.FilePersistence;
import app_kvServer.persistence.KVPersistence;
import common.zookeeper.ZKWrapper;
import ecs.IECSNode;
import logger.LogSetup;

/**
 * The implementation of server functionality defined in {@link IKVServer}.
 * Contains an entry point for the server application.
 */
public class KVServer implements IKVServer, Runnable {

	private static final Logger log = Logger.getLogger(KVServer.class);

	private final int port;
	private final KVCache cache;
	private final KVPersistence persistence;

	public static enum ServerStatus {
		/** Server running and serving all requests normally */
		RUNNING,
		/** Server is stopped, no requests are to be processed */
		STOPPED,
		/** Server locked for write, only get requests are served */
		WRITE_LOCKED
	}

	private ServerStatus status = ServerStatus.STOPPED;
	private boolean isWriteLocked = false;

	private ServerSocket serverSocket;
	private List<ClientConnection> clients = new ArrayList<>(); // TODO handle de-registering clients

	private final String name;
	private final ZKWrapper zkWrapper;

	private IECSNode config = null;
	private NavigableMap<String, IECSNode> hashring = new TreeMap<>();

	/**
	 * Main entry point for the key-value server application.
	 * 
	 * @param args Contains the server name at args[0], ZooKeeper hostname at
	 *            args[1], and ZooKeeper port at args[2].
	 */
	public static void main(String[] args) {
		try {
			if (args.length == 3) {
				String name = args[0];
				String zkHostname = args[1];
				int zkPort = Integer.parseInt(args[2]);

				String jarpath = new File(KVServer.class.getProtectionDomain().getCodeSource()
						.getLocation().toURI().getPath()).getParent();
				LogSetup.initialize(jarpath + "/logs/" + args[0] + ".log", Level.INFO);
				log.info("Initializing server with arguments name=" + name
						+ " zkHostname=" + zkHostname
						+ " zkPort=" + zkPort);
				new KVServer(name, zkHostname, zkPort);
			} else {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: KVServer <serverName> <zkHostname> <zkPort>");
			}

		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);

		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid <zkPort>! Not a number!");
			System.out.println("Usage: KVServer <serverName> <zkHostname> <zkPort>");
			System.exit(1);

		} catch (KeeperException | InterruptedException e) {
			System.out.println("Error! Could not instantiate KVServer!");
			e.printStackTrace();
		} catch (URISyntaxException e) {
			System.out.println("Error! Could not identify the jar directory");
			e.printStackTrace();
		}
	}

	/**
	 * Start KV Server with selected name
	 * 
	 * @param name Unique name of server
	 * @param zkHostname Hostname where ZooKeeper is running
	 * @param zkPort Port where ZooKeeper is running
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public KVServer(String name, String zkHostname, int zkPort) throws KeeperException, InterruptedException {
		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException("Cannot instantiate server with missing name");
		}
		this.name = name;
		this.zkWrapper = new ZKWrapper(zkHostname, zkPort);

		// attempt to retrieve startup information from ZooKeeper
		while (true) { // TODO replace infinite loop
			Collection<IECSNode> nodes = zkWrapper.getMetadataNodeData(new ServiceTopologyWatcher(this, zkWrapper));

			setCachedMetadata(nodes);

			for (IECSNode node : nodes) {
				if (this.name.equals(node.getNodeName())) {
					this.config = node;

					this.port = node.getNodePort();

					// set up cache
					String cacheStrategy = node.getCacheStrategy();
					int cacheSize = node.getCacheSize();

					this.cache = chooseCache(cacheStrategy, cacheSize);

					// set up storage
					String storageIdentifier = this.name + "-data.csv";
					this.persistence = new FilePersistence(storageIdentifier);

					log.info("Created KVServer with "
							+ "port=" + port + ", "
							+ "cacheSize=" + cacheSize + ", "
							+ "strategy=" + cacheStrategy);

					// begin execution on new thread
					new Thread(this).start();
					return;
				}
			}
		}
	}

	/**
	 * Creates and starts KV Server at given port.
	 * 
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed to
	 *            keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache is
	 *            full and there is a GET- or PUT-request on a key that is currently
	 *            not contained in the cache. Options are "FIFO", "LRU", and "LFU".
	 */
	@Deprecated
	public KVServer(int port, int cacheSize, String strategy) {

		this.port = port;

		// set up cache
		this.cache = chooseCache(strategy, cacheSize);

		// set up storage
		String storageIdentifier = "Server " + String.valueOf(port) + ".csv";
		this.persistence = new FilePersistence(storageIdentifier);

		log.info("Created KVServer with "
				+ "port=" + port + ", "
				+ "cacheSize=" + cacheSize + ", "
				+ "strategy=" + strategy);

		// unused fields (M2)
		this.name = null;
		this.zkWrapper = null;

		this.start();

		// begin execution on new thread
		new Thread(this).start();
	}

	private KVCache chooseCache(String cacheStrategy, int cacheSize) {
		KVCache cache;

		switch (cacheStrategy) {
		case "FIFO":
			cache = new FifoCache();
			break;
		case "LRU":
			cache = new LruCache();
			break;
		case "LFU":
			cache = new LfuCache();
			break;
		default:
			cache = null;
			log.warn("Invalid caching strategy \"" + cacheStrategy + "\"; using null cache");
			break;
		}
		Optional.ofNullable(cache).ifPresent(cm -> cm.setCacheSize(cacheSize));

		return cache;
	}

	@Override
	public void run() {
		boolean isInit = initializeServer();
		if (!isInit) {
			log.fatal("Could not initialize server");
			return;
		}

		// Check if any data is waiting to be migrated from other nodes
		readMigrationNode();

		// Send acknowledgement to ECS
		notifyECS();
		
		// Setup sync for server status with KV service global status
		syncServerStatus();

		// main accept loop
		while (!serverSocket.isClosed()) {
			try {
				log.debug("Listening for client connections...");
				Socket clientSocket = serverSocket.accept();
				ClientConnection connection = new ClientConnection(clientSocket, this);
				clients.add(connection);
				new Thread(connection).start();

				log.info("Connected to "
						+ clientSocket.getInetAddress().getHostName()
						+ " on port " + clientSocket.getPort());

			} catch (SocketTimeoutException e) {
				log.debug("Accept timed out");
			} catch (IOException e) {
				log.error("Error! " + "Unable to establish connection", e);
			}
		}

		// shut down client connections
		for (ClientConnection client : clients) {
			try {
				client.close();
			} catch (IOException e) {
				log.error("Unable to close client connection", e);
			}
		}

		log.info("Server stopped.");
	}

	private boolean initializeServer() {
		log.info("Initializing server on " + getHostname());
		try {
			serverSocket = new ServerSocket(port);
			log.info("Server listening on port: " + serverSocket.getLocalPort());
			serverSocket.setSoTimeout(2000);
			return true;

		} catch (BindException e) {
			log.error("Port " + port + " is already bound", e);
		} catch (IOException e) {
			log.error("Cannot open server socket", e);
		}

		return false;
	}

	/**
	 * Uses this server's ZNode to communicate with the ECS (e.g. for sending
	 * notification of startup).
	 */
	protected void notifyECS() {
		try {
			zkWrapper.updateNode(config.getBaseNodePath(), "FINISHED".getBytes("UTF-8"));
		} catch (NullPointerException | UnsupportedEncodingException | KeeperException | InterruptedException e) {
			log.warn("Exception while attempting to notify ECS", e);
		}
	}

    private void readMigrationNode() {
        List<String> childNodes = zkWrapper.getChildNodes(config.getMigrationNodePath(), new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                if (watchedEvent.getType() == EventType.NodeChildrenChanged) {
                    readMigrationNode();
                }
            }
        });

        if (childNodes == null) {
            log.warn("Child nodes for " + config.getMigrationNodePath() + " is NULL");
            return;
        }

        // Receive migrated data from other servers
        MigrationManager.receiveData(childNodes, zkWrapper, this);
    }

	private void syncServerStatus() {
		try {
			String kvStatus = zkWrapper.getNodeData(ZKWrapper.KV_SERVICE_STATUS_NODE, new Watcher() {

				@Override
				public void process(WatchedEvent event) {
					if (event.getType() == EventType.NodeDataChanged) {
						syncServerStatus();
					}

                    // Shutdown server if its node has been deleted
					else if (event.getType() == EventType.NodeDeleted) {
					    kill();
                    }
				}
			});
			
			if (kvStatus.equals(ZKWrapper.RUNNING_STATUS)) {
				this.status = ServerStatus.RUNNING;
			}
			else if (kvStatus.equals(ZKWrapper.STOPPED_STATUS)) {
				this.status = ServerStatus.STOPPED;
			}
			
			log.info("Server status changed to: " + kvStatus);
			
		} catch (KeeperException | InterruptedException e) {
			log.warn("Exception while checking KV Service status", e);
		}
	}

	@Override
	public int getPort() {
		return port;
	}

	@Override
	public String getHostname() {
		try {
			InetAddress ip = InetAddress.getLocalHost();
			String hostname = ip.getHostName();
			return hostname;

		} catch (UnknownHostException e) {
			log.error("IP address of host could not be determined", e);
		}

		return null;
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		return Optional.ofNullable(cache)
				.map(KVCache::getCacheStrategy)
				.orElse(CacheStrategy.None);
	}

	@Override
	public int getCacheSize() {
		return Optional.ofNullable(cache)
				.map(KVCache::getCacheSize)
				.orElse(0);
	}

	@Override
	public void registerClientConnection(ClientConnection client) {
		this.clients.add(client);
	}

	@Override
	public void deregisterClientConnection(ClientConnection client) {
		this.clients.remove(client);
	}

	@Override
	public synchronized boolean inStorage(String key) {
		return persistence.containsKey(key);
	}

	@Override
	public synchronized boolean inCache(String key) {
		return Optional.ofNullable(cache)
				.map(cm -> cm.containsKey(key))
				.orElse(false);
	}

	@Override
	public synchronized String getKV(String key) throws Exception {
		return Optional.ofNullable(cache)
				.map(cm -> cm.get(key))
				.orElseGet(() -> {
					String value = persistence.get(key);
					cache.put(key, value);
					return value;
				});
	}

	public synchronized Map<String, String> getAllKV() {
	    return persistence.getAll();
    }

	@Override
	public synchronized void putKV(String key, String value) {
		putAndGetPrevKV(key, value);
	}

	@Override
	public synchronized String putAndGetPrevKV(String key, String value) {
		Optional.ofNullable(cache)
				.ifPresent(cm -> cm.put(key, value));
		return persistence.put(key, value);
	}

	@Override
	public synchronized void clearCache() {
		Optional.ofNullable(cache)
				.ifPresent(KVCache::clear);
	}

	@Override
	public synchronized void clearStorage() {
		persistence.clear();
	}

	@Override
	public void kill() {
		System.exit(0); // immediately shutdown the JVM
	}

	@Override
	public void close() {
		try {
			serverSocket.close(); // causes a SocketException, triggering run()'s epilogue
		} catch (IOException e) {
			log.error("Unable to close server socket", e);
		}
	}

	@Override
	public void start() {
		status = isWriteLocked ? ServerStatus.WRITE_LOCKED : ServerStatus.RUNNING;
	}

	@Override
	public void stop() {
		status = ServerStatus.STOPPED;
	}

	@Override
	public void lockWrite() {
		isWriteLocked = true;
		status = status == ServerStatus.STOPPED ? status : ServerStatus.WRITE_LOCKED;
	}

	@Override
	public void unlockWrite() {
		isWriteLocked = false;
		status = status == ServerStatus.STOPPED ? status : ServerStatus.RUNNING;
	}

	/**
	 * Returns the current status of this server.
	 * 
	 * @return The status
	 * @see ServerStatus
	 */
	public ServerStatus getStatus() {
		return status;
	}

	@Override
	public boolean moveData(String[] hashRange, String targetName) {
	    return MigrationManager.sendData(hashRange, targetName, zkWrapper, this);
	}

	public NavigableMap<String, IECSNode> getCachedMetadata() {
		return hashring;
	}

	public void setCachedMetadata(Collection<IECSNode> nodes) {
		hashring = new TreeMap<>();
		nodes.forEach(node -> hashring.put(node.getNodeHashRangeStart(), node));
		
		for (IECSNode node : nodes) {
			if (this.name.equals(node.getNodeName())) {
				this.config = node;
			}
		}
	}

	public IECSNode getCurrentConfig() {
		return config;
	}

	public IECSNode findSuccessor() {
		return findSuccessor(config.getNodeHashRangeStart());
	}

	public IECSNode findSuccessor(String hash) {
		return Optional.ofNullable(hashring.higherEntry(hash))
				.orElse(hashring.firstEntry())
				.getValue();
	}

	public IECSNode findPredecessor() {
		return findPredecessor(config.getNodeHashRangeStart());
	}

	public IECSNode findPredecessor(String hash) {
		return Optional.ofNullable(hashring.lowerEntry(hash))
				.orElse(hashring.lastEntry())
				.getValue();
	}
	
	/**
	 * Identifies the server which is responsible for the given hash, given the
	 * information available to this cache.
	 * 
	 * @param keyHash The MD5 hash for the key to find a server for
	 * @return The server matching the key,
	 *         or <code>null</code> if the cache is empty
	 */
	public IECSNode findServer(String keyHash) {

		if (hashring.isEmpty())
			return null;

		return Optional
				// find the first server past the key in the hash ring
				.ofNullable(hashring.ceilingEntry(keyHash))
				.map(Map.Entry::getValue)

				// otherwise use the first server (guaranteed to exist because of above check)
				.orElse(hashring.firstEntry().getValue());
	}
}
