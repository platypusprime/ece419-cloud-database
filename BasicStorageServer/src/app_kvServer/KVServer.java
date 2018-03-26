package app_kvServer;

import static common.zookeeper.ZKPathUtil.KV_SERVICE_STATUS_NODE;
import static common.zookeeper.ZKSession.FINISHED;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
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
import java.util.Optional;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import app_kvServer.cache.FifoCache;
import app_kvServer.cache.KVCache;
import app_kvServer.cache.LfuCache;
import app_kvServer.cache.LruCache;
import app_kvServer.migration.MigrationMessage;
import app_kvServer.migration.MigrationWatcher;
import app_kvServer.persistence.FilePersistence;
import app_kvServer.persistence.KVPersistence;
import app_kvServer.persistence.KVPersistenceChunkator;
import common.HashUtil;
import common.KVServiceTopology;
import common.zookeeper.ChangeNotificationWatcher;
import common.zookeeper.ZKPathUtil;
import common.zookeeper.ZKSession;
import ecs.IECSNode;
import logger.LogSetup;

/**
 * The implementation of server functionality defined in {@link IKVServer}.
 * Contains an entry point for the server application.
 */
public class KVServer implements IKVServer, Runnable {

	private static final Logger log = Logger.getLogger(KVServer.class);

	private static final String PERSISTENCE_FILENAME_FORMAT = "persistence/%s-data.txt";

	private static final int HEARTBEAT_INTERVAL = 1000;

	private final int port;
	private final KVCache cache;
	private final KVPersistence persistence;

	/** Contains values for possible server states. */
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
	// TODO handle de-registering clients
	private List<ClientConnection> clients = new ArrayList<>();

	private final String name;
	private final ZKSession zkSession;

	private IECSNode config = null;
	private KVServiceTopology serviceConfig;
	private ServiceStatusWatcher serviceStatusWatcher = null;
	private Thread heartbeatThread;

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
	 * Starts a KV server with the given name and ZooKeeper configuration. All
	 * additional configuration is obtained from ZooKeeper, as provided by the ECS.
	 * 
	 * @param name Unique name of server
	 * @param zkHostname Hostname where ZooKeeper is running
	 * @param zkPort Port where ZooKeeper is running
	 * @throws InterruptedException
	 * @throws KeeperException
	 */
	public KVServer(String name, String zkHostname, int zkPort) throws KeeperException, InterruptedException {
		log.info("KVServer " + name + " initializing");

		if (name == null || name.isEmpty()) {
			throw new IllegalArgumentException("Cannot instantiate server with missing name");
		}
		this.name = name;
		this.zkSession = new ZKSession(zkHostname, zkPort);
		this.serviceStatusWatcher = new ServiceStatusWatcher(this, zkSession);

		// Attempt to retrieve startup information from ZooKeeper
		/* NOTE: the ECS is blocked from making topology changes until the server is
		 * fully set up so setting the watcher here should be safe */
		log.debug("Server " + name + " retrieving service configuration for initialization");
		Collection<IECSNode> nodes = zkSession.getMetadataNodeData(new ServiceTopologyWatcher(this, zkSession));
		setServiceConfig(nodes);

		// Set fields based on config
		if (this.config != null) {
			this.port = this.config.getNodePort();

			// set up cache
			String cacheStrategy = this.config.getCacheStrategy();
			int cacheSize = this.config.getCacheSize();

			this.cache = chooseCache(cacheStrategy, cacheSize);

			// set up storage
			String persistenceFilename = String.format(PERSISTENCE_FILENAME_FORMAT, this.name);
			this.persistence = new FilePersistence(persistenceFilename);

			log.info("Created KVServer " + name + " with "
					+ "port=" + port + ", "
					+ "cacheSize=" + cacheSize + ", "
					+ "strategy=" + cacheStrategy);

			// begin execution on new thread
			new Thread(this).start();

		} else {
			throw new IllegalStateException("Could not find server's config in service configuration znode");
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
		this.zkSession = null;

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
			log.debug("Unknown caching strategy \"" + cacheStrategy + "\"; using null cache");
			break;
		}
		Optional.ofNullable(cache).ifPresent(cm -> cm.setCacheSize(cacheSize));

		return cache;
	}

	@Override
	public void run() {
		// Bind listening port
		boolean isBound = bindServer();
		if (!isBound) {
			log.fatal("Could not bind server " + name + " to address " + getHostname() + ":" + getPort());
			return;
		}

		// Setup sync for server status with KV service global status
		if (serviceStatusWatcher != null) {
			try {
				zkSession.getNodeData(KV_SERVICE_STATUS_NODE, serviceStatusWatcher);
			} catch (KeeperException | InterruptedException e) {
				log.error("Exception while setting up service status watcher", e);
			}
		}

		// Send notification of initialization success to ECS
		try {
			notifyEcs();
		} catch (NullPointerException | KeeperException | InterruptedException e) {
			log.warn("Exception while attempting to notify ECS", e);
		}

		// Start heartbeat
		initializeHeartbeat();

		// Initialize migration watcher and perform initial migration
		MigrationWatcher migrationWatcher = new MigrationWatcher(this, zkSession);
		migrationWatcher.process(null);

		// main accept loop
		while (!serverSocket.isClosed()) {
			try {
				log.trace("Listening for client connections...");
				Socket clientSocket = serverSocket.accept();
				ClientConnection connection = new ClientConnection(clientSocket, this);
				clients.add(connection);
				new Thread(connection).start();

				log.info("Connected to "
						+ clientSocket.getInetAddress().getHostName()
						+ " on port " + clientSocket.getPort());

			} catch (SocketTimeoutException e) {
				log.trace("Accept timed out");
			} catch (IOException e) {
				if (!serverSocket.isClosed()) {
					log.error("Error! " + "Unable to establish connection", e);
				} else {
					log.info("Socket closed; shutting down server " + name);
				}
				break;
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
		heartbeatThread.interrupt();

		log.info("Server " + name + " stopped.");
	}

	private boolean bindServer() {
		log.debug("Initializing server " + name + " on host " + getHostname());
		try {
			serverSocket = new ServerSocket(port);
			serverSocket.setSoTimeout(2000);
			log.debug("Server " + name + " bound to port: " + serverSocket.getLocalPort());
			return true;

		} catch (BindException e) {
			log.error("Port " + port + " is already bound", e);
		} catch (IOException e) {
			log.error("Cannot open server socket", e);
		}

		return false;
	}

	/**
	 * Uses this server's znode to communicate with the ECS (e.g. for sending
	 * notification of startup).
	 * 
	 * @throws KeeperException TODO
	 * @throws InterruptedException TODO
	 */
	private void notifyEcs() throws KeeperException, InterruptedException {
		String statusNode = ZKPathUtil.getStatusZnode(config);
		log.debug("Notifying ECS on znode " + statusNode);

		zkSession.updateNode(statusNode, FINISHED.getBytes(UTF_8));

		// wait for ECS to clear the notification
		log.debug("Waiting for ECS to clear notification on znode " + statusNode + "...");
		Object monitor = new Object();
		synchronized (monitor) {
			String status = zkSession.getNodeData(statusNode, new ChangeNotificationWatcher(monitor));
			if (status != null && !status.isEmpty()) {
				monitor.wait(15 * 1000);
			}

			status = zkSession.getNodeData(statusNode);
			if (status == null || status.isEmpty()) {
				log.debug(statusNode + " cleared");
			} else {
				log.warn("Server did not clear notification on " + statusNode + " within " + 15 * 1000 + " ms");
			}
		}

	}

	private void initializeHeartbeat() {
		log.debug("Initializing heartbeat thread for server " + name);

		this.heartbeatThread = new Thread() {
			public void run() {
				int heartbeatCounter = 0;
				while (!isInterrupted()) {
					try {
						// 50 here is arbitrary
						heartbeatCounter = (heartbeatCounter + 1) % 50;
						zkSession.updateNode(ZKPathUtil.getHeartbeatZnode(config),
								Integer.toString(heartbeatCounter));
						Thread.sleep(HEARTBEAT_INTERVAL);
					} catch (KeeperException e) {
						log.warn("Exception while updating heartbeat", e);
					} catch (InterruptedException e) {
						log.debug("Heartbeat thread interrupted; stopping heartbeat");
					}
				}
			}
		};
		heartbeatThread.start();
	}

	@Override
	public String getName() {
		return name;
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
				.orElseGet(() -> persistence.get(key));
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
		log.info("Opening server " + name + " to requests");
		status = isWriteLocked ? ServerStatus.WRITE_LOCKED : ServerStatus.RUNNING;
	}

	@Override
	public void stop() {
		log.info("Stopping server " + name + " for requests");
		status = ServerStatus.STOPPED;
	}

	@Override
	public void lockWrite() {
		log.info("Locking server " + name + " for writes");
		isWriteLocked = true;
		status = status == ServerStatus.STOPPED ? status : ServerStatus.WRITE_LOCKED;
	}

	@Override
	public void unlockWrite() {
		log.info("Unlocking server " + name + " for writes");
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
		log.info("Moving data in range [" + hashRange[0] + "," + hashRange[1] + "]"
				+ " from " + this.name + " to " + targetName);
		String targetNode;
		try {
			targetNode = zkSession.createMigrationZnode(this.name, targetName);
		} catch (KeeperException | InterruptedException e) {
			log.error("Could not create migration znode", e);
			return false;
		}

		try (KVPersistenceChunkator it = persistence.chunkator()) {

			while (it.hasNextChunk()) {
				Map<String, String> kvPairs = it
						.nextChunk(key -> HashUtil.containsHash(HashUtil.toMD5(key), hashRange));

				// Prepare message
				MigrationMessage message = new MigrationMessage(kvPairs);
				String data = message.toJSON();

				// Send message via znode
				log.debug("Sending chunk from " + this.name + " to " + targetName);
				zkSession.updateNode(targetNode, data);

				// Wait for target to consume
				log.debug("Waiting for receiver to consume data from " + targetNode);
				Object monitor = new Object();
				synchronized (monitor) {
					try {
						ChangeNotificationWatcher updateNotifier = new ChangeNotificationWatcher(monitor);
						String currentData = zkSession.getNodeData(targetNode, updateNotifier);
						while (currentData != null && !currentData.isEmpty()) {
							log.debug("Blocking until change on " + targetNode);
							monitor.wait(); // wait until the watcher gets an event
							currentData = zkSession.getNodeData(targetNode, updateNotifier);
						}
					} catch (KeeperException e) {
						log.warn("Exception while reading transfer znode", e);

					} catch (InterruptedException e) {
						log.warn("Interrupted while receiving data", e);
					}
				}

				String response = zkSession.getNodeData(targetNode);
				while (response != null && !response.isEmpty()) {
					response = zkSession.getNodeData(targetNode);
				}
			}

			// Signal transfer completion to target node
			log.debug("No more data to be transfered from " + this.name + " to " + targetName + ";"
					+ " signalling completion");
			zkSession.updateNode(targetNode, FINISHED);

			// Busy-wait for node deletion
			boolean deleted = !zkSession.checkNodeExists(targetNode, null);
			while (!deleted) {
				deleted = !zkSession.checkNodeExists(targetNode, null);
			}

			// Remove sent data from own persistence
			persistence.clearRange(hashRange);
			log.info("Data transfer completed. Deleted from self.");

		} catch (KeeperException | InterruptedException e) {
			log.error("Error while transferring data to " + targetName, e);
			return false;

		} catch (IOException e) {
			log.error("IO exception while reading persistence", e);
			return false;
		}

		return true;
	}

	/**
	 * Returns the most recent metadata for this server.
	 * 
	 * @return This server's configuration
	 */
	public IECSNode getServerConfig() {
		return config;
	}

	/**
	 * Retrieves the most recent metadata for the entire service.
	 * 
	 * @return The service topology
	 */
	public KVServiceTopology getServiceConfig() {
		return this.serviceConfig;
	}

	/**
	 * Updates this server's cached service metadata with the given information.
	 * 
	 * @param nodes The updated node metadata for participating servers
	 */
	public void setServiceConfig(Collection<IECSNode> nodes) {
		log.debug("Updating service configuration for server " + name);

		this.serviceConfig = new KVServiceTopology(nodes);
		IECSNode newConfig = this.serviceConfig.getNodeOfName(name);

		if (newConfig != null) {
			this.config = newConfig;
			log.trace("Updating server configuration for server " + name);
		} else {
			log.debug("Server " + name + " no longer exists in the topology");
		}
	}

}
