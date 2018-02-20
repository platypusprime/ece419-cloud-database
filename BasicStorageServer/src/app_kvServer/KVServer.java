package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.cache.FifoCacheManager;
import app_kvServer.cache.KVCacheManager;
import app_kvServer.cache.LfuCacheManager;
import app_kvServer.cache.LruCacheManager;
import app_kvServer.persistence.FilePersistenceManager;
import app_kvServer.persistence.KVPersistenceManager;
import logger.LogSetup;

/**
 * The implementation of server functionality defined in {@link IKVServer}.
 * Contains an entry point for the server application.
 */
public class KVServer implements IKVServer, Runnable {

	private static final Logger log = Logger.getLogger(KVServer.class);

	private static final String SERVER_CONSOLE_PATTERN = "KVServer> %m%n";

	private final int port;
	private final KVCacheManager cacheManager;
	private final KVPersistenceManager persistenceManager;

	private ServerSocket serverSocket;
	private List<ClientConnection> clients = new ArrayList<>(); // TODO handle de-registering clients

	/**
	 * Main entry point for the echo server application.
	 * 
	 * @param args contains the port number at args[0], cache size at args[1], and
	 *            cache strategy at args[2].
	 */
	public static void main(String[] args) {
		try {
			LogSetup.initialize("logs/server.log", Level.INFO, SERVER_CONSOLE_PATTERN);
			if (args.length == 3) {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				new KVServer(port, cacheSize, strategy);
			} else {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: KVServer <port> <cacheSize> <cacheStrategy>");
			}
	
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
	
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument(s) <port> and/or <cacheSize>! Not a number!");
			System.out.println("Usage: Server <port> <cacheSize> <cacheStrategy>");
			System.exit(1);
		}
	}

	/**
	 * Start KV Server with selected name
	 * 
	 * @param name unique name of server
	 * @param zkHostname hostname where zookeeper is running
	 * @param zkPort port where zookeeper is running
	 */
	public KVServer(String name, String zkHostname, int zkPort) {
		// TODO Auto-generated method stub
		this(-1, -1, null);
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
		// set up cache
		switch (strategy) {
		case "FIFO":
			cacheManager = new FifoCacheManager();
			break;
		case "LRU":
			cacheManager = new LruCacheManager();
			break;
		case "LFU":
			cacheManager = new LfuCacheManager();
			break;
		default:
			cacheManager = null;
			log.warn("Invalid caching strategy \"" + strategy + "\"; using null cache");
			break;
		}

		Optional.ofNullable(cacheManager).ifPresent(cm -> cm.setCacheSize(cacheSize));

		// set up storage
		persistenceManager = new FilePersistenceManager();

		log.info("Created KVServer with "
				+ "port=" + port + ", "
				+ "cacheSize=" + cacheSize + ", "
				+ "strategy=" + strategy);

		this.port = port;

		// begin execution on new thread
		new Thread(this).start();
	}

	@Override
	public void run() {
		boolean isInit = initializeServer();
		if (!isInit) {
			log.fatal("Could not initialize server");
			return;
		}

		// main accept loop
		while (!serverSocket.isClosed()) {
			try {
				log.info("Listening for client connections...");
				Socket clientSocket = serverSocket.accept();
				ClientConnection connection = new ClientConnection(clientSocket, this);
				clients.add(connection);
				new Thread(connection).start();

				log.info("Connected to "
						+ clientSocket.getInetAddress().getHostName()
						+ " on port " + clientSocket.getPort());

			} catch (SocketTimeoutException e) {
				// do nothing
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
		return Optional.ofNullable(cacheManager)
				.map(KVCacheManager::getCacheStrategy)
				.orElse(CacheStrategy.None);
	}

	@Override
	public int getCacheSize() {
		return Optional.ofNullable(cacheManager)
				.map(KVCacheManager::getCacheSize)
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
		return persistenceManager.containsKey(key);
	}

	@Override
	public synchronized boolean inCache(String key) {
		return Optional.ofNullable(cacheManager)
				.map(cm -> cm.containsKey(key))
				.orElse(false);
	}

	@Override
	public synchronized String getKV(String key) throws Exception {
		return Optional.ofNullable(cacheManager)
				.map(cm -> cm.get(key))
				.orElseGet(() -> {
					String value = persistenceManager.get(key);
					cacheManager.put(key, value);
					return value;
				});
	}

	@Override
	public synchronized void putKV(String key, String value) throws Exception {
		putAndGetPrevKV(key, value);
	}

	@Override
	public synchronized String putAndGetPrevKV(String key, String value) throws Exception {
		Optional.ofNullable(cacheManager)
				.ifPresent(cm -> cm.put(key, value));
		return persistenceManager.put(key, value);
	}

	@Override
	public synchronized void clearCache() {
		Optional.ofNullable(cacheManager)
				.ifPresent(KVCacheManager::clear);
	}

	@Override
	public synchronized void clearStorage() {
		persistenceManager.clear();
	}

	@Override
	public void kill() {
		System.exit(1); // immediately shutdown the JVM
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
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void lockWrite() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void unlockWrite() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean moveData(String[] hashRange, String targetName) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}
}
