package app_kvServer;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import app_kvServer.cache.FifoCacheManager;
import app_kvServer.cache.KVCacheManager;
import app_kvServer.cache.LfuCacheManager;
import app_kvServer.cache.LruCacheManager;
import app_kvServer.cache.NoCacheManager;
import app_kvServer.persistence.FilePersistenceManager;
import app_kvServer.persistence.KVPersistenceManager;
import logger.LogSetup;

/**
 * The implementation of server functionality defined in {@link IKVServer}.
 * Contains an entry point for the server application.
 */
public class KVServer implements IKVServer, Runnable {

	private static final Logger log = Logger.getLogger(KVServer.class);

	private static final String CONSOLE_PATTERN = "KVServer> %m%n";

	private final int port;
	private final KVCacheManager cacheManager;
	private final KVPersistenceManager persistenceManager;

	private ServerSocket serverSocket;
	private List<ClientConnection> clients = new ArrayList<>();
	private boolean isRunning;

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
			cacheManager = new NoCacheManager();
			break;
		}

		// set up storage
		persistenceManager = new FilePersistenceManager();
		cacheManager.setPersistenceManager(persistenceManager);
		cacheManager.setCacheSize(cacheSize);

		log.info("Created KVServer with "
				+ "port=" + port + ", "
				+ "cacheSize=" + cacheSize + ", "
				+ "strategy=" + strategy);

		this.port = port;

		new Thread(this).start();
	}

	@Override
	public void run() {
		isRunning = initializeServer();

		// Main accept loop
		while (serverSocket != null && isRunning()) {
			try {
				log.info("Listening for client connections...");
				Socket clientSocket = serverSocket.accept();
				ClientConnection connection = new ClientConnection(clientSocket, this);
				clients.add(connection);
				new Thread(connection).start();

				log.info("Connected to "
						+ clientSocket.getInetAddress().getHostName()
						+ " on port " + clientSocket.getPort());
			} catch (IOException e) {
				log.error("Error! " + "Unable to establish connection. \n", e);
			}
		}

		// shut down client connections
		for (ClientConnection client : clients) {
			try {
				client.close();
			} catch (IOException e) {
				log.error("Error! " + "Unable to close client connection. \n", e);
			}
		}

		log.info("Server stopped.");
	}

	private boolean isRunning() {
		return this.isRunning;
	}

	private boolean initializeServer() {
		log.info("initializing server on " + getHostname());
		try {
			serverSocket = new ServerSocket(port);
			log.info("Server listening on port: " + serverSocket.getLocalPort());
			return true;

		} catch (BindException e) {
			log.error("Port " + port + " is already bound");
		} catch (IOException e) {
			log.error("Cannot open server socket");
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
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return null;
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		return cacheManager.getCacheStrategy();
	}

	@Override
	public int getCacheSize() {
		return cacheManager.getCacheSize();
	}

	@Override
	public boolean inStorage(String key) {
		return persistenceManager.containsKey(key);
	}

	@Override
	public boolean inCache(String key) {
		return cacheManager.containsKey(key);
	}

	@Override
	public String getKV(String key) throws Exception {
		return cacheManager.get(key);
	}

	@Override
	public void putKV(String key, String value) throws Exception {
		cacheManager.put(key, value);
	}

	@Override
	public void clearCache() {
		cacheManager.clear();
	}

	@Override
	public void clearStorage() {
		persistenceManager.clear();
	}

	@Override
	public void kill() {
		System.exit(1); // immediately shutdown the JVM
	}

	@Override
	public void close() {
		// trigger run()'s epilogue
		isRunning = false;
		try {
			serverSocket.close();
		} catch (IOException e) {
			log.error("Error! " + "Unable to close server socket. \n", e);
		}
	}

	/**
	 * Main entry point for the echo server application.
	 * 
	 * @param args contains the port number at args[0], cache size at args[1], and
	 *            cache strategy at args[2].
	 */
	public static void main(String[] args) {
		try {
			LogSetup.initialize("logs/server.log", Level.INFO, CONSOLE_PATTERN);
			if (args.length != 3) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: KVServer <port> <cacheSize> <strategy>");
			} else {
				int port = Integer.parseInt(args[0]);
				int cacheSize = Integer.parseInt(args[1]);
				String strategy = args[2];
				new KVServer(port, cacheSize, strategy);
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);

		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port> <cache size> <cache strategy>");
			System.exit(1);
		}
	}
}
