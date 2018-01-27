package app_kvServer;

import app_kvServer.cache.FifoCacheManager;
import app_kvServer.cache.KVCacheManager;
import app_kvServer.cache.LfuCacheManager;
import app_kvServer.cache.LruCacheManager;
import app_kvServer.cache.NoCacheManager;
import app_kvServer.persistence.KVPersistenceManager;

import java.net.InetAddress;
import java.net.UnknownHostException;

import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.IOException;

import logging.LogSetup;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
public class KVServer implements IKVServer {

	private final int port;
	private final KVCacheManager cacheManager;
	private final KVPersistenceManager persistenceManager;

	private static Logger logger = Logger.getRootLogger();
	private ServerSocket serverSocket;
	private boolean running;
	/**
	 * Start KV Server at given port
	 * 
	 * @param port given port for storage server to operate
	 * @param cacheSize specifies how many key-value pairs the server is allowed to
	 *            keep in-memory
	 * @param strategy specifies the cache replacement strategy in case the cache is
	 *            full and there is a GET- or PUT-request on a key that is currently
	 *            not contained in the cache. Options are "FIFO", "LRU", and "LFU".
	 */
	public KVServer(int port, int cacheSize, String strategy) {
		this.port = port;
		// TODO set up communications
		running = initializeServer();
		// set up storage
		persistenceManager = new KVPersistenceManager(); // TODO new AsyncPersistenceManager();

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
		
		cacheManager.setPersistenceManager(persistenceManager);

		// Main accept loop
		if(serverSocket != null) {
			while(isRunning()){
				try {
					Socket client = serverSocket.accept();
					ClientConnection connection =
							new ClientConnection(client);
					new Thread(connection).start();

					logger.info("Connected to "
							+ client.getInetAddress().getHostName()
							+  " on port " + client.getPort());
				} catch (IOException e) {
					logger.error("Error! " +
							"Unable to establish connection. \n", e);
				}
			}
		}
		logger.info("Server stopped.");
	}

	private boolean isRunning() {
		return this.running;
	}

	private boolean initializeServer() {
		logger.info("Initialize server ...");
		try {
			serverSocket = new ServerSocket(port);
			logger.info("Server listening on port: "
					+ serverSocket.getLocalPort());
			return true;

		} catch (IOException e) {
			logger.error("Error! Cannot open server socket:");
			if(e instanceof BindException){
				logger.error("Port " + port + " is already bound!");
			}
			return false;
		}
	}
	@Override
	public int getPort() {
		return port;
	}

	@Override
	public String getHostname() {
		ip = InetAddress.getLocalHost();
		hostname = ip.getHostName();
		return hostname;
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
		System.exit(1);
	}

	@Override
	public void close() {
	}

	// NOT SURE IF MAIN IS NEEDED
	/**
	 * Main entry point for the echo server application.
	 * @param args contains the port number at args[0].
	 */
	public static void main(String[] args) {
		try {
			new LogSetup("logs/server.log", Level.ALL);
			if(args.length != 1) {
				System.out.println("Error! Invalid number of arguments!");
				System.out.println("Usage: Server <port>!");
			} else {
				int port = Integer.parseInt(args[0]);
				new Server(port).start();
			}
		} catch (IOException e) {
			System.out.println("Error! Unable to initialize logger!");
			e.printStackTrace();
			System.exit(1);
		} catch (NumberFormatException nfe) {
			System.out.println("Error! Invalid argument <port>! Not a number!");
			System.out.println("Usage: Server <port>!");
			System.exit(1);
		}
	}
}
