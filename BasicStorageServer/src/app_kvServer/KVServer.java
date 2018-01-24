package app_kvServer;

import app_kvServer.cache.FifoCacheManager;
import app_kvServer.cache.KVCacheManager;
import app_kvServer.cache.LfuCacheManager;
import app_kvServer.cache.LruCacheManager;
import app_kvServer.cache.NoCacheManager;
import app_kvServer.persistence.KVPersistenceManager;

public class KVServer implements IKVServer {

	private final int port;
	private final KVCacheManager cacheManager;
	private final KVPersistenceManager persistenceManager;

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

		// set up storage
		persistenceManager = null; // TODO new AsyncPersistenceManager();

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
	}

	@Override
	public int getPort() {
		return port;
	}

	@Override
	public String getHostname() {
		// TODO Auto-generated method stub
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
		System.exit(1);
	}

	@Override
	public void close() {
		// TODO Auto-generated method stub
	}
}
