package app_kvServer.cache;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Implements common functionality for all cache manager classes. Exposes a
 * contract on which cache manager classes can implement their own replacement
 * strategies.
 */
public abstract class AbstractCacheManager implements KVCacheManager {

	private static Logger log = Logger.getLogger(KVCacheManager.class);

	private int size;
	private Map<String, String> data = new HashMap<>();
	private KVPersistenceManager persistenceManager;

	@Override
	public void setPersistenceManager(KVPersistenceManager persistenceManager) {
		this.persistenceManager = persistenceManager;
	}

	@Override
	public void setCacheSize(int size) {
		this.size = size;
		while (data.size() >= this.size) {
			evict();
		}
	}

	@Override
	public int getCacheSize() {
		return size;
	}

	@Override
	public synchronized boolean containsKey(String key) {
		return data.containsKey(key);
	}

	@Override
	public synchronized String get(String key) {
		log.info("Looking up key '" + key + "' in cache...");

		if (containsKey(key)) {
			registerUsage(key);
			String value = data.get(key);
			log.info("Value found in cache for key '" + key + "': '" + value + "'");
			return value;

		} else {
			String val = persistenceManager.get(key);
			if (val != null) updateCache(key, val);
			return val;
		}
	}

	@Override
	public synchronized String put(String key, String value) {
		if (value == null) {
			persistenceManager.put(key, value);
			return removeKey(key);
		}
		
		String oldVal = null;

		if (containsKey(key)) {
			oldVal = data.put(key, value);
			registerUsage(key);

		} else {
			updateCache(key, value);
		}

		persistenceManager.put(key, value);
		return oldVal;
	}

	protected void updateCache(String key, String value) {
		while (data.size() >= size) {
			evict();
		}

		data.put(key, value);
		registerUsage(key);
	}

	@Override
	public void clear() {
		log.info("Clearing cache");
		data.clear();
	}

	/**
	 * Records an instance of usage for the specified key. Should be called whenever
	 * a successful get or put operation is completed.
	 * 
	 * @param key The key to record usage for
	 */
	protected abstract void registerUsage(String key);

	/**
	 * Evicts a single record from this cache, based on the cache strategy.
	 * 
	 * @return The removed key-value entry
	 */
	protected abstract Map.Entry<String, String> evict();

	/**
	 * Removes the key-value entry associated with the specified key.
	 * 
	 * @param key The key to remove
	 * @return The value previously associated with the key, or <code>null</code> if
	 *         no such mapping existed
	 */
	protected String removeKey(String key) {
		return data.remove(key);
	}
}
