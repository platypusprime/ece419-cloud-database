package app_kvServer.cache;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import app_kvServer.persistence.KVPersistenceManager;

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

	protected abstract void registerUsage(String key);

	protected abstract Map.Entry<String, String> evict();

	protected String removeKey(String key) {
		return data.remove(key);
	}
}
