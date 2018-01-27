package app_kvServer.cache;

import java.util.Map;

import app_kvServer.persistence.KVPersistenceManager;

public abstract class AbstractCacheManager implements KVCacheManager {

	private int size;
	private Map<String, String> data;
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
	public boolean containsKey(String key) {
		return data.containsKey(key);
	}

	@Override
	public String get(String key) {
		if (containsKey(key)) {
			registerUsage(key);
			return data.get(key);
		} else {
			String val = persistenceManager.get(key);
			if (val != null) updateCache(key, val);
			return val;
		}
	}

	@Override
	public String getQuietly(String key) {
		return data.get(key);
	}

	@Override
	public String put(String key, String value) {
		String oldVal = null;

		if (containsKey(key)) {
			oldVal = data.put(key, value);
			registerUsage(key);

		} else {
			updateCache(key, value);
		}
		// TODO:return status for persistenceManager as well
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
		data.clear();
	}

	protected abstract void registerUsage(String key);

	protected abstract Map.Entry<String, String> evict();

	protected String removeKey(String key) {
		return data.remove(key);
	}
}
