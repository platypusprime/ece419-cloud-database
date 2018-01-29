package app_kvServer.cache;

import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.persistence.KVPersistenceManager;

public class NoCacheManager implements KVCacheManager {

	private KVPersistenceManager persistence;

	@Override
	public CacheStrategy getCacheStrategy() {
		return CacheStrategy.None;
	}

	@Override
	public void setPersistenceManager(KVPersistenceManager persistenceManager) {
		this.persistence = persistenceManager;
	}

	@Override
	public void setCacheSize(int size) {}

	@Override
	public int getCacheSize() {
		return 0;
	}

	@Override
	public boolean containsKey(String key) {
		return false;
	}

	@Override
	public String get(String key) {
		return persistence.get(key);
	}

	@Override
	public String put(String key, String value) {
		return persistence.put(key, value);
	}

	@Override
	public void clear() {
		persistence.clear();
	}

}
