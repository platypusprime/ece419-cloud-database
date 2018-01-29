package app_kvServer.cache;

import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.persistence.KVPersistenceManager;

public interface KVCacheManager {

	public CacheStrategy getCacheStrategy();

	public void setPersistenceManager(KVPersistenceManager persistenceManager);

	public void setCacheSize(int size);

	public int getCacheSize();

	public boolean containsKey(String key);

	public String get(String key);

	public String put(String key, String value);

	public void clear();

}
