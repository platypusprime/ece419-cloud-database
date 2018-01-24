package app_kvServer.cache;

import app_kvServer.IKVServer.CacheStrategy;

public interface KVCacheManager {

	public CacheStrategy getCacheStrategy();

	public void setCacheSize(int size);

	public int getCacheSize();

	public boolean containsKey(String key);

	public String get(String key);

	public String getQuietly(String key);

	public String put(String key, String value);

	public void clear();

}
