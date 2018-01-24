package app_kvServer.cache;

import java.util.Map.Entry;

import app_kvServer.IKVServer.CacheStrategy;

public class NoCacheManager extends AbstractCacheManager {

	@Override
	public CacheStrategy getCacheStrategy() {
		return CacheStrategy.None;
	}

	@Override
	protected void registerUsage(String key) {
		// TODO Auto-generated method stub
	}

	@Override
	protected Entry<String, String> evict() {
		// TODO Auto-generated method stub
		return null;
	}

}
