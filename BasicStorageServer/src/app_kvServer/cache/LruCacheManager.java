package app_kvServer.cache;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map.Entry;

import app_kvServer.IKVServer.CacheStrategy;

import java.util.Set;

public class LruCacheManager extends AbstractCacheManager {

	private Set<String> keys = new LinkedHashSet<>();

	@Override
	public CacheStrategy getCacheStrategy() {
		return CacheStrategy.LFU;
	}

	@Override
	protected void registerUsage(String key) {
		if (keys.contains(key)) {
			keys.remove(key);
		}

		keys.add(key);
	}

	@Override
	protected Entry<String, String> evict() {
		Iterator<String> iterator = keys.iterator();
		if (iterator.hasNext()) {
			String oldestKey = iterator.next();

			keys.remove(oldestKey);
			String value = removeKey(oldestKey);
			return new AbstractMap.SimpleEntry<>(oldestKey, value);
		}

		return null;
	}

	@Override
	public void clear() {
		super.clear();
		keys.clear();
	}

}
