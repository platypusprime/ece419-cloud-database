package app_kvServer.cache;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;

import app_kvServer.IKVServer.CacheStrategy;

public class FifoCacheManager extends AbstractCacheManager {

	private Set<String> keys = new LinkedHashSet<>();

	@Override
	public CacheStrategy getCacheStrategy() {
		return CacheStrategy.FIFO;
	}

	@Override
	protected void registerUsage(String key) {
		// only care about first usage
		if (!keys.contains(key)) {
			keys.add(key);
		}
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
