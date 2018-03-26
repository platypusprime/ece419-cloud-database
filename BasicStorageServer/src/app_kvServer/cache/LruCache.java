package app_kvServer.cache;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer.CacheStrategy;

import java.util.Set;

/**
 * A cache implementing the LRU strategy. Keeps track of the insertion order of
 * keys and treats usages as new insertions. Chooses the oldest key for
 * eviction.
 */
public class LruCache extends AbstractCache {

	private static Logger log = Logger.getLogger(KVCache.class);

	private Set<String> keys = new LinkedHashSet<>();

	/**
	 * Creates a LRU cache with an initial capacity of 0.
	 */
	public LruCache() {
		log.info("Created LRU cache");
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		return CacheStrategy.LRU;
	}

	@Override
	protected void registerUsage(String key) {
		if (keys.contains(key)) {
			keys.remove(key);
		}

		keys.add(key);

		log.info("Recorded usage for key: " + key);
	}

	@Override
	protected Entry<String, String> evict() {
		Iterator<String> iterator = keys.iterator();
		if (iterator.hasNext()) {
			String lruKey = iterator.next();

			keys.remove(lruKey);
			String value = removeKey(lruKey);

			log.debug("Evicted least recently used key: " + lruKey);

			return new AbstractMap.SimpleEntry<>(lruKey, value);
		}

		return null;
	}

	@Override
	public void clear() {
		super.clear();
		keys.clear();
	}

}
