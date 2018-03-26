package app_kvServer.cache;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import java.util.Set;

import app_kvServer.IKVServer.CacheStrategy;

/**
 * A cache implementing the FIFO strategy. Keeps track of the insertion order of
 * keys and chooses the oldest key for eviction.
 */
public class FifoCache extends AbstractCache {

	private static Logger log = Logger.getLogger(KVCache.class);

	private Set<String> keys = new LinkedHashSet<>();

	/**
	 * Creates a FIFO cache with an initial capacity of 0.
	 */
	public FifoCache() {
		log.info("Created FIFO cache");
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		return CacheStrategy.FIFO;
	}

	@Override
	protected void registerUsage(String key) {
		// only care about first usage
		if (!keys.contains(key)) {
			keys.add(key);
			log.debug("Recorded first usage for key: " + key);
		}
	}

	@Override
	protected Entry<String, String> evict() {
		Iterator<String> iterator = keys.iterator();
		if (iterator.hasNext()) {
			String oldestKey = iterator.next();

			keys.remove(oldestKey);
			String value = removeKey(oldestKey);

			log.debug("Evicted oldest key: " + oldestKey);

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
