package app_kvServer.cache;

import java.util.AbstractMap.SimpleEntry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer.CacheStrategy;

/**
 * A cache implementing the FIFO strategy. Keeps track of the usage frequency of
 * keys and chooses the least frequently used key for eviction.
 */
public class LfuCache extends AbstractCache {

	private static Logger log = Logger.getLogger(KVCache.class);

	private Map<String, FrequencyKeyPair> keys;
	private SortedSet<FrequencyKeyPair> usages;

	/**
	 * Creates a LFU cache with an initial capacity of 0.
	 */
	public LfuCache() {
		usages = new TreeSet<>();
		keys = new HashMap<>();
		log.info("Created LFU cache");
	}

	@Override
	public CacheStrategy getCacheStrategy() {
		return CacheStrategy.LFU;
	}

	@Override
	protected void registerUsage(String key) {
		if (keys.containsKey(key)) {
			FrequencyKeyPair fkp = keys.get(key);
			FrequencyKeyPair newFkp = fkp.incrementUsages();
			keys.put(key, newFkp);
			usages.remove(fkp);
			usages.add(newFkp);
		} else {
			FrequencyKeyPair fkp = new FrequencyKeyPair(key, 1);
			keys.put(key, fkp);
			usages.add(fkp);
		}

		log.debug("Recorded usage for key: " + key);
	}

	@Override
	protected Entry<String, String> evict() {
		Iterator<FrequencyKeyPair> iterator = usages.iterator();
		if (iterator.hasNext()) {
			FrequencyKeyPair lfuFkp = iterator.next();
			String lfuKey = lfuFkp.key;
			String lfuValue = removeKey(lfuKey);
			usages.remove(lfuFkp);
			keys.remove(lfuKey);

			return new SimpleEntry<>(lfuKey, lfuValue);

		} else {
			return null;
		}
	}

	@Override
	public void clear() {
		super.clear();
		usages.clear();
		keys.clear();
	}

	/**
	 * An immutable object holding a key and its number of occurrences.
	 */
	private class FrequencyKeyPair implements Comparable<FrequencyKeyPair> {
		private final String key;
		private final int frequency;

		/**
		 * Creates a new FK pair.
		 * 
		 * @param key The key to set
		 * @param frequency The occurrences to set
		 */
		public FrequencyKeyPair(String key, int frequency) {
			this.key = key;
			this.frequency = frequency;
		}

		/**
		 * Creates a new FK pair with the same key and frequency increased by one.
		 * 
		 * @return An incremented FK pair
		 */
		public FrequencyKeyPair incrementUsages() {
			return new FrequencyKeyPair(key, frequency + 1);
		}

		@Override
		public int compareTo(FrequencyKeyPair o) {
			int diff = this.frequency - o.frequency;
			if (diff == 0) diff = this.key.compareTo(o.key);
			return diff;
		}
	}

}
