package app_kvServer.cache;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import app_kvServer.IKVServer.CacheStrategy;

/**
 * A cache implementing the FIFO strategy. Keeps track of the usage frequency of
 * keys and chooses the least frequently used key for eviction.
 */
public class LfuCacheManager extends AbstractCacheManager {

	private static Logger log = Logger.getLogger(KVCacheManager.class);
	
	private Map<String, Integer> usages = new HashMap<String, Integer>();

	/**
	 * Creates a LFU cache with an initial capacity of 0.
	 */
	public LfuCacheManager() {
		log.info("Created LFU cache manager");
	}
	
	@Override
	public CacheStrategy getCacheStrategy() {
		return CacheStrategy.LFU;
	}

	@Override
	protected void registerUsage(String key) {
		if (usages.containsKey(key)) {
			int oldUsages = usages.get(key);
			usages.put(key, oldUsages + 1);

		} else {
			usages.put(key, 1);
		}

		log.debug("Recorded usage for key: " + key);
	}

	@Override
	protected Entry<String, String> evict() {
		String lfuKey = usages.entrySet().stream()
				.min((entry1, entry2) -> entry1.getValue().compareTo(entry2.getValue()))
				.map(Map.Entry<String, Integer>::getKey)
				.orElse(null);

		usages.remove(lfuKey);
		String value = removeKey(lfuKey);
		
		log.debug("Evicted least frequently used key: " + lfuKey);
		
		return new AbstractMap.SimpleEntry<>(lfuKey, value);
	}

	@Override
	public void clear() {
		super.clear();
		usages.clear();
	}

}
