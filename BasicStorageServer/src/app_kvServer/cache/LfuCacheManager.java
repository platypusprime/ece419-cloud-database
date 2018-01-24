package app_kvServer.cache;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import app_kvServer.IKVServer.CacheStrategy;

public class LfuCacheManager extends AbstractCacheManager {

	private Map<String, Integer> usages = new HashMap<String, Integer>();

	@Override
	public CacheStrategy getCacheStrategy() {
		return CacheStrategy.LFU;
	}

	@Override
	protected void registerUsage(String key) {
		if (usages.containsKey(key)) {
			int oldUsages = usages.get(key);
			usages.put(key, oldUsages++);

		} else {
			usages.put(key, 1);
		}
	}

	@Override
	protected Entry<String, String> evict() {
		String lfuKey = usages.entrySet().stream()
				.min((entry1, entry2) -> entry1.getValue().compareTo(entry2.getValue()))
				.map(Map.Entry<String, Integer>::getKey)
				.orElse(null);

		usages.remove(lfuKey);
		String value = removeKey(lfuKey);
		return new AbstractMap.SimpleEntry<>(lfuKey, value);
	}

	@Override
	public void clear() {
		super.clear();
		usages.clear();
	}

}
