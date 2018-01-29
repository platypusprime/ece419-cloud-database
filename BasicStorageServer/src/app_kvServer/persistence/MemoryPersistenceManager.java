package app_kvServer.persistence;

import java.util.HashMap;
import java.util.Map;

public class MemoryPersistenceManager implements KVPersistenceManager {

	private Map<String, String> data = new HashMap<>();

	@Override
	public boolean containsKey(String key) {
		return data.containsKey(key);
	}

	@Override
	public String get(String key) {
		return data.get(key);
	}

	@Override
	public String put(String key, String value) {
		return data.put(key, value);
	}

	@Override
	public void clear() {
		data.clear();
	}

}
