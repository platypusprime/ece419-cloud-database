package testing.app_kvServer.persistence;

import java.util.HashMap;
import java.util.Map;

import app_kvServer.persistence.KVPersistence;

/**
 * This class simulates a key-value persistence layer in memory for testing
 * purposes. Wraps a {@link HashMap} to handle storage and retrieval of
 * key-value entries.
 */
public class MemoryPersistence implements KVPersistence {

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

	@Override
	public Map<String, String> getAll() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean insertAll(Map<String, String> pairs) {
		// TODO Auto-generated method stub
		return false;
	}

}
