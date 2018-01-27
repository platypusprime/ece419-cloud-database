package app_kvServer.persistence;

public interface KVPersistenceManager{
	public boolean containsKey(String key);

	public String get(String key);

	public String put(String key, String value);

	public void clear();

}
