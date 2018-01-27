package app_kvServer.persistence;

/**
 * Provides the basic contract for classes which manipulate key-value based
 * persistences.
 */
public interface KVPersistenceManager {

	/**
	 * Checks whether the persistence contains the given key.
	 * 
	 * @param key The key to check
	 * @return <code>true</code> if the persistence contains an entry for the key,
	 *         <code>false</code> otherwise
	 */
	public boolean containsKey(String key);

	/**
	 * Retrieves the value associated with the given key from the persistence.
	 * 
	 * @param key The key to retrieve the value for
	 * @return The value associated with the key, or <code>null</code> if no such
	 *         value exists
	 */
	public String get(String key);

	/**
	 * Inserts or updates the given key-value pair in the persistence.
	 * 
	 * @param key The key to set
	 * @param value The corresponding value to set
	 * @return The previous value associated with the given key, or
	 *         <code>null</code> if no such entry existed
	 */
	public String put(String key, String value);

	/**
	 * Removes all key-value pairs from the persistence.
	 */
	public void clear();

}
