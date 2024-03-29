package app_kvServer.persistence;

import java.util.Map;

/**
 * Provides the basic contract for classes which manipulate key-value based
 * persistences.
 */
public interface KVPersistence {

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
	 * Retrieves all key-value pairs currently in the storage
	 * 
	 * @return Map of key-value pairs
	 * @deprecated Use {@link #chunkator()} instead.
	 */
	@Deprecated
	public Map<String, String> getAll();

	/**
	 * Retrieves a new chunk-based iterator for batch access to key-value pairs in
	 * this persistence.
	 * 
	 * @return A new iterator
	 */
	public KVPersistenceChunkator chunkator();

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
	 * Inserts all key-value pairs from the given map into the persistent storage
	 * 
	 * @param pairs The pairs to insert
	 * @return <code>true</code> if all keys were successfully added,
	 *         <code>false</code> otherwise
	 */
	public boolean insertAll(Map<String, String> pairs);

	/**
	 * Removes all key-value pairs from the persistence.
	 */
	public void clear();

	/**
	 * Removes all key-value pairs in the given range from the persistence.
	 * 
	 * @param hashRange The key hash range to clear
	 */
	public void clearRange(String[] hashRange);

}
