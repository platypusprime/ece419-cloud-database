package app_kvServer.cache;

import app_kvServer.IKVServer.CacheStrategy;

/**
 * Provides a contract for key-value cache systems.
 */
public interface KVCache {

	/**
	 * Returns the replacement strategy implemented by this cache.
	 * 
	 * @return The cache strategy
	 */
	public CacheStrategy getCacheStrategy();

	/**
	 * Returns this cache's current capacity.
	 * 
	 * @return The cache size
	 */
	public int getCacheSize();

	/**
	 * Resizes this cache to the specified size.
	 * 
	 * @param size The size to set
	 * @throws IllegalArgumentException If a negative size is provided
	 */
	public void setCacheSize(int size) throws IllegalArgumentException;

	/**
	 * Checks whether the specified key currently exists in the cache.
	 * Implementations of this method should not affect the cache data.
	 * 
	 * @param key The key to check
	 * @return <code>true</code> if this cache currently contains a key-value entry
	 *         associated with the specified key, <code>false</code> otherwise
	 */
	public boolean containsKey(String key);

	/**
	 * Retrieves the corresponding value for the specified key, if that entry
	 * currently exists in this cache.
	 * 
	 * @param key The key to retrieve the value for
	 * @return The associated value, or <code>null</code> if no such entry exists
	 */
	public String get(String key);

	/**
	 * Adds, updates, or deletes the specified key-value pair for this cache.
	 * 
	 * @param key The key to put
	 * @param value The value to associate with the key. If this is
	 *            <code>null</code>, the entry for the specified key is removed
	 * @return the previous value associated with key, or <code>null</code> if no
	 *         such value existed
	 */
	public String put(String key, String value);

	/**
	 * Clears all key-value entries from this cache.
	 */
	public void clear();

}
