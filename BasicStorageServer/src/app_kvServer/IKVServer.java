package app_kvServer;

/**
 * Provides the basic contract for the server application.
 */
public interface IKVServer {

	/** Contains the various cache strategies available to the server. */
	public enum CacheStrategy {
		/** No cache. */
		None,
		/** Least-recently used. */
		LRU,
		/** Least-frequently used. */
		LFU,
		/** First-in-first-out. */
		FIFO
	};

	/**
	 * Get the port number of the server
	 * 
	 * @return port number
	 */
	public int getPort();

	/**
	 * Get the hostname of the server
	 * 
	 * @return hostname of server
	 */
	public String getHostname();

	/**
	 * Get the cache strategy of the server
	 * 
	 * @return cache strategy
	 */
	public CacheStrategy getCacheStrategy();

	/**
	 * Get the cache size
	 * 
	 * @return cache size
	 */
	public int getCacheSize();

	/**
	 * Check if key is in storage. NOTE: does not modify any other properties
	 * 
	 * @param key The key to check
	 * @return true if key in storage, false otherwise
	 */
	public boolean inStorage(String key);

	/**
	 * Check if key is in storage. NOTE: does not modify any other properties
	 * 
	 * @param key The key to check
	 * @return true if key in storage, false otherwise
	 */
	public boolean inCache(String key);

	/**
	 * Get the value associated with the key
	 * 
	 * @param key The key to retrieve value for
	 * @return value associated with key
	 * @throws Exception when key not in the key range of the server
	 */
	public String getKV(String key) throws Exception;

	/**
	 * Put the key-value pair into storage
	 * 
	 * @param key The key to set
	 * @param value The value to set. If this is <code>null</code> or the empty
	 *            string, the existing entry for the key will be deleted.
	 * @throws Exception when key not in the key range of the server
	 */
	public void putKV(String key, String value) throws Exception;

	/**
	 * Clear the local cache of the server
	 */
	public void clearCache();

	/**
	 * Clear the storage of the server
	 */
	public void clearStorage();

	/**
	 * Abruptly stop the server without any additional actions NOTE: this includes
	 * performing saving to storage
	 */
	public void kill();

	/**
	 * Gracefully stop the server, can perform any additional actions
	 */
	public void close();

}
