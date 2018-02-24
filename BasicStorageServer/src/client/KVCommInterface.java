package client;

import common.messages.KVMessage;

/**
 * Provides a high-level contract for communications with the KV server
 * application.
 * 
 * @see KVStore
 */
public interface KVCommInterface {

	/**
	 * Establishes a connection to the configured KV server. Does nothing if already
	 * connected to a server.
	 *
	 * @throws Exception If a connection could not be established
	 */
	public void connect() throws Exception;

	/**
	 * Disconnects the client from the currently connected KV server. Does nothing
	 * if not currently connected to a server.
	 */
	public void disconnect();

	/**
	 * Tests whether this store is currently connected to a server.
	 * 
	 * @return <code>true</code> if connected to server,
	 *         <code>false</code> otherwise
	 */
	public boolean isConnected();

	/**
	 * Issues an insert, update, or delete request to the connected KV server.
	 *
	 * @param key The key to put a value for
	 * @param value The value to associate with the given key. If empty or
	 *            <code>null</code>, a deletion is requested instead.
	 * @return The server response, which describes the result of the request and
	 *         any errors that may have occurred
	 * @throws Exception If the request could not be sent
	 *             (e.g. not connected to a server).
	 */
	public KVMessage put(String key, String value) throws Exception;

	/**
	 * Issues a retrieve request to the connected KV server.
	 *
	 * @param key The key to retrieve the value for
	 * @return the value, which is indexed by the given key.
	 * @throws Exception If the request could not be sent
	 *             (e.g. not connected to a server).
	 */
	public KVMessage get(String key) throws Exception;

}
