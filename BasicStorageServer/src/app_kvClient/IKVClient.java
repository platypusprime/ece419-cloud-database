package app_kvClient;

import client.KVCommInterface;

/**
 * Provides a contract for interfacing with the KV client functionality.
 */
public interface IKVClient {

	/**
	 * Creates a new connection at the specified address.
	 * 
	 * @param hostname The hostname to connect to
	 * @param port The port to connect to
	 * @throws Exception If a connection to the server can not be established
	 */
	public void newConnection(String hostname, int port) throws Exception;

	/**
	 * Returns the client-side communications module currently associated with this
	 * class. Used to interact with the KV server.
	 * 
	 * @return The KV store object
	 */
	public KVCommInterface getStore();

}
