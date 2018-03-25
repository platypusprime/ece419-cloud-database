package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.KVServiceTopology;
import common.messages.BasicKVMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.StreamUtil;
import ecs.ECSNode;
import ecs.IECSNode;

/**
 * Provides the implementation for the client-side communications module.
 */
public class KVStore implements KVCommInterface {

	private static final Logger log = Logger.getLogger(KVStore.class);

	private static final int MAX_KEY_LENGTH = 20;
	private static final int MAX_VALUE_LENGTH = 120 * 1000;

	private Socket clientSocket;
	private OutputStream out;
	private InputStream in;

	private IECSNode currentServer;
	private boolean isConnected;

	private final KVServiceTopology mdCache;
	private final StreamUtil streamUtil;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.currentServer = new ECSNode(address, port);
		this.mdCache = new KVServiceTopology();
		mdCache.updateNode(currentServer);
		this.isConnected = false;
		this.streamUtil = new StreamUtil();
	}

	@Override
	public void connect() throws Exception {
		clientSocket = new Socket();
		clientSocket.connect(currentServer.getNodeSocketAddress());
		this.isConnected = true;

		out = clientSocket.getOutputStream();
		in = clientSocket.getInputStream();

		log.info("Connection established");
	}

	@Override
	public void disconnect() {
		log.info("try to close connection ...");

		try {
			tearDownConnection();
		} catch (IOException ioe) {
			log.error("Unable to close connection!");
		}

		this.isConnected = false;
	}

	@Override
	public boolean isConnected() {
		return isConnected;
	}

	private void tearDownConnection() throws IOException {
		log.info("tearing down the connection ...");
		if (clientSocket != null) {
			clientSocket.close();
			clientSocket = null;
			log.info("connection closed!");
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		if (clientSocket == null)
			throw new IllegalStateException("Not currently connected to server");

		validateKey(key);
		validateValue(value);
		return sendMessage(new BasicKVMessage(key, value, StatusType.PUT));
	}

	@Override
	public KVMessage get(String key) throws Exception {
		if (clientSocket == null)
			throw new IllegalStateException("Not currently connected to server");

		validateKey(key);
		return sendMessage(new BasicKVMessage(key, null, StatusType.GET));
	}

	/**
	 * Checks whether the given string satisfies the KV server key requirements.
	 * 
	 * @param key The key to check
	 * @throws IllegalArgumentException if the key:
	 *             <ul>
	 *             <li>is <code>null</code> or empty</li>
	 *             <li>exceeds the maximum key length</li>
	 *             <li>contains illegal characters</li>
	 *             </ul>
	 */
	public static void validateKey(String key) throws IllegalArgumentException {
		if (key == null || key.isEmpty())
			throw new IllegalArgumentException("Null or missing key");

		if (key.length() > MAX_KEY_LENGTH)
			throw new IllegalArgumentException("Key exceeds the maximum length");

		String keyTrim = key.trim();
		if (keyTrim.contains(" ") || keyTrim.contains("\n"))
			throw new IllegalArgumentException("Key contains illegal whitespace (space or newline)");
	}

	/**
	 * Checks whether the given string satisfies the KV server value requirements.
	 * 
	 * @param value The value to check
	 * @throws IllegalArgumentException if the value:
	 *             <ul>
	 *             <li>exceeds the maximum value length</li>
	 *             <li>contains illegal characters</li>
	 *             </ul>
	 */
	public static void validateValue(String value) throws IllegalArgumentException {
		// allow null values for deletion purposes
		if (value == null || value.isEmpty())
			return;

		if (value.length() > MAX_VALUE_LENGTH)
			throw new IllegalArgumentException("Value exceeds the maximum length");

		if (value.trim().contains("\n"))
			throw new IllegalArgumentException("Value contains illegal whitespace (newline)");
	}

	/**
	 * Sends the specified <code>put</code> or <code>get</code> message to the
	 * currently connected server, retrying transparently if the wrong server is
	 * contacted.
	 * 
	 * @param message The <code>put</code> or <code>get</code> message to send
	 * @return The final server response
	 * @throws Exception If an error occurs during forming the request or
	 *             communicating with the server
	 */
	private KVMessage sendMessage(KVMessage message) throws Exception {
		String responseStr;
		KVMessage response = null;
		boolean gotRightServer = false;

		// keep contacting servers until the right one gets hit
		while (!gotRightServer) {

			// TODO handle case where server rejects request or is offline
			streamUtil.sendMessage(out, message);

			responseStr = streamUtil.receiveString(in);
			response = streamUtil.deserializeKVMessage(responseStr);

			if (response.getStatus() == StatusType.SERVER_NOT_RESPONSIBLE) {
				// cached information for the selected server is stale; purge it from the cache
				mdCache.invalidateNode(currentServer);

				// update metadata for new server
				IECSNode responsibleServer = response.getResponsibleServer();
				mdCache.updateNode(responsibleServer);
				
				disconnect();
				currentServer = responsibleServer;
				connect();

			} else {
				gotRightServer = true;
			}
		}

		return response;
	}

}
