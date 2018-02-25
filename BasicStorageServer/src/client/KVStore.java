package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import common.HashUtil;
import common.messages.BasicKVMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import ecs.ECSNode;
import ecs.IECSNode;

/**
 * Provides the implementation for the client-side communications module.
 */
public class KVStore implements KVCommInterface {

	private static final Logger log = Logger.getLogger(KVStore.class);

	private static final int MAX_KEY_LENGTH = 20;
	private static final int MAX_VALUE_LENGTH = 120000;

	private Socket clientSocket;
	private OutputStream out;
	private InputStream in;

	private IECSNode currentServer;
	private final Map<String, IECSNode> serverMetadata;
	private boolean isConnected;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.currentServer = new ECSNode(address, port);
		this.serverMetadata = new HashMap<>();
		this.isConnected = false;
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
		if (clientSocket == null) {
			throw new IllegalArgumentException("Not currently connected to server");

		} else if (key == null || key.isEmpty() || key.length() > MAX_KEY_LENGTH
				|| key.trim().contains(" ") || key.trim().contains("\n")) {
			throw new IllegalArgumentException("Illegal <key> value");

		} else if (value != null && (value.length() > MAX_VALUE_LENGTH || value.trim().contains("\n"))) {
			throw new IllegalArgumentException("Illegal <value> value");
		}

		return sendMessage(new BasicKVMessage(key, value, StatusType.PUT));
	}

	@Override
	public KVMessage get(String key) throws Exception {
		if (clientSocket == null) {
			throw new IllegalArgumentException("Not currently connected to server");

		} else if (key == null || key.isEmpty() || key.length() > MAX_KEY_LENGTH
				|| key.trim().contains(" ") || key.trim().contains("\n")) {
			throw new IllegalArgumentException("Illegal <key> value");

		}
		return sendMessage(new BasicKVMessage(key, null, StatusType.GET));
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
		String keyHash = HashUtil.toMD5(message.getKey());
		KVMessage response = null;
		boolean gotRightServer = false;

		// keep contacting servers until the right one gets hit
		while (!gotRightServer) {
			boolean gotServerFromCache = false;

			// check if the current server is thought to be able to handle the request
			if (currentServer.containsHash(keyHash)) {
				gotServerFromCache = true;
			} else {
				// search cache for an appropriate server
				for (Map.Entry<String, IECSNode> entry : serverMetadata.entrySet()) {
					IECSNode server = entry.getValue();
					if (server.containsHash(keyHash)) {
						// TODO handle the log messages from disconnect() and connect() calls
						disconnect();
						currentServer = server;
						connect();
						gotServerFromCache = true;
						break;
					}
				}
			}

			BasicKVMessage.sendMessage(out, message);

			response = BasicKVMessage.receiveMessage(in);
			if (response.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
				gotRightServer = true;
			} else {
				// cached information for the selected server is stale; purge it from the cache
				if (gotServerFromCache) {
					serverMetadata.remove(currentServer.getNodeName());
				}

				// update metadata for new server
				IECSNode responsibleServer = response.getResponsibleServer();
				serverMetadata.put(responsibleServer.getNodeName(), responsibleServer);
				currentServer = responsibleServer;
			}
		}

		return response;
	}

}
