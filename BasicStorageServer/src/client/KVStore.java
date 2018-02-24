package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

import common.HashUtil;
import common.messages.BasicKVMessage;
import common.messages.HashRange;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

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

	private InetSocketAddress currentServer;
	private final Map<HashRange, InetSocketAddress> serverMetadata;
	private boolean isConnected;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.currentServer = new InetSocketAddress(address, port);
		this.serverMetadata = new HashMap<>();
		this.isConnected = false;
	}

	@Override
	public void connect() throws Exception {
		clientSocket = new Socket();
		clientSocket.connect(currentServer);
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
		String keyHash;
		KVMessage response = null;
		boolean gotRightServer = false;

		if (clientSocket == null) {
			throw new IllegalArgumentException("Not currently connected to server");

		} else if (key == null || key.isEmpty() || key.length() > MAX_KEY_LENGTH
				|| key.trim().contains(" ") || key.trim().contains("\n")) {
			throw new IllegalArgumentException("Illegal <key> value");

		} else if (value.length() > MAX_VALUE_LENGTH || value.trim().contains("\n")) {
			throw new IllegalArgumentException("Illegal <value> value");
		}

		keyHash = HashUtil.toMD5(key);

		// keep contacting servers until the right one gets hit
		while (!gotRightServer) {

			// check which server is responsible for the key
			InetSocketAddress server = serverMetadata.entrySet().stream()
					.filter(entry -> entry.getKey().containsHash(keyHash))
					.findFirst()
					.map(Map.Entry::getValue).get();

			// swap servers if necessary
			if (!server.equals(currentServer)) {
				// TODO deal with the log messages
				disconnect();
				currentServer = server;
				connect();
			}

			KVMessage putMessage = new BasicKVMessage(key, value, StatusType.PUT);
			BasicKVMessage.sendMessage(out, putMessage);

			response = BasicKVMessage.receiveMessage(in);
			if (response.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
				gotRightServer = true;
			} else {
				// update metadata
				serverMetadata.clear();
				serverMetadata.putAll(response.getHashRanges());
			}
		}

		return response;
	}

	@Override
	public KVMessage get(String key) throws Exception {
		String keyHash;
		KVMessage response = null;
		boolean gotRightServer = false;

		if (clientSocket == null) {
			throw new IllegalArgumentException("Not currently connected to server");

		} else if (key == null || key.isEmpty() || key.length() > MAX_KEY_LENGTH
				|| key.trim().contains(" ") || key.trim().contains("\n")) {
			throw new IllegalArgumentException("Illegal <key> value");

		}

		keyHash = HashUtil.toMD5(key);

		// keep contacting servers until the right one gets hit
		while (!gotRightServer) {

			// check which server is responsible for the key
			InetSocketAddress server = serverMetadata.entrySet().stream()
					.filter(entry -> entry.getKey().containsHash(keyHash))
					.findFirst()
					.map(Map.Entry::getValue).get();

			// swap servers if necessary
			if (!server.equals(currentServer)) {
				// TODO deal with the log messages
				disconnect();
				currentServer = server;
				connect();
			}

			KVMessage getMessage = new BasicKVMessage(key, null, StatusType.GET);
			BasicKVMessage.sendMessage(out, getMessage);

			response = BasicKVMessage.receiveMessage(in);
			if (response.getStatus() != StatusType.SERVER_NOT_RESPONSIBLE) {
				gotRightServer = true;
			} else {
				// update metadata
				serverMetadata.clear();
				serverMetadata.putAll(response.getHashRanges());
			}
		}

		return response;
	}

}
