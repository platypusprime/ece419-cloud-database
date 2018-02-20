package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.messages.BasicKVMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.SerializableKVMessage;

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

	private final String address;
	private final int port;

	/**
	 * Initialize KVStore with address and port of KVServer
	 * 
	 * @param address the address of the KVServer
	 * @param port the port of the KVServer
	 */
	public KVStore(String address, int port) {
		this.address = address;
		this.port = port;
	}

	@Override
	public void connect() throws Exception {
		clientSocket = new Socket(address, port);
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

		} else if (value.length() > MAX_VALUE_LENGTH || value.trim().contains("\n")) {
			throw new IllegalArgumentException("Illegal <value> value");
		}

		SerializableKVMessage putMessage = new BasicKVMessage(key, value, StatusType.PUT);
		BasicKVMessage.sendMessage(out, putMessage);

		return BasicKVMessage.receiveMessage(in);
	}

	@Override
	public KVMessage get(String key) throws Exception {
		if (clientSocket == null) {
			throw new IllegalArgumentException("Not currently connected to server");

		} else if (key == null || key.isEmpty() || key.length() > MAX_KEY_LENGTH
				|| key.trim().contains(" ") || key.trim().contains("\n")) {
			throw new IllegalArgumentException("Illegal <key> value");

		}

		SerializableKVMessage getMessage = new BasicKVMessage(key, null, StatusType.GET);
		BasicKVMessage.sendMessage(out, getMessage);

		return BasicKVMessage.receiveMessage(in);

	}

}
