package client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.messages.BasicKVMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.SerializableKVMessage;

public class KVStore implements KVCommInterface {

	private Logger logger = Logger.getRootLogger();
	// private Set<ClientSocketListener> listeners;
	private boolean running;

	private Socket clientSocket;

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
		// listeners = new HashSet<ClientSocketListener>();
		setRunning(true);
		logger.info("Connection established");
	}

	@Override
	public void disconnect() {
		logger.info("try to close connection ...");

		try {
			tearDownConnection();
			// for (ClientSocketListener listener : listeners) {
			// listener.handleStatus(SocketStatus.DISCONNECTED);
			// }
		} catch (IOException ioe) {
			logger.error("Unable to close connection!");
		}
	}

	private void tearDownConnection() throws IOException {
		setRunning(false);
		logger.info("tearing down the connection ...");
		if (clientSocket != null) {
			clientSocket.close();
			clientSocket = null;
			logger.info("connection closed!");
		}
	}

	@Override
	public KVMessage put(String key, String value) throws Exception {
		if (clientSocket == null) {
			throw new IllegalArgumentException("Not currently connected to server");
		}

		SerializableKVMessage getMessage = new BasicKVMessage(key, null, StatusType.GET);
		String getMessageStr = getMessage.toMessageString();

		// open stream
		try (Writer writer = new OutputStreamWriter(clientSocket.getOutputStream());
				Reader streamReader = new InputStreamReader(clientSocket.getInputStream());
				BufferedReader reader = new BufferedReader(streamReader)) {
			writer.write(getMessageStr);
			writer.flush();
			// TODO log message

			String ln = reader.readLine();
			return new BasicKVMessage(ln);
		} catch (IOException e) {
			throw new IOException("IOException when processing message", e);
		}
	}

	@Override
	public KVMessage get(String key) throws Exception {
		if (clientSocket == null) {
			throw new IllegalArgumentException("Not currently connected to server");
		}

		SerializableKVMessage getMessage = new BasicKVMessage(key, null, StatusType.GET);
		String getMessageStr = getMessage.toMessageString();

		// open stream
		try (Writer writer = new OutputStreamWriter(clientSocket.getOutputStream());
				Reader streamReader = new InputStreamReader(clientSocket.getInputStream());
				BufferedReader reader = new BufferedReader(streamReader)) {
			writer.write(getMessageStr);
			writer.flush();
			// TODO log message

			String ln = reader.readLine();
			return new BasicKVMessage(ln);
		} catch (IOException e) {
			throw new IOException("IOException when processing message", e);
		}
	}

	public boolean isRunning() {
		return running;
	}

	public void setRunning(boolean running) {
		this.running = running;
	}
}
