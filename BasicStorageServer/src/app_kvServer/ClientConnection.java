package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import org.apache.log4j.Logger;

import common.messages.BasicKVMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;

/**
 * The class oversees a single server-side client connection session. When run
 * on a thread, polls socket input stream for messages and processes them.
 * Closes the socket and associated streams on thread termination.
 */
public class ClientConnection implements Runnable {

	private static Logger log = Logger.getLogger(ClientConnection.class);

	private Socket clientSocket;
	private final KVServer server;

	private boolean isOpen;

	// TODO implement equals()

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket the Socket object for the client connection.
	 * @param server The object serving client requests
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.server = server;
	}

	/**
	 * Initializes and starts the client connection. Loops until the connection is
	 * closed or aborted by the client.
	 */
	public void run() {
		try (OutputStream out = clientSocket.getOutputStream();
				InputStream in = clientSocket.getInputStream()) {

			isOpen = true;
			while (isOpen) {

				// receive message from client
				KVMessage request = null;
				try {
					log.info("Listening for client messages");
					request = BasicKVMessage.receiveMessage(in);
				} catch (IOException e) {
					log.error("Error! Connection lost!", e);
					isOpen = false;
				}

				String outKey = null;
				String outValue = null;
				StatusType outStatus = null;

				switch (request.getStatus()) {
				case GET:
					try {
						outKey = request.getKey();
						outValue = server.getKV(request.getKey());
						if (outValue != null) {
							outStatus = StatusType.GET_SUCCESS;
							log.info("get success: " + request.getKey() + ":" + outValue);
						} else {
							outStatus = StatusType.GET_ERROR;
							log.warn("result of get is null; reporting error");
						}
					} catch (Exception e) {
						outStatus = StatusType.GET_ERROR;
						log.error("error while retrieving get result", e);
					}
					break;

				case PUT:
					boolean keyExists = server.inCache(request.getKey()) || server.inStorage(request.getKey());
					boolean valueEmpty = request.getValue() == null || request.getValue().isEmpty();
					outKey = request.getKey();
					outValue = request.getValue();
					try {
						server.putKV(request.getKey(), request.getValue());
						if (keyExists && valueEmpty) {
							outStatus = StatusType.DELETE_SUCCESS;
						} else if (!keyExists && valueEmpty) {
							outStatus = StatusType.DELETE_ERROR;
						} else if (keyExists && !valueEmpty) {
							outStatus = StatusType.PUT_UPDATE;
						} else if (!keyExists && !valueEmpty) {
							outStatus = StatusType.PUT_SUCCESS;
						}
					} catch (Exception e) {
						if (valueEmpty) {
							outStatus = StatusType.DELETE_ERROR;
						} else {
							outStatus = StatusType.PUT_ERROR;
						}
						log.error("error while retrieving put result", e);
					}
					break;

				default:
					// ignore unexpected requests
					break;
				}
				KVMessage outMsg = new BasicKVMessage(outKey, outValue, outStatus);
				BasicKVMessage.sendMessage(out, outMsg);

				/*
				 * connection either terminated by the client or lost due to network problems
				 */

			}

		} catch (IOException e) {
			log.error("Error! Connection could not be established!", e);

		}
	}

	/**
	 * Closes this connection, as well as associated sockets and streams.
	 * 
	 * @throws IOException If an exception occurs while closing the socket.
	 */
	public void close() throws IOException {
		isOpen = false;
		clientSocket.close();
	}

}
