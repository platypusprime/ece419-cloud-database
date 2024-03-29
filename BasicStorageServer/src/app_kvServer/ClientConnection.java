package app_kvServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.Objects;

import org.apache.log4j.Logger;

import com.google.gson.JsonSyntaxException;

import app_kvServer.KVServer.ServerStatus;
import common.HashUtil;
import common.messages.BasicKVMessage;
import common.messages.KVMessage;
import common.messages.MetadataUpdateMessage;
import common.messages.KVMessage.StatusType;
import common.messages.StreamUtil;
import ecs.IECSNode;

/**
 * The class oversees a single server-side client connection session. When run
 * on a thread, polls socket input stream for messages and processes them.
 * Closes the socket and associated streams on thread termination.
 */
public class ClientConnection implements Runnable {

	private static Logger log = Logger.getLogger(ClientConnection.class);

	private Socket clientSocket;
	private final KVServer server;
	private final StreamUtil streamUtil;

	private boolean isOpen;

	/**
	 * Constructs a new CientConnection object for a given TCP socket.
	 * 
	 * @param clientSocket the Socket object for the client connection.
	 * @param server The object serving client requests
	 */
	public ClientConnection(Socket clientSocket, KVServer server) {
		this.clientSocket = clientSocket;
		this.server = server;
		this.streamUtil = new StreamUtil();
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

				KVMessage request = receiveRequest(in);
				if (request == null) continue;

				KVMessage response = createResponse(request);
				if (response != null) {
					streamUtil.sendMessage(out, response);
				} else {
					log.error("Could not create response to request " + request);
				}

				/* connection either terminated by the client or lost due to network problems */
			}

		} catch (IOException e) {
			log.error("Error! Connection could not be established!", e);

		}
	}

	private KVMessage createResponse(KVMessage request) {
		String outKey = request.getKey();
		String outValue = null;
		StatusType outStatus = null;

		ServerStatus serverStatus = this.server.getStatus();

		if (serverStatus == ServerStatus.STOPPED) {
			return new BasicKVMessage(null, null, StatusType.SERVER_STOPPED);
		}

		// Check if server is responsible for this key
		String keyHash = HashUtil.toMD5(outKey);
		if (!server.getServerConfig().containsHash(keyHash)) {

			// Send metadata update message containing info for the server that is
			// responsible for this key
			IECSNode correctServer = server.getServiceConfig().findResponsibleServer(keyHash);
			MetadataUpdateMessage metadataUpdateMessage = new MetadataUpdateMessage(correctServer);
			log.info("Sending metadata response: " + metadataUpdateMessage);
			return metadataUpdateMessage;
		}

		switch (request.getStatus()) {
		case GET:
			try {
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
			if (serverStatus == ServerStatus.WRITE_LOCKED) {
				return new BasicKVMessage(null, null, StatusType.SERVER_WRITE_LOCK);
			}

			boolean keyExists = server.inCache(request.getKey()) || server.inStorage(request.getKey());
			boolean valueEmpty = request.getValue() == null || request.getValue().isEmpty();
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
			return null;
		}

		return new BasicKVMessage(outKey, outValue, outStatus);
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

	private KVMessage receiveRequest(InputStream in) {
		// receive message from client
		String inStr = null;
		KVMessage request = null;
		try {
			log.trace("Listening for client messages");
			inStr = streamUtil.receiveString(in);
			// TODO validate message type
			// String msgType = streamUtil.identifyMessageType(inStr);
			request = streamUtil.deserializeKVMessage(inStr);

		} catch (JsonSyntaxException e) {
			log.error("Could not deserialize request: " + inStr, e);
		} catch (SocketException e) {
			if (e.getMessage().equals("Socket closed")) {
				log.warn("Socket closed");
			} else {
				log.error("Socket exception while receiving request", e);
			}
			this.isOpen = false;
		} catch (IOException e) {
			log.error("IOException while receiving request", e);
			this.isOpen = false;
		}

		return request;
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof ClientConnection)) {
			return false;
		}

		ClientConnection otherConnection = (ClientConnection) o;
		return Objects.equals(this.clientSocket, otherConnection.clientSocket);
	}

	@Override
	public int hashCode() {
		return this.clientSocket.hashCode();
	}

}
