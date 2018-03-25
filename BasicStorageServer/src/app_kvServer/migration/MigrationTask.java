package app_kvServer.migration;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.Socket;
import java.util.Map;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import app_kvServer.persistence.KVPersistence;

@Deprecated
public class MigrationTask implements Runnable {
	
	public enum OpType {
		SEND,
		RECEIVE
	}
	
	private OpType type;
	
	private static final Logger log = Logger.getLogger(MigrationTask.class);
	
	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	private Socket socket;
	
	private KVPersistence persistenceManager;
	
	
	/**
	 * Creates a migration task for migrating data to or from another storage server
	 * 
	 * @param socket The socket connected to the other storage server
	 * @param persistenceManager The KVPersistenceManager instance responsible for interacting
	 *         with disk storage
	 * @param type The type of the migration operation (i.e. send or receive)
	 */
	public MigrationTask(Socket socket, KVPersistence persistenceManager, OpType type) {
		this.type = type;
		this.socket = socket;
		this.persistenceManager = persistenceManager;
	}

	@Override
	public void run() {
		if (type == OpType.SEND) {
			// Migrate data to the target server through 'socket'
			send();
		}
		else if (type == OpType.RECEIVE) {
			// Receive data from the source server through 'socket'
			receive();
		}
		
		closeConnection(socket);
	}
	
	private void receive() {
		try {
			// Receive data
			InputStream in = socket.getInputStream();
			String msgStr = read(in);
			Type type = new TypeToken<Map<String, String>>(){}.getType();
			Map<String, String> data = new Gson().fromJson(msgStr, type);
			
			log.info("Received data: '" + msgStr.trim() + "'");

			// TODO: Activate write lock
			
			persistenceManager.insertAll(data);
			
			// TODO: Deactivate write lock
			
			// Send back response
			OutputStream out = socket.getOutputStream();
			String responseStr = "MIGRATION_SUCCESSFUL" + '\n';
			byte[] msgBytes = responseStr.getBytes(UTF_8);

			out.write(msgBytes, 0, msgBytes.length);
			out.flush();
			log.info("Sent response: '" + responseStr.trim() + "'");
			
		} catch (IOException e) {
			log.error("Could not receive data from source server", e);
		}
	}

	private void send() {
		try {
			OutputStream out = socket.getOutputStream();
			Map<String, String> data = null /* persistenceManager.getAll() */;
			String msgStr = new Gson().toJson(data) + '\n';
			byte[] msgBytes = msgStr.getBytes(UTF_8);

			out.write(msgBytes, 0, msgBytes.length);
			out.flush();
			log.info("Sent message: '" + msgStr.trim() + "'");

		} catch (IOException e) {
			log.error("Could not send data to target server", e);
			return;
		}

		// Receive response
		try {
			InputStream in = socket.getInputStream();
			String res = read(in);
			log.info("Received message: '" + res.trim() + "'");

		} catch (IOException e) {
			log.error("Could not get response from target server", e);
		}
	}
	
	private String read(InputStream in) throws IOException {
		log.debug("Waiting for message...");

		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		byte read;
		boolean reading = true;
		while ((read = (byte) in.read()) != -1 // EOS
				&& read != 10  	// '\n'
				&& read != 13 	// '\r'
				&& reading) {			
			
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			bufferBytes[index] = read;
			index++;

			/* stop reading if DROP_SIZE is reached */
			if (msgBytes != null && msgBytes.length + index >= DROP_SIZE) {
				reading = false;
			}
		}

		if (msgBytes == null) {
			tmp = new byte[index];
			System.arraycopy(bufferBytes, 0, tmp, 0, index);
		} else {
			tmp = new byte[msgBytes.length + index];
			System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
			System.arraycopy(bufferBytes, 0, tmp, msgBytes.length, index);
		}

		msgBytes = tmp;
		String msgString = new String(msgBytes, UTF_8);
		return msgString;
	}
	
	private void closeConnection(Socket s) {
		log.info("tearing down migration server connection...");
		try {
			s.close();
		} catch (IOException e) {
			log.error("Unable to close connection!");
		}
	}
}
