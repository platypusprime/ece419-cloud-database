package common.messages;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

public class BasicKVMessage implements SerializableKVMessage {

	private static final Logger log = Logger.getLogger(BasicKVMessage.class);

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;
	
	private final String key;
	private final String value;
	private final StatusType status;

	public BasicKVMessage(String key, String value, StatusType status) {
		this.key = key;
		this.value = value;
		this.status = status;
	}

	public static BasicKVMessage fromString(String msgString) {
		String key = null, value = null;
		StatusType status;

		String[] tokens = msgString.split(" ", 3);

		status = StatusType.valueOf(tokens[0]);

		if (tokens.length == 2) {
			key = tokens[1].trim();
		} else if (tokens.length == 3) {
			key = tokens[1].trim();
			value = tokens[2].trim();
		}

		return new BasicKVMessage(key, value, status);
	}

	public static BasicKVMessage fromBytes(byte[] bytes) throws IOException {
		String msgString = new String(bytes, "UTF-8");
		log.debug("Building KV message from bytes: " + msgString);
		return BasicKVMessage.fromString(msgString);
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public String getValue() {
		return value;
	}

	@Override
	public StatusType getStatus() {
		return status;
	}

	@Override
	public String toMessageString() {
		if (status == null) {
			log.warn("message status is null; cannot form message string");
			return null;
		}
		StringBuilder msgBuilder = new StringBuilder();
		msgBuilder.append(status.name());
		if (key != null && !key.isEmpty()) {
			msgBuilder.append(" ").append(key);
		}
		if (value != null && !value.isEmpty()) {
			msgBuilder.append(" ").append(value);
		}

		return msgBuilder.append("\n").toString();
	}

	public static void sendMessage(OutputStream out, SerializableKVMessage msg) throws IOException {
		String msgStr = msg.toMessageString();
		byte[] msgBytes = msgStr.getBytes("UTF-8");

		out.write(msgBytes, 0, msgBytes.length);
		out.flush();
		log.info("Sent message: '" + msgStr.trim() + "'");
	}

	public static SerializableKVMessage receiveMessage(InputStream in) throws IOException {
		log.debug("Waiting for message...");
		
		int index = 0;
		byte[] msgBytes = null, tmp = null;
		byte[] bufferBytes = new byte[BUFFER_SIZE];

		byte read;
		boolean reading = true;
		while ((read = (byte) in.read()) != 13 && read != 10 && read != -1 && reading) {/* carriage return */
			/* if buffer filled, copy to msg array */
			if (index == BUFFER_SIZE) {
				if (msgBytes == null) {
					tmp = new byte[BUFFER_SIZE];
					System.arraycopy(bufferBytes, 0, tmp, 0, BUFFER_SIZE);
				} else {
					tmp = new byte[msgBytes.length + BUFFER_SIZE];
					System.arraycopy(msgBytes, 0, tmp, 0, msgBytes.length);
					System.arraycopy(bufferBytes, 0, tmp, msgBytes.length,
							BUFFER_SIZE);
				}

				msgBytes = tmp;
				bufferBytes = new byte[BUFFER_SIZE];
				index = 0;
			}

			bufferBytes[index] = read;
			index++;

			/* stop reading is DROP_SIZE is reached */
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

		/* build final String */
		SerializableKVMessage msg = BasicKVMessage.fromBytes(msgBytes);
		log.info("Received message: '" + msg.toMessageString().trim() + "'");
		return msg;
	}

}
