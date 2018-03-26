package common.messages;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

/**
 * Provides utility methods for general stream-based communications. Serializes
 * messages as JSON using custom GSON type adapters.
 */
public class StreamUtil {

	/** The JSON attribute name for message type. */
	public static final String TYPE_ATTR = "type";
	
	private static final Logger log = Logger.getLogger(StreamUtil.class);

	private static final int BUFFER_SIZE = 1024;
	private static final int DROP_SIZE = 1024 * BUFFER_SIZE;

	private final Gson gson;

	/**
	 * Constructs a new stream utility with the standard set of custom GSON
	 * type adapters.
	 */
	public StreamUtil() {
		GsonBuilder gsonBuilder = new GsonBuilder();
		gsonBuilder.registerTypeAdapter(KVMessage.class, new KVMessageDeserializer());
		gsonBuilder.registerTypeAdapter(KVMessage.class, new KVMessageSerializer());
		gson = gsonBuilder.create();
	}

	/**
	 * Transmits a given message through the specified output stream after
	 * serialization into JSON format and encoding into UTF-8.
	 * 
	 * @param out The stream on which to transmit the message
	 * @param msg The message to transmit
	 * @throws IOException If an IO exception occurs while encoding or
	 *             transmitting the message
	 */
	public void sendMessage(OutputStream out, Object msg) throws IOException {
		String msgStr = gson.toJson(msg, KVMessage.class) + "\n";
		byte[] msgBytes = msgStr.getBytes(UTF_8);

		out.write(msgBytes, 0, msgBytes.length);
		out.flush();

		log.info("Sent message: '" + msgStr.trim() + "'");
	}

	/**
	 * Reads and decodes a newline-terminated message from the specified input
	 * stream as a UTF-8 string.
	 * 
	 * @param in The stream from which to read the message
	 * @return A string representation of the byte read, not including the
	 *         terminating newline character
	 * @throws IOException If an I/O exception occurs while reading or encoding the
	 *             message bytes
	 */
	public String receiveString(InputStream in) throws IOException {
		log.trace("Waiting for message...");

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
		return new String(msgBytes, UTF_8);
	}

	/**
	 * Determines the message type of a given JSON string, based on a
	 * standard format.
	 * 
	 * @param json The message to identify
	 * @return The message type, or <code>null</code> if the message type could not
	 *         be determined
	 */
	public String identifyMessageType(String json) {
		try {
			JsonElement je = new JsonParser().parse(json);
			// TODO
			return je.getAsJsonObject().get(TYPE_ATTR).getAsString();

		} catch (JsonParseException e) {
			log.warn("Could not parse message as JSON", e);
		} catch (NullPointerException | ClassCastException e) {
			log.warn("Missing or malformed type attribute", e);
		}
		return null;
	}

	/**
	 * Deserializes the given message string as a {@link KVMessage} using the
	 * registered GSON type adapters.
	 * 
	 * @param msgStr The message to deserialize
	 * @return The deserialized message, as a <code>KVMessage</code>
	 * @throws JsonSyntaxException If the message is invalid or malformed
	 */
	public KVMessage deserializeKVMessage(String msgStr) throws JsonSyntaxException {
		return gson.fromJson(msgStr, KVMessage.class);
	}

}
