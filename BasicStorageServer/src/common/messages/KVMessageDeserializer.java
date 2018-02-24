package common.messages;

import static common.messages.HashRange.END_ATTR;
import static common.messages.HashRange.START_ATTR;
import static common.messages.KVMessage.HOST_ATTR;
import static common.messages.KVMessage.KEY_ATTR;
import static common.messages.KVMessage.METADATA_ATTR;
import static common.messages.KVMessage.PORT_ATTR;
import static common.messages.KVMessage.STATUS_ATTR;

import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

import common.messages.KVMessage.StatusType;

/**
 * A custom JSON deserializer for key-value messages.
 */
public class KVMessageDeserializer implements JsonDeserializer<KVMessage> {

	@Override
	public KVMessage deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		JsonObject messageObject;
		StatusType status;

		// cast JSON as JSON object
		if (!json.isJsonObject())
			throw new JsonParseException("Provided JSON is not a JSON object");
		messageObject = json.getAsJsonObject();

		// parse status type
		String statusString = getMandatoryString(messageObject, STATUS_ATTR);
		try {
			status = StatusType.valueOf(statusString);
		} catch (IllegalArgumentException e) {
			throw new JsonParseException("Invalid status value: \"" + statusString + "\"", e);
		}

		switch (status) {
		case SERVER_NOT_RESPONSIBLE:
			return deserializeMetadataUpdateMessage(messageObject, status);
		default:
			return deserializeBasicKVMessage(messageObject, status);
		}

	}

	/**
	 * Deserializes a general KV message with a key, value, and status type.
	 * 
	 * @param json The JSON object to deserialize
	 * @param status The status type associated with the message
	 * @return A {@link BasicKVMessage} containing the deserialized fields
	 */
	public KVMessage deserializeBasicKVMessage(JsonObject json, StatusType status) {
		String key = null, value = null;
		// parse key
		if (json.has(KEY_ATTR) && json.get(KEY_ATTR).isJsonPrimitive())
			key = json.getAsJsonPrimitive(KEY_ATTR).getAsString();

		// parse value
		if (json.has(KEY_ATTR) && json.get(KEY_ATTR).isJsonPrimitive())
			value = json.getAsJsonPrimitive(KEY_ATTR).getAsString();

		return new BasicKVMessage(key, value, status);
	}

	/**
	 * Deserializes a general KV message with a key, value, and status type.
	 * 
	 * @param json The JSON object to deserialize
	 * @param status The status type associated with the message
	 * @return A {@link BasicKVMessage} containing the deserialized fields
	 * @throws JsonParseException If the JSON is not in the expected format
	 */
	public KVMessage deserializeMetadataUpdateMessage(JsonObject json, StatusType status) throws JsonParseException {
		Map<HashRange, InetSocketAddress> hashRanges = new HashMap<>();

		// extract metadata array
		if (!json.has(METADATA_ATTR) || !json.get(METADATA_ATTR).isJsonArray())
			throw new JsonParseException("Missing or malformed metadata array");
		JsonArray hashRangesArray = json.getAsJsonArray(METADATA_ATTR);

		// add each element in the metadata array as an entry in the hash range table
		for (JsonElement hashRangeElement : hashRangesArray) {
			String start, end, host;
			int port;

			if (!hashRangeElement.isJsonObject())
				throw new JsonParseException("Invalid metadata array element; expected JSON object");
			JsonObject hashRangeObject = hashRangeElement.getAsJsonObject();

			start = getMandatoryString(hashRangeObject, START_ATTR);
			end = getMandatoryString(hashRangeObject, END_ATTR);

			host = getMandatoryString(hashRangeObject, HOST_ATTR);
			String portString = getMandatoryString(hashRangeObject, PORT_ATTR);
			try {
				port = Integer.parseInt(portString);
			} catch (NumberFormatException e) {
				throw new JsonParseException("Invalid port attribute value: " + portString, e);
			}
			hashRanges.put(new HashRange(start, end), new InetSocketAddress(host, port));
		}

		return new MetadataUpdateMessage(hashRanges, status);
	}

	/**
	 * Retrieves the value of a string attribute in a JSON object. Throws an
	 * exception if the value cannot be retrieved.
	 * 
	 * @param json The JSON object to search
	 * @param key The attribute name
	 * @return The string value of the attribute
	 * @throws JsonParseException If the attribute is missing or
	 *             not a JSON primitive
	 */
	private String getMandatoryString(JsonObject json, String key) throws JsonParseException {
		if (!json.has(key) || !json.get(key).isJsonPrimitive())
			throw new JsonParseException("Missing or malformed " + key + " attribute");

		return json.getAsJsonPrimitive(key).getAsString();
	}

}
