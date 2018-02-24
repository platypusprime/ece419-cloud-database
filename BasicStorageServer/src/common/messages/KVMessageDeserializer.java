package common.messages;

import static common.messages.KVMessage.KEY_ATTR;
import static common.messages.KVMessage.METADATA_ATTR;
import static common.messages.KVMessage.STATUS_ATTR;

import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Set;

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
			return deserializeMetadataUpdateMessage(messageObject, context, status);
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
	 * @param context The deserialization context to use for deserializing
	 *            {@link ServerMetadata} objects
	 * @param status The status type associated with the message
	 * @return A {@link BasicKVMessage} containing the deserialized fields
	 * @throws JsonParseException If the JSON is not in the expected format
	 */
	public KVMessage deserializeMetadataUpdateMessage(JsonObject json, JsonDeserializationContext context,
			StatusType status) throws JsonParseException {
		Set<ServerMetadata> metadata = new HashSet<>();

		// extract metadata array
		if (!json.has(METADATA_ATTR) || !json.get(METADATA_ATTR).isJsonArray())
			throw new JsonParseException("Missing or malformed metadata array");
		JsonArray hashRangesArray = json.getAsJsonArray(METADATA_ATTR);

		// add each element in the metadata array as an entry in the hash range table
		for (JsonElement hashRangeElement : hashRangesArray) {
			ServerMetadata currentMetadata = context.deserialize(hashRangeElement, ServerMetadata.class);
			metadata.add(currentMetadata);
		}

		return new MetadataUpdateMessage(metadata, status);
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
