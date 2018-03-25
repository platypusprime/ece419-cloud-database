package app_kvServer.migration;

import java.util.Map;
import java.util.Objects;

import com.google.gson.Gson;

/**
 * This class provides the means to package key-value pairs for migration
 * between two servers.
 */
public class MigrationMessage {

	/** The actual data (i.e. key-value pairs) */
	private Map<String, String> data;

	/**
	 * Creates a message containing the given key-value pairs.
	 * 
	 * @param data The key-value pairs to embed
	 */
	public MigrationMessage(Map<String, String> data) {
		this.data = Objects.requireNonNull(data);
	}

	/**
	 * Retrieves the contained key-value pairs.
	 * 
	 * @return This message's data
	 */
	public Map<String, String> getData() {
		return data;
	}

	/**
	 * Serializes this message as a JSON string.
	 * 
	 * @return The serialized JSON representation of this message
	 */
	public String toJSON() {
		Gson gson = new Gson();
		return gson.toJson(this);
	}

	/**
	 * Deserializes the given string into a <code>MigrationMessage</code> object.
	 * 
	 * @param data A string containing the serialized JSON migration message
	 * @return The deserialized message
	 */
	public static MigrationMessage fromJSON(String data) {
		Gson gson = new Gson();
		return gson.fromJson(data, MigrationMessage.class);
	}
}
