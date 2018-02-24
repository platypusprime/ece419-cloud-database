package common.messages;

import static common.messages.KVMessage.KEY_ATTR;
import static common.messages.KVMessage.STATUS_ATTR;
import static common.messages.KVMessage.VALUE_ATTR;

import java.lang.reflect.Type;
import java.util.Optional;
import java.util.Set;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * A custom JSON serializer for key-value messages.
 */
public class KVMessageSerializer implements JsonSerializer<KVMessage> {

	@Override
	public JsonElement serialize(KVMessage src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject messageObject = new JsonObject();

		Optional.ofNullable(src.getStatus())
				.map(Enum::name)
				.ifPresent(status -> messageObject.addProperty(STATUS_ATTR, status));
		Optional.ofNullable(src.getKey())
				.ifPresent(key -> messageObject.addProperty(KEY_ATTR, key));
		Optional.ofNullable(src.getValue())
				.ifPresent(value -> messageObject.addProperty(VALUE_ATTR, value));

		Set<ServerMetadata> metadata = src.getServerMetadata();
		if (metadata != null && !metadata.isEmpty()) {
			JsonArray metadataArray = new JsonArray();
			messageObject.add(KVMessage.METADATA_ATTR, metadataArray);
			for (ServerMetadata serverMetadata : metadata) {
				JsonElement metadataElement = context.serialize(serverMetadata, ServerMetadata.class);
				metadataArray.add(metadataElement);
			}
		}

		return messageObject;
	}

}
