package common.messages;

import static common.messages.HashRange.END_ATTR;
import static common.messages.HashRange.START_ATTR;
import static common.messages.KVMessage.HOST_ATTR;
import static common.messages.KVMessage.KEY_ATTR;
import static common.messages.KVMessage.PORT_ATTR;
import static common.messages.KVMessage.STATUS_ATTR;
import static common.messages.KVMessage.VALUE_ATTR;

import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Optional;

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

		Map<HashRange, InetSocketAddress> hashRanges = src.getHashRanges();
		if (hashRanges != null && !hashRanges.isEmpty()) {
			JsonArray metadataArray = new JsonArray();
			messageObject.add(KVMessage.METADATA_ATTR, metadataArray);
			for (Map.Entry<HashRange, InetSocketAddress> entry : hashRanges.entrySet()) {
				JsonObject serverObject = new JsonObject();

				HashRange range = entry.getKey();
				serverObject.addProperty(START_ATTR, range.getStart());
				serverObject.addProperty(END_ATTR, range.getEnd());

				InetSocketAddress address = entry.getValue();
				serverObject.addProperty(HOST_ATTR, address.getHostName());
				serverObject.addProperty(PORT_ATTR, address.getPort());

				metadataArray.add(serverObject);
			}
		}

		return messageObject;
	}

}
