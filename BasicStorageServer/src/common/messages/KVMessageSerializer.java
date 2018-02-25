package common.messages;

import static common.messages.KVMessage.KEY_ATTR;
import static common.messages.KVMessage.RESPONSIBLE_NODE_ATTR;
import static common.messages.KVMessage.STATUS_ATTR;
import static common.messages.KVMessage.VALUE_ATTR;
import static ecs.IECSNode.NODE_HOST_ATTR;
import static ecs.IECSNode.NODE_NAME_ATTR;
import static ecs.IECSNode.NODE_PORT_ATTR;
import static ecs.IECSNode.NODE_RANGE_END_ATTR;
import static ecs.IECSNode.NODE_RANGE_START_ATTR;

import java.lang.reflect.Type;
import java.util.Optional;

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

		Optional.ofNullable(src.getResponsibleServer())
				.ifPresent(server -> {
					JsonObject serverObject = new JsonObject();

					serverObject.addProperty(NODE_NAME_ATTR, server.getNodeName());
					serverObject.addProperty(NODE_HOST_ATTR, server.getNodeHost());
					serverObject.addProperty(NODE_PORT_ATTR, server.getNodePort());
					String[] hashRange = server.getNodeHashRange();
					serverObject.addProperty(NODE_RANGE_START_ATTR, hashRange[0]);
					serverObject.addProperty(NODE_RANGE_END_ATTR, hashRange[1]);

					messageObject.add(RESPONSIBLE_NODE_ATTR, serverObject);
				});

		return messageObject;
	}

}
