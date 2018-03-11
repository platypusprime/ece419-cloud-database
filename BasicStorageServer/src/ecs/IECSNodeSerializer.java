package ecs;

import static ecs.IECSNode.NODE_CACHE_SIZE_ATTR;
import static ecs.IECSNode.NODE_CACHE_STRATEGY_ATTR;
import static ecs.IECSNode.NODE_HOST_ATTR;
import static ecs.IECSNode.NODE_NAME_ATTR;
import static ecs.IECSNode.NODE_PORT_ATTR;
import static ecs.IECSNode.NODE_RANGE_END_ATTR;
import static ecs.IECSNode.NODE_RANGE_START_ATTR;

import java.lang.reflect.Type;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

/**
 * A custom JSON serializer for server metadata objects.
 */
public class IECSNodeSerializer implements JsonSerializer<IECSNode> {

	@Override
	public JsonElement serialize(IECSNode src, Type typeOfSrc, JsonSerializationContext context) {
		JsonObject messageObject = new JsonObject();

		messageObject.addProperty(NODE_NAME_ATTR, src.getNodeName());
		messageObject.addProperty(NODE_HOST_ATTR, src.getNodeHost());
		messageObject.addProperty(NODE_PORT_ATTR, src.getNodePort());
		messageObject.addProperty(NODE_RANGE_START_ATTR, src.getNodeHashRangeStart());
		messageObject.addProperty(NODE_RANGE_END_ATTR, src.getNodeHashRangeEnd());
		messageObject.addProperty(NODE_CACHE_STRATEGY_ATTR, src.getCacheStrategy());
		messageObject.addProperty(NODE_CACHE_SIZE_ATTR, src.getCacheSize());

		return messageObject;
	}

}
