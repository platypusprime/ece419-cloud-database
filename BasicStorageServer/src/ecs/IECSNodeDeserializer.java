package ecs;

import java.lang.reflect.Type;

import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;

/**
 * The custom deserializer for {@link IECSNode}. Simply ensures that instances
 * of this interface are handled as {@link ECSNode} objects.
 */
public class IECSNodeDeserializer implements JsonDeserializer<IECSNode> {

	@Override
	public IECSNode deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		return context.deserialize(json, ECSNode.class);
	}

}
