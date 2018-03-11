package testing.ecs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import common.zookeeper.ZKWrapper;
import ecs.ECSNode;
import ecs.IECSNode;
import ecs.IECSNodeDeserializer;
import ecs.IECSNodeSerializer;
import edu.emory.mathcs.backport.java.util.Collections;

/**
 * Tests operations related to the {@link IECSNode} implementation, including
 * creation and serialization/deserialization.
 */
public class ECSNodeTest {

	/**
	 * Tests the serialization and deserialization of an {@link IECSNode} object
	 * using the GSON library and {@link IECSNodeSerializer}.
	 */
	@Test
	public void iecsNodeSerializationTest() {
		Gson gson = new GsonBuilder()
				.registerTypeAdapter(IECSNode.class, new IECSNodeSerializer())
				.registerTypeAdapter(IECSNode.class, new IECSNodeDeserializer())
				.create();

		// serialize a sample node stored in a list (as used by the ECS)
		IECSNode origNode = new ECSNode("server-foo", "host-foo", 12345, "FIFO", 2);
		List<?> origList = Collections.singletonList(origNode);
		String serialized = gson.toJson(origList);
		assertNotNull(serialized);

		// deserialize and check that all fields are preserved
		List<IECSNode> deserializedList = gson.fromJson(serialized, ZKWrapper.IECS_NODE_LIST_TYPE);
		assertEquals(1, deserializedList.size());
		IECSNode deserializedNode = deserializedList.get(0);
		assertEquals("server-foo", deserializedNode.getNodeName());
		assertEquals("host-foo", deserializedNode.getNodeHost());
		assertEquals(12345, deserializedNode.getNodePort());
		assertEquals("FIFO", deserializedNode.getCacheStrategy());
		assertEquals(2, deserializedNode.getCacheSize());
	}

}
