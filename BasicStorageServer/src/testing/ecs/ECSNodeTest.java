package testing.ecs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import common.zookeeper.ZKWrapper;
import ecs.ECSNode;
import ecs.IECSNode;
import ecs.IECSNodeDeserializer;
import ecs.IECSNodeSerializer;

/**
 * Tests operations related to the {@link IECSNode} implementation, including
 * creation and serialization/deserialization.
 */
public class ECSNodeTest {

	/**
	 * Tests the {@link ECSNode#ECSNode(String, int) ECSNode(String, int)}
	 * constructor.
	 */
	@Test
	public void testESCNodeSparseConstructor() {
		IECSNode node = new ECSNode("some-host", 0);
		assertEquals("3b59be01b665ade53876477dc0b1b7f4", node.getNodeHashRangeStart());
		assertEquals("ECSNode{ name:null address:some-host:0 range:[3b59be01b665ade53876477dc0b1b7f4,) }",
				node.toString());
	}

	/**
	 * Tests the {@link ECSNode#containsHash(String)} function using various hash
	 * ranges.
	 */
	@Test
	public void testContainsHash() {
		// construct a node with an arbitrary name and port number
		IECSNode node = new ECSNode(null, "some-host", 0, null, 0);
		assertEquals("3b59be01b665ade53876477dc0b1b7f4", node.getNodeHashRangeStart());

		String invalid = "3b59be01b665ade53876477dc0b1b7fg";
		String oneLower = "3b59be01b665ade53876477dc0b1b7f3";
		String manyLower = "1b59be01b665ade53876477dc0b1b7f4";
		String oneHigher = "3b59be01b665ade53876477dc0b1b7f5";
		String manyHigher = "cb59be01b665ade53876477dc0b1b7f4";

		// a newly initialized node has no end-port number so its hash range should
		// encompass all valid MD5 hashes
		assertFalse(node.containsHash(invalid));
		assertTrue(node.containsHash(oneLower));
		assertTrue(node.containsHash(manyLower));
		assertTrue(node.containsHash(oneHigher));
		assertTrue(node.containsHash(manyHigher));

		// add an end value below the start hash
		node.setNodeHashRangeEnd("20000000000000000000000000000000");
		assertTrue(node.containsHash(oneLower));
		assertFalse(node.containsHash(manyLower));
		assertFalse(node.containsHash(oneHigher));
		assertFalse(node.containsHash(manyHigher));

		// add an end value above the start hash (loop-around)
		node.setNodeHashRangeEnd("c0000000000000000000000000000000");
		assertTrue(node.containsHash(oneLower));
		assertTrue(node.containsHash(manyLower));
		assertFalse(node.containsHash(oneHigher));
		assertTrue(node.containsHash(manyHigher));
	}

	/**
	 * Tests the serialization and deserialization of an {@link IECSNode} object
	 * using the GSON library and {@link IECSNodeSerializer}.
	 */
	@Test
	public void testSerialization() {
		Gson gson = new GsonBuilder()
				.registerTypeAdapter(IECSNode.class, new IECSNodeSerializer())
				.registerTypeAdapter(IECSNode.class, new IECSNodeDeserializer())
				.create();

		// serialize a sample node stored in a list (as used by the ECS)
		IECSNode origNode = new ECSNode("server-foo", "host-foo", 12345, "FIFO", 2);
		List<IECSNode> origList = Collections.singletonList(origNode);
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
