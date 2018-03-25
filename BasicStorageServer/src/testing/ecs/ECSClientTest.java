package testing.ecs;

import static common.zookeeper.ZKSession.RUNNING_STATUS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import app_kvECS.ECSClient;
import app_kvECS.IECSClient;
import app_kvECS.LocalServerInitializer;
import common.HashUtil;
import common.zookeeper.ZKPathUtil;
import common.zookeeper.ZKSession;
import ecs.IECSNode;
import logger.LogSetup;

/**
 * Tests operations related to the {@link IECSClient} implementation, including
 * creation and serialization/deserialization.
 */
public class ECSClientTest {

	private static final String ZK_HOSTNAME = "127.0.0.1";
	private static final int ZK_PORT = 2181;

	private ECSClient ecsClient;
	private ZKSession zkSession;

	@BeforeClass
	public static void classSetup() throws IOException {
		LogSetup.initialize("logs/test/ESCClientTest.log", Level.DEBUG);
	}

	@Before
	public void setup() {
		this.zkSession = new ZKSession(ZK_HOSTNAME, ZK_PORT);
		this.ecsClient = new ECSClient("test-ecs.config", zkSession, new LocalServerInitializer(ZK_HOSTNAME, ZK_PORT));
	}

	@After
	public void teardown() {
		ecsClient.shutdown();
	}

	@AfterClass
	public static void classTeardown() {
		LogSetup.teardown();
	}

	/**
	 * Tests the ECSClient.addNodes(int count, String cacheStrategy, int cacheSize)
	 * and ECSClient.removeNodes(Collection<String> nodeNames) function.
	 */
	@Test
	public void testECSClientAddAndRemove() {
		Collection<IECSNode> nodes = ecsClient.addNodes(1, "FIFO", 200);
		for (IECSNode node : nodes) {
			assertNotNull(node);
			String nodeName = node.getNodeName();
			assertTrue(ecsClient.removeNodes(Arrays.asList(nodeName)));
		}
	}

	/**
	 * Tests the {@link ECSClient#getNodes()} function.
	 */
	@Test
	public void testECSClientGetNodes() {
		Collection<IECSNode> nodes = ecsClient.setupNodes(5, "FIFO", 200);
		Map<String, IECSNode> nodeMap = ecsClient.getNodes();
		assertNotNull(nodeMap);
	}

	/**
	 * Tests the ECSClient.getNodeByKey(String key) function by verifying
	 * the result falls within the return node's hash range.
	 */
	@Test
	public void testECSClientGetNodeByKey() {
		Collection<IECSNode> nodes = ecsClient.setupNodes(5, "FIFO", 200);
		String key = "testing";
		String keyHash = HashUtil.toMD5(key);
		IECSNode node = ecsClient.getNodeByKey(key);
		assertTrue(node.containsHash(keyHash));
	}

	/**
	 * Tests the ECSClient.start() function by verifying node data
	 */
	@Test
	public void testECSClientstart() {
		ecsClient.start();
		try {
			String data = zkSession.getNodeData(ZKPathUtil.KV_SERVICE_STATUS_NODE);
			assertEquals(data, RUNNING_STATUS);
		} catch (KeeperException | InterruptedException e) {
		}
	}

}
