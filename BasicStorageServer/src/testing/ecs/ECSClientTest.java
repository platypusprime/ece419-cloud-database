package testing.ecs;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


import static common.zookeeper.ZKSession.KV_SERVICE_STATUS_NODE;
import static common.zookeeper.ZKSession.RUNNING_STATUS;

import java.util.Collection;
import java.util.Arrays;
import java.util.Map;

import org.junit.Test;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.apache.zookeeper.KeeperException;

import app_kvECS.ECSClient;
import ecs.ECSNode;
import ecs.IECSNode;
import common.HashUtil;
import common.zookeeper.ZKSession;


/**
 * Tests operations related to the {@link IECSCLient} implementation, including
 * creation and serialization/deserialization.
 */
public class ECSClientTest {

    private ECSClient ecsClient;
    private ZKSession zkSession;

    @Before
    public void setUp() {
        this.ecsClient = new ECSClient("127.0.0.1", 2181);
        this.zkSession = new ZKSession("127.0.0.1", 2181);
    }

    @After
    public void tearDown() {
        ecsClient.shutdown();
    }

    /**
     * Tests the ECSCLient.addNodes(int count, String cacheStrategy, int cacheSize)
     * and ECSCLient.removeNodes(Collection<String> nodeNames) function.
     */
    @Ignore
    @Test
    public void testECSClientAddAndRemove() {
        Collection<IECSNode> nodes = ecsClient.setupNodes(5, "FIFO", 200);
        for(IECSNode node : nodes) {
            assertNotNull(node);
            String nodeName = node.getNodeName();
            assertTrue(ecsClient.removeNodes(Arrays.asList(nodeName)));
        }
    }

    /**
     * Tests the ECSCLient.getNodes() function.
     */
    @Test
    public void testECSClientGetNodes() {
        Collection<IECSNode> nodes = ecsClient.setupNodes(5, "FIFO", 200);
        Map<String, IECSNode> nodeMap = ecsClient.getNodes();
        assertNotNull(nodeMap);
    }

    /**
     * Tests the ECSCLient.getNodeByKey(String key) function by verifying
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
     * Tests the ECSCLient.start() function by verifying node data
     */
    @Test
    public void testECSClientstart() {
        ecsClient.start();
        try {
            String data = zkSession.getNodeData(KV_SERVICE_STATUS_NODE);
            assertEquals(data, RUNNING_STATUS);
        } catch (KeeperException | InterruptedException e) {
        }
    }

}
