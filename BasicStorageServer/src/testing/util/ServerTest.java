package testing.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import common.KVServiceTopology;
import common.zookeeper.ZKPathUtil;
import common.zookeeper.ZKSession;
import ecs.ECSNode;
import ecs.IECSNode;
import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import app_kvServer.KVServer;
import logger.LogSetup;

import static common.zookeeper.ZKSession.FINISHED;
import static common.zookeeper.ZKSession.RUNNING_STATUS;
import static common.zookeeper.ZKSession.STOPPED_STATUS;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Provides setup and teardown methods for tests classes which require a server1
 * to be running. The server1 is set up at port 50000 with a 10-entry FIFO cache.
 */
public abstract class ServerTest {

	protected static KVServiceTopology topology;
	private static ZKSession zkSession;

	protected static KVServer server1;
	protected static KVServer server2;

	private static Collection<IECSNode> nodes;

	private static final String server1Name = "test-server1";
	private static final String server2Name = "test-server2";

	private static final String zkHost = "localhost";
	private static final int zkPort = 2181;

	/**
	 * Initializes the server1 used for this test class and sets up logging.
	 */
	@SuppressWarnings("deprecation")
	@BeforeClass
	public static void serverSetup() throws KeeperException, InterruptedException {
		try {
			LogSetup.initialize("logs/testing/test.log", Level.ERROR);
			// suppress console appender so ant task isn't flooded with logs
			Logger.getRootLogger().removeAppender("stdout");

			IECSNode node1 = new ECSNode(server1Name, "localhost", 60001, "FIFO", 10);
			IECSNode node2 = new ECSNode(server2Name, "localhost", 60002, "FIFO", 10);

			nodes = Arrays.asList(new IECSNode[]{node1, node2});

			topology = new KVServiceTopology();
			zkSession = new ZKSession(zkHost, zkPort);
			createZnodes(nodes);

			// Start servers
			server1 = new KVServer(server1Name, zkHost, zkPort);
			server2 = new KVServer(server2Name, zkHost, zkPort);

			for (IECSNode node: nodes) {
				String status = zkSession.getNodeData(ZKPathUtil.getStatusZnode(node));

				// Busy wait for status response
				while (status == null || !status.equals("FINISHED")) {
					status = zkSession.getNodeData(ZKPathUtil.getStatusZnode(node));
				}
				zkSession.updateNode(ZKPathUtil.getStatusZnode(node), new byte[0]);
			}

			// Set storage service status to RUNNING
			zkSession.updateNode(ZKPathUtil.KV_SERVICE_STATUS_NODE, RUNNING_STATUS.getBytes(UTF_8));

			server1.clearCache();
			server1.clearStorage();

			server2.clearCache();
			server2.clearStorage();

		} catch (IOException e) {
			System.err.println("I/O exception while initializing logger");
			e.printStackTrace();
		}
	}

	private static void createZnodes(Collection<IECSNode> nodes) throws KeeperException, InterruptedException {
		KVServiceTopology topology = new KVServiceTopology();
		topology.addNodes(nodes);

		zkSession.createNode(ZKPathUtil.KV_SERVICE_MD_NODE);
		zkSession.createNode(ZKPathUtil.KV_SERVICE_STATUS_NODE, STOPPED_STATUS.getBytes(UTF_8));
		zkSession.createNode(ZKPathUtil.KV_SERVICE_LOGGING_NODE, Level.ERROR.toString().getBytes(UTF_8));

		for (IECSNode node: nodes) {
			zkSession.createNode(ZKPathUtil.getStatusZnode(node));
			zkSession.createNode(String.format("/%s-migration", node.getNodeName()));
			zkSession.createNode(String.format("/%s-replication", node.getNodeName()));
			zkSession.createNode(String.format("/%s-heartbeat", node.getNodeName()));

			String migrationNode = zkSession.createMigrationZnode("ecs", node.getNodeName());
			zkSession.updateNode(migrationNode, FINISHED);
		}

		zkSession.updateMetadataNode(topology);
	}

	private static void deleteZnodes(Collection<IECSNode> nodes) throws KeeperException, InterruptedException {
		for (IECSNode node: nodes) {

			// Delete migration nodes
			List<String> migrationNodes = zkSession.getChildNodes(ZKPathUtil.getMigrationRootZnode(node), null);
			for (String m: migrationNodes) {
				zkSession.deleteNode(ZKPathUtil.getMigrationRootZnode(node) + "/" + m);
			}

			// Delete replication nodes
			List<String> replicationNodes = zkSession.getChildNodes(ZKPathUtil.getReplicationRootZnode(node), null);
			for (String r: replicationNodes) {
				zkSession.deleteNode(ZKPathUtil.getReplicationRootZnode(node) + "/" + r);
			}

			zkSession.deleteNode(String.format("/%s-heartbeat", node.getNodeName()));
			zkSession.deleteNode(String.format("/%s-replication", node.getNodeName()));
			zkSession.deleteNode(String.format("/%s-migration", node.getNodeName()));
			zkSession.deleteNode(ZKPathUtil.getStatusZnode(node));
		}

		zkSession.deleteNode(ZKPathUtil.KV_SERVICE_MD_NODE);
		zkSession.deleteNode(ZKPathUtil.KV_SERVICE_STATUS_NODE);
		zkSession.deleteNode(ZKPathUtil.KV_SERVICE_LOGGING_NODE);
	}

	/**
	 * Closes the server1 used by this class after all tests have been executed.
	 */
	@AfterClass
	public static void serverTeardown() throws KeeperException, InterruptedException, IOException {
		server1.clearCache();
		server1.clearStorage();

		server2.clearCache();
		server2.clearStorage();

		server1.die();
		server2.die();

		deleteZnodes(nodes);
		zkSession.close();
	}

}
