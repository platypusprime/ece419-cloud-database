package testing;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import app_kvECS.ECSClient;
import app_kvECS.LocalServerInitializer;
import app_kvServer.persistence.FilePersistence;
import app_kvServer.persistence.KVPersistence;
import app_kvServer.persistence.KVPersistenceChunkator;
import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.zookeeper.ZKSession;
import ecs.IECSNode;
import logger.LogSetup;

/**
 * This test class measures various performance metrics for the overall cloud
 * database service. All tests expect that a ZooKeeper service is running and
 * accessible at <code>localhost:2181</code>.
 */
public class PerformanceTest {

	private static final Logger log = Logger.getLogger(PerformanceTest.class);

	private static final String ZK_HOSTNAME = "127.0.0.1";
	private static final int ZK_PORT = 2181;

	private ECSClient ecsClient;
	private ZKSession zkSession;

	/**
	 * Sets up logging.
	 * 
	 * @throws IOException If an I/O exception occurs
	 */
	@BeforeClass
	public static void classSetup() throws IOException {
		LogSetup.initialize("logs/test/PerformanceTest.log", Level.INFO);
	}

	/**
	 * Sets up the ECS which will be used to manage test servers, much like in a
	 * production environment. The ECS is initialized with a test config file and
	 * deploys servers locally.
	 */
	@Before
	public void setup() {
		log.info("Setting up ECS client before test...");
		this.zkSession = new ZKSession(ZK_HOSTNAME, ZK_PORT);
		this.ecsClient = new ECSClient("test-ecs.config", zkSession, new LocalServerInitializer(ZK_HOSTNAME, ZK_PORT));
	}

	/**
	 * Gracefully shuts down the ECS client, which should also stop associated
	 * servers.
	 */
	@After
	public void teardown() {
		log.info("Running test teardown...");
		this.ecsClient.shutdown();
	}

	/**
	 * Resets the logging configuration.
	 */
	@AfterClass
	public static void classTeardown() {
		log.info("Running class teardown for PerformanceTest.java...");
		LogSetup.teardown();
	}

	/**
	 * Tests the the cloud database' ability to load data. This is done by partially
	 * loading the Enron corpus, which contains a total of about 0.5M messages. All
	 * operations are performed with a single client and a service topology
	 * consisting of 3 cacheless servers.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testLoad() throws Exception {
		Collection<IECSNode> nodes = new ArrayList<>();
		nodes.addAll(ecsClient.addNodes(3, "NONE", 200));

		KVStore client = new KVStore("127.0.0.1", 3100);
		client.connect();

		ecsClient.start();
		Thread.sleep(1000); // give some time for startup to finish

		KVPersistence src = new FilePersistence("enron-1000.txt");

		KVPersistenceChunkator chunkator = src.chunkator();
		while (chunkator.hasNextChunk()) {
			Map<String, String> chunk = chunkator.nextChunk();
			for (Map.Entry<String, String> entry : chunk.entrySet()) {
				KVMessage resp = client.put(entry.getKey(), entry.getValue());
				if (resp.getStatus() != StatusType.PUT_SUCCESS && resp.getStatus() != StatusType.PUT_UPDATE) {
					log.fatal("Unexpected response: " + resp);
				}
			}
		}
	}
}
