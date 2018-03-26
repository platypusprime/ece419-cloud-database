package testing.common.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.junit.Assert.assertEquals;

import java.net.InetAddress;

import org.junit.Test;

import common.zookeeper.ZKSession;

/**
 * Tests the functionality of the ZooKeeper wrapper layer.
 */
public class ZKWrapperTest {

	/**
	 * Run this test with a ZooKeeper service running on localhost:2181. Using
	 * <code>zoo_sample.cfg</code>, which comes with the ZooKeeper distribution will
	 * accomplish this.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testSetGet() throws Exception {
		ZKSession session = new ZKSession(InetAddress.getLocalHost().getHostName(), 2181);
		session.createNode("/foo", "bar".getBytes(UTF_8), EPHEMERAL);
		assertEquals("bar", session.getNodeData("/foo"));
		session.deleteNode("/foo");
		session.close();
	}

}
