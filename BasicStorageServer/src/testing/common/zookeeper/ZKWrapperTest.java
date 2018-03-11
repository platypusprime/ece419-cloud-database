package testing.common.zookeeper;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;

import org.junit.Test;

import common.zookeeper.ZKWrapper;

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
	public void sanityTest() throws Exception {
		ZKWrapper wrapper = new ZKWrapper(InetAddress.getLocalHost().getHostName(), 2181);
		wrapper.createNode("/foo", "bar".getBytes("UTF-8"));
		assertEquals("bar", wrapper.getNodeData("/foo"));
		wrapper.close();
	}

}
