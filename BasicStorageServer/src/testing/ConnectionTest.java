package testing;

import java.net.UnknownHostException;

import org.junit.Test;

import client.KVStore;
import testing.util.ServerTest;

/**
 * Tests basic connection functionality of the client application.
 * 
 * @see KVStore#connect()
 */
public class ConnectionTest extends ServerTest {

	/**
	 * Tests successful connection to a server.
	 * 
	 * @throws Exception Should not be thrown
	 */
	@Test
	public void testConnectionSuccess() throws Exception {
		KVStore kvClient = new KVStore("localhost", 60001);
		kvClient.connect();
	}

	/**
	 * Tests connection to an unknown host.
	 * 
	 * @throws Exception Expected to be {@link UnknownHostException}
	 */
	@Test(expected = UnknownHostException.class)
	public void testUnknownHost() throws Exception {
		KVStore kvClient = new KVStore("unknown", 50000);
		kvClient.connect();
	}

	/**
	 * Tests connection to an illegal port.
	 * 
	 * @throws Exception Expected to be {@link IllegalArgumentException}
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testIllegalPort() throws Exception {
		KVStore kvClient = new KVStore("localhost", 123456789);
		kvClient.connect();
	}

}
