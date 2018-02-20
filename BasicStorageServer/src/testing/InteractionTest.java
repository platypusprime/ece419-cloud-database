package testing;

import static org.junit.Assert.assertEquals;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import client.KVStore;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import testing.util.ServerTest;

/**
 * The pre-packaged piece of shit test class provided in the starter package.
 */
public class InteractionTest extends ServerTest {

	private KVStore kvClient;

	/**
	 * Initializes a client before each test.
	 * 
	 * @throws Exception If the client cannot connect to the server.
	 */
	@Before
	public void clientSetup() throws Exception {
		kvClient = new KVStore("localhost", 50000);
		kvClient.connect();
	}

	/**
	 * Disconnects the client after each test.
	 */
	@After
	public void clientTeardown() {
		kvClient.disconnect();
	}

	/**
	 * Sends a put request for a key-value pair and checks for a
	 * <code>PUT_SUCCESS</code> result
	 * from the server.
	 * 
	 * @throws Exception
	 */
	@Test
	public void testPut() throws Exception {
		String key = "foo2";
		String value = "bar2";
		KVMessage response = null;

		response = kvClient.put(key, value);
		assertEquals(StatusType.PUT_SUCCESS, response.getStatus());
	}

	/**
	 * Sends a put request while not connected to a server,
	 * which should result in an exception.
	 * 
	 * @throws Exception When {@link KVStore#put(String, String) put()} is called
	 */
	@Test(expected = Exception.class)
	public void testPutDisconnected() throws Exception {
		kvClient.disconnect();
		String key = "foo";
		String value = "bar";

		kvClient.put(key, value);
	}

	/**
	 * Sends two put requests for the same key with different values.
	 * 
	 * @throws Exception If an exception occurs during a request
	 */
	@Test
	public void testUpdate() throws Exception {
		String key = "updateTestValue";
		String initialValue = "initial";
		String updatedValue = "updated";

		KVMessage response = null;

		kvClient.put(key, initialValue);
		response = kvClient.put(key, updatedValue);

		assertEquals(StatusType.PUT_UPDATE, response.getStatus());
		assertEquals(updatedValue, response.getValue());
	}

	/**
	 * Sends an insert request and then a delete request for the same key, and
	 * checks that the newly inserted key-value pair was properly deleted.
	 * 
	 * @throws Exception if an exception occurs during a request
	 */
	@Test
	public void testDelete() throws Exception {
		String key = "deleteTestValue";
		String value = "toDelete";

		KVMessage response = null;

		kvClient.put(key, value);
		response = kvClient.put(key, "");

		assertEquals(StatusType.DELETE_SUCCESS, response.getStatus());
	}

	/**
	 * Sends a put request and then a get request for the same key, and checks that
	 * the newly inserted key-value pair was properly retrieved.
	 * 
	 * @throws Exception if an exception occurs during a request
	 */
	@Test
	public void testGet() throws Exception {
		String key = "foo";
		String value = "bar";
		KVMessage response = null;

		kvClient.put(key, value);
		response = kvClient.get(key);

		assertEquals(value, response.getValue());
	}

	/**
	 * Sends a get request for an unset key, and checks that the server properly
	 * responds with a <code>GET_ERROR</code> response.
	 * 
	 * @throws Exception if an exception occurs during a request
	 */
	@Test
	public void testGetUnsetValue() throws Exception {
		String key = "an-unset-value";
		KVMessage response = null;

		response = kvClient.get(key);

		assertEquals(StatusType.GET_ERROR, response.getStatus());
	}

}
