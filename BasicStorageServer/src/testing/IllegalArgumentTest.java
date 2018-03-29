package testing;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import app_kvServer.KVServer;
import client.KVCommInterface;
import client.KVStore;
import testing.util.ServerTest;

/**
 * Contains tests for the {@link KVStore} implementation of the
 * {@link KVCommInterface} interface. Starts a {@link KVServer} instance running
 * on port 50000 for convenience but should not ever use it.
 */
public class IllegalArgumentTest extends ServerTest {

	/** The expected exception used for finer-grained exception testing. */
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	/** The client under test. */
	private KVCommInterface client;

	/**
	 * Initializes the communications interface under test and connects it to the
	 * test server.
	 * 
	 * @throws Exception If an exception occurs during client-server connection
	 */
	@Before
	public void setUp() throws Exception {
		client = new KVStore("localhost", 60001);
		client.connect();
	}

	/**
	 * Disconnects the client from the server.
	 */
	@After
	public void tearDown() {
		client.disconnect();
	}

	/**
	 * Tests using a null key in a put operation.
	 * 
	 * @throws Exception If an exception occurs during the put operation.
	 *             Expected to be an {@link IllegalArgumentException}.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testNullKeyPut() throws Exception {
		client.put(null, "foo");
	}

	/**
	 * Test using an empty key in a put operation.
	 * 
	 * @throws Exception If an exception occurs during the put operation.
	 *             Expected to be an {@link IllegalArgumentException}.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testEmptyKeyPut() throws Exception {
		client.put("", "foo");
	}

	/**
	 * Test using a key straddling the size limit and one just exceeding it in
	 * put operations.
	 * 
	 * @throws Exception If an exception occurs during either put operation.
	 *             Expects the 2nd put to throw an {@link IllegalArgumentException}.
	 */
	@Test
	public void testOversizeKeyPut() throws Exception {
		client.put("01234567890123456789", "foo");
		thrown.expect(IllegalArgumentException.class);
		client.put("012345678901234567890", "foo");
	}

	/**
	 * Test using a key containing internal whitespace in a put operation.
	 * 
	 * @throws Exception If an exception occurs during the put operation.
	 *             Expected to be an {@link IllegalArgumentException}.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testIllegalCharKeyPut() throws Exception {
		client.put("two words", "foo");
	}

	/**
	 * Test using a null key in a get operation.
	 * 
	 * @throws Exception If an exception occurs during the get operation.
	 *             Expected to be an {@link IllegalArgumentException}.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testNullKeyGet() throws Exception {
		client.get(null);
	}

	/**
	 * Test using an empty key in a get operation.
	 * 
	 * @throws Exception If an exception occurs during the get operation.
	 *             Expected to be an {@link IllegalArgumentException}.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testEmptyKeyGet() throws Exception {
		client.get("");
	}

	/**
	 * Test using a key straddling the size limit and one just exceeding it in
	 * get operations.
	 * 
	 * @throws Exception If an exception occurs during either put operation.
	 *             Expects the 2nd put to throw an {@link IllegalArgumentException}.
	 */
	@Test
	public void testOversizeKeyGet() throws Exception {
		client.get("01234567890123456789");
		thrown.expect(IllegalArgumentException.class);
		client.get("012345678901234567890");
	}

	/**
	 * Test using a key containing internal whitespace in a get operation.
	 * 
	 * @throws Exception If an exception occurs during the get operation.
	 *             Expected to be an {@link IllegalArgumentException}.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testIllegalCharKeyGet() throws Exception {
		client.get("two words");
	}

}
