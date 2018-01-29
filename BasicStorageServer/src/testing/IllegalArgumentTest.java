package testing;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

import client.KVCommInterface;
import client.KVStore;
import junit.framework.TestCase;

public class IllegalArgumentTest extends TestCase {

	private KVCommInterface client;

	@Rule public ExpectedException thrown = ExpectedException.none();

	@Override
	protected void setUp() {
		client = new KVStore("localhost", 50000);
		try {
			client.connect();
		} catch (Exception e) {

		}
	}

	@Override
	protected void tearDown() {
		try {
			client.disconnect();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void testNullKeyPut() {

		Exception ex = null;
		try {
			client.put(null, "foo");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}

	public void testEmptyKeyPut() {

		Exception ex = null;
		try {
			client.put("", "foo");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}

	public void testOversizeKeyPut() {

		Exception ex = null;

		try {
			client.put("01234567890123456789", "foo");
		} catch (Exception e) {
			ex = e;
		}
		assertNull(ex);

		try {
			client.put("012345678901234567890", "foo");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}

	public void testIllegalCharKeyPut() {

		Exception ex = null;
		try {
			client.put("two words", "foo");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}

	public void testNullKeyGet() {

		Exception ex = null;
		try {
			client.get(null);
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}

	public void testEmptyKeyGet() {

		Exception ex = null;
		try {
			client.get("");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}

	public void testOversizeKeyGet() {

		Exception ex = null;

		try {
			client.get("01234567890123456789");
		} catch (Exception e) {
			ex = e;
		}
		assertNull(ex);

		try {
			client.get("012345678901234567890");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}

	public void testIllegalCharKeyGet() {

		Exception ex = null;
		try {
			client.get("two words");
		} catch (Exception e) {
			ex = e;
		}
		assertTrue(ex instanceof IllegalArgumentException);
	}

}
