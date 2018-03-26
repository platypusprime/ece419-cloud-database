/**
 * 
 */
package testing.common.messages;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import common.messages.BasicKVMessage;
import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.StreamUtil;

/**
 * Test class for the {@link StreamUtil} class.
 */
public class StreamUtilTest {

	private StreamUtil util;

	/**
	 * Instantiates the stream utility under test.
	 */
	@Before
	public void setup() {
		util = new StreamUtil();
	}

	/**
	 * Tests transmission of a {@link KVMessage} with status <code>PUT</code>.
	 * 
	 * @throws IOException If an I/O exception occurs
	 */
	@Test
	public void testSendPut() throws IOException {
		byte[] sentBytes;
		BasicKVMessage msg = new BasicKVMessage("foo", null, StatusType.PUT);
		try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			util.sendMessage(out, msg);
			sentBytes = out.toByteArray();
		}
		String sentStr = new String(sentBytes, UTF_8);
		assertEquals("{\"key\":\"foo\",\"status\":\"PUT\"}\n", sentStr);

	}

	/**
	 * Tests reception of a {@link KVMessage} with status <code>PUT</code>.
	 * 
	 * @throws IOException If an I/O exception occurs
	 */
	@Test
	public void testReceivePut() throws IOException {
		String msg = "{\"key\":\"foo\",\"status\":\"PUT\"}\n";
		ByteArrayInputStream in = new ByteArrayInputStream(msg.getBytes(UTF_8));
		String inMsgStr = util.receiveString(in);
		KVMessage rcvMsg = util.deserializeKVMessage(inMsgStr);
		assertEquals(StatusType.PUT, rcvMsg.getStatus());
		assertEquals("foo", rcvMsg.getKey());
		assertNull(rcvMsg.getValue());
		assertNull(rcvMsg.getResponsibleServer());
	}
}
