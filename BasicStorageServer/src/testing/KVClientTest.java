package testing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.ServerSocket;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import app_kvClient.KVClient;
import client.KVCommInterface;
import common.messages.BasicKVMessage;
import common.messages.KVMessage.StatusType;
import testing.util.LogInstrumentingTest;

/**
 * Tests the functionality of the client command-line interface.
 */
public class KVClientTest extends LogInstrumentingTest {

	private KVClient client;
	private KVCommInterface mockStore;

	@Override
	public Class<?> getClassUnderTest() {
		return KVClient.class;
	}

	/**
	 * Instantiates the client object before each test.
	 */
	@Before
	public void setup() {
		client = new KVClient();

		mockStore = mock(KVCommInterface.class);
		client.setStore(mockStore);
	}

	/**
	 * Checks that the CLI is able to start up correctly.
	 * 
	 * @throws UnsupportedEncodingException If an exception occurs while encoding
	 *             the test command string
	 */
	@Test
	@Ignore("includes side-effects for logger setup")
	public void testInterface() throws UnsupportedEncodingException {
		ByteArrayInputStream s = new ByteArrayInputStream("quit".getBytes("UTF-8"));
		System.setIn(s);
		KVClient.main(null);
	}

	/**
	 * Tests valid usage of the <code>connect</code> CLI command.
	 * 
	 * @throws IOException If an exception occurs while setting up the server socket
	 */
	@Test
	public void testConnect() throws IOException {
		boolean stop;

		client.setStore(null); // remove mock store

		// set up a temporary server socket
		try (ServerSocket serverSocket = new ServerSocket(8033)) {
			// listen for single client
			new Thread(() -> {
				try {
					serverSocket.accept();
				} catch (IOException e) {
					fail("IOException while accepting client connection");
				}
			}).start();

			stop = client.handleCommand("connect localhost 8033");
			assertFalse(stop);

			// if any exception occurs, an ERROR level message should be logged
			assertAllLogsMatch(event -> !event.getLevel().equals(Level.ERROR));

			// tested in separate test; assumed to work
			client.handleCommand("disconnect");
		}
	}

	/**
	 * Tests usage of the <code>connect</code> CLI command while already connected
	 * to a server.
	 */
	@Test
	public void testConnectInvalidState() {
		boolean stop = client.handleCommand("connect localhost 8000");
		assertFalse(stop);
		assertLastLogEquals(Level.ERROR, "Already connected to server; disconnect first");
	}

	/**
	 * Tests usage of the <code>connect</code> CLI command when provided an
	 * insufficient number of arguments.
	 */
	@Test
	public void testConnectMissingArgs() {
		boolean stop;

		client.setStore(null); // remove mock store

		// handle case with no args
		stop = client.handleCommand("connect");
		assertFalse(stop);
		assertLastLogEquals(Level.ERROR, "Insufficient arguments for connect command");
		clearTracker();

		// handle case with no hostname
		stop = client.handleCommand("connect 8000");
		assertFalse(stop);
		assertLastLogEquals(Level.ERROR, "Insufficient arguments for connect command");
		clearTracker();

		// handle case with no port
		stop = client.handleCommand("connect localhost");
		assertFalse(stop);
		assertLastLogEquals(Level.ERROR, "Insufficient arguments for connect command");
	}

	/**
	 * Tests usage of the <code>connect</code> CLI command with a non-numeric port
	 * value.
	 */
	@Test
	public void testConnectInvalidPort() {
		boolean stop;

		client.setStore(null); // remove mock store

		stop = client.handleCommand("connect localhost 8oOo");
		assertFalse(stop);
		assertLastLogEquals(Level.ERROR, "Could not parse port number as an integer: 8oOo");
	}

	/**
	 * Tests valid usage of the <code>disconnect</code> CLI command.
	 */
	@Test
	public void testDisconnect() {
		assertTrue(client.getStore() == mockStore);

		boolean stop = client.handleCommand("disconnect");
		assertFalse(stop);

		assertNull(client.getStore());
		verify(mockStore).disconnect();

		assertLastLogEquals(Level.INFO, "Disconnect successful");
	}

	/**
	 * Tests usage of the <code>disconnect</code> CLI command while not currently
	 * connected to a server.
	 */
	@Test
	public void testDisconnectInvalidState() {
		boolean stop;

		client.setStore(null);
		stop = client.handleCommand("disconnect");

		assertFalse(stop);
		assertLastLogEquals(Level.WARN, "No current server connection; skipping disconnect");
	}

	/**
	 * Tests usage of the <code>put</code> CLI command with valid state and
	 * two arguments.
	 * 
	 * @throws Exception For mock verification
	 */
	@Test
	public void testPut() throws Exception {
		boolean stop;

		stop = client.handleCommand("put foo bar");
		assertFalse(stop);
		verify(mockStore).put(eq("foo"), eq("bar"));

	}

	/**
	 * Tests usage of the <code>put</code> CLI command with valid state and
	 * one argument.
	 * 
	 * @throws Exception For mock verification
	 */
	@Test
	public void testDelete() throws Exception {
		boolean stop;

		stop = client.handleCommand("put foo");
		assertFalse(stop);
		verify(mockStore).put(eq("foo"), isNull());
	}

	/**
	 * Tests usage of the <code>put</code> CLI command when provided an insufficient
	 * number of arguments.
	 */
	@Test
	public void testPutMissingArgs() {
		boolean stop;

		// check server exception case
		stop = client.handleCommand("put");
		assertFalse(stop);
		assertLastLogEquals(Level.ERROR, "Insufficient arguments for put command");
	}

	/**
	 * Tests usage of the <code>put</code> CLI command while not connected to a
	 * server.
	 */
	@Test
	public void testPutInvalidState() {
		boolean stop;

		client.setStore(null); // remove mock store

		stop = client.handleCommand("put foo bar");
		assertFalse(stop);
		assertLastLogEquals(Level.ERROR, "FAILURE - not connected to server; cannot execute put");
	}

	/**
	 * Tests usage of the <code>get</code> CLI command with valid state and
	 * arguments.
	 * 
	 * @throws Exception For mock verification
	 */
	@Test
	public void testGet() throws Exception {
		boolean stop;

		stop = client.handleCommand("get foo");
		assertFalse(stop);
		verify(mockStore).get(eq("foo"));
	}

	/**
	 * Tests usage of the <code>get</code> CLI command when provided an insufficient
	 * number of arguments.
	 */
	@Test
	public void testGetMissingArgs() {
		boolean stop;

		// check server exception case
		stop = client.handleCommand("get");
		assertFalse(stop);
		assertLastLogEquals(Level.ERROR, "Insufficient arguments for get command");
	}

	/**
	 * Tests usage of the <code>get</code> CLI command while not connected to a
	 * server.
	 */
	@Test
	public void testGetInvalidState() {
		boolean stop;

		client.setStore(null); // remove mock store

		stop = client.handleCommand("get foo");
		assertFalse(stop);
		assertLastLogEquals(Level.ERROR, "FAILURE - not connected to server; cannot execute get");
	}

	/**
	 * Tests client handling of a particular server response type after executing a
	 * <code>put</code> CLI command. Does this by checking the {@link KVClient}
	 * class log for an expected logging event.
	 * 
	 * @param command The CLI command to test
	 * @param status The server response status to handle
	 * @param logLevel The expected log level in the latest client log
	 * @param logMessage The expected log message in the latest client log
	 * @throws Exception For mocking
	 */
	private void testPutResponse(String command, StatusType status, Level logLevel, String logMessage)
			throws Exception {
		boolean stop;

		// set up mocking for the mock KV store
		when(mockStore.put(any(), any())).then(invoc -> new BasicKVMessage(invoc.getArgument(0), null, status));

		stop = client.handleCommand(command);
		assertFalse(stop);
		assertLastLogEquals(logLevel, logMessage);
	}

	/**
	 * Tests client handling of a success status in the server response for an
	 * insertion request.
	 * 
	 * @throws Exception For mocking
	 */
	@Test
	public void testPutSuccess() throws Exception {
		testPutResponse("put foo bar", StatusType.PUT_SUCCESS, Level.INFO, "SUCCESS - put successful");
	}

	/**
	 * Tests client handling of a success status in the server response for an
	 * update request.
	 * 
	 * @throws Exception For mocking
	 */
	@Test
	public void testUpdateSuccess() throws Exception {
		testPutResponse("put foo bar", StatusType.PUT_UPDATE, Level.INFO, "SUCCESS - put successful");
	}

	/**
	 * Tests client handling of a success status in the server response for a
	 * deletion request.
	 * 
	 * @throws Exception For mocking
	 */
	@Test
	public void testDeleteSuccess() throws Exception {
		testPutResponse("put foo", StatusType.DELETE_SUCCESS, Level.INFO, "SUCCESS - put successful");
	}

	/**
	 * Tests client handling of a failure status in the server response for an
	 * insertion request.
	 * 
	 * @throws Exception For mocking
	 */
	@Test
	public void testPutFailure() throws Exception {
		testPutResponse("put foo bar", StatusType.PUT_ERROR, Level.INFO, "FAILURE - put unsuccessful");
	}

	/**
	 * Tests client handling of a failure status in the server response for a
	 * deletion request.
	 * 
	 * @throws Exception For mocking
	 */
	@Test
	public void testDeleteFailure() throws Exception {
		testPutResponse("put foo", StatusType.DELETE_ERROR, Level.INFO, "FAILURE - put unsuccessful");
	}

	/**
	 * Tests client handling of an unexpected status type in the server response for
	 * a <code>put</code> CLI command.
	 * 
	 * @throws Exception For mocking
	 */
	@Test
	public void testPutUnexpectedResponse() throws Exception {
		testPutResponse("put foo bar", StatusType.PUT, Level.WARN, "FAILURE - unexpected response: PUT");
	}

	/**
	 * Tests client handling of an exception while executing a <code>put</code>
	 * CLI command.
	 * 
	 * @throws Exception For mocking
	 */
	@Test
	public void testPutException() throws Exception {
		boolean stop;

		when(mockStore.put(any(), any())).thenThrow(Exception.class);

		// check server exception case
		stop = client.handleCommand("put foo bar");
		assertFalse(stop);
		assertLastLogEquals(Level.ERROR, "Exception encountered while executing put");
	}

	/**
	 * Tests client handling of a particular server response type after executing a
	 * <code>put</code> or <code>get</code> CLI command. Does this by checking the
	 * {@link KVClient} class log for an expected logging event.
	 * 
	 * @param command The CLI command to test
	 * @param value The value to return in the mock response
	 * @param status The server response status to handle
	 * @param logLevel The expected log level in the latest client log
	 * @param logMessage The expected log message in the latest client log
	 * @throws Exception For mocking
	 */
	private void testGetResponse(String command, String value, StatusType status, Level logLevel, String logMessage)
			throws Exception {
		boolean stop;

		// set up mocking for the mock KV store
		when(mockStore.get(any())).then(invoc -> new BasicKVMessage(invoc.getArgument(0), value, status));

		stop = client.handleCommand(command);
		assertFalse(stop);
		assertLastLogEquals(logLevel, logMessage);
	}

	/**
	 * Tests client handling of a success status in the server response for a
	 * retrieval request.
	 * 
	 * @throws Exception For mocking
	 */
	@Test
	public void testGetSuccess() throws Exception {
		testGetResponse("get foo", "bar", StatusType.GET_SUCCESS, Level.INFO, "SUCCESS - get result is foo:bar");
	}

	/**
	 * Tests client handling of a failure status in the server response for a
	 * retrieval request.
	 * 
	 * @throws Exception For mocking
	 */
	@Test
	public void testGetFailure() throws Exception {
		testGetResponse("get foo", "bar", StatusType.GET_ERROR, Level.INFO, "FAILURE - could not get value for foo");
	}

	/**
	 * Tests client handling of an unexpected status type in the server response for
	 * a <code>get</code> CLI command.
	 * 
	 * @throws Exception For mocking
	 */
	@Test
	public void testGetUnexpectedResponse() throws Exception {
		testGetResponse("get foo", "bar", StatusType.PUT, Level.WARN, "FAILURE - unexpected response: PUT");
	}

	/**
	 * Tests client handling of an exception while executing a <code>get</code>
	 * CLI command.
	 * 
	 * @throws Exception For mocking
	 */
	@Test
	public void testGetException() throws Exception {
		when(mockStore.get(any())).thenThrow(Exception.class);

		// check server exception case
		boolean stop = client.handleCommand("get foo");
		assertFalse(stop);
		assertLastLogEquals(Level.ERROR, "Exception encountered while executing get");
	}

	/**
	 * Tests the functionality of the <code>logLevel</code> CLI command when valid
	 * input is provided.
	 */
	@Test
	public void testLogLevel() {
		boolean stop = client.handleCommand("logLevel INFO");
		assertFalse(stop);
		assertEquals(Level.INFO, Logger.getRootLogger().getLevel());
		assertLastLogEquals(Level.INFO, "Set log level: INFO");
	}

	/**
	 * Tests the functionality of the <code>logLevel</code> CLI command when extra
	 * tokens are provided in addition to valid input.
	 */
	@Test
	public void testLogLevelExtraArgs() {
		boolean stop = client.handleCommand("logLevel DEBUG FOO BAR");
		assertFalse(stop);
		assertEquals(Level.DEBUG, Logger.getRootLogger().getLevel());
		assertLastLogEquals(Level.INFO, "Set log level: DEBUG");
	}

	/**
	 * Tests the functionality of the <code>logLevel</code> CLI command when an
	 * incorrect number of inputs is provided.
	 */
	@Test
	public void testLogLevelMissingArgs() {
		boolean stop;

		// check empty string case
		stop = client.handleCommand("logLevel");
		assertFalse(stop);
		assertEquals(Level.ALL, Logger.getRootLogger().getLevel());
		assertLastLogEquals(Level.ERROR, "Insufficient arguments for logLevel command");
		clearTracker();

		// check multiple whitespace case
		stop = client.handleCommand("logLevel  ");
		assertFalse(stop);
		assertEquals(Level.ALL, Logger.getRootLogger().getLevel());
		assertLastLogEquals(Level.ERROR, "Insufficient arguments for logLevel command");
		clearTracker();

		// make sure command is not just setting the log level to ALL
		Logger.getRootLogger().setLevel(Level.TRACE);
		stop = client.handleCommand("logLevel");
		assertFalse(stop);
		assertEquals(Level.TRACE, Logger.getRootLogger().getLevel());
		assertLastLogEquals(Level.ERROR, "Insufficient arguments for logLevel command");
	}

	/**
	 * Tests the functionality of the <code>logLevel</code> CLI command when an
	 * invalid log level is provided.
	 */
	@Test
	public void testLogLevelBadArgs() {
		boolean stop = client.handleCommand("logLevel foo");
		assertFalse(stop);
		assertEquals(Level.ALL, Logger.getRootLogger().getLevel());
		assertLastLogEquals(Level.ERROR, "Invalid log level: foo");
	}

	/**
	 * Tests the functionality of the <code>help</code> CLI command.
	 */
	@Test
	public void testHelp() {
		boolean stop = client.handleCommand("help");
		assertFalse(stop);
		assertLogExists(Level.INFO, "KV CLIENT HELP (Usage):");
	}

	/**
	 * Tests handling of an empty command.
	 */
	@Test
	public void testEmptyCommand() {
		boolean stop;

		// check empty string case
		stop = client.handleCommand("");
		assertFalse(stop);
		assertLogExists(Level.INFO, "KV CLIENT HELP (Usage):");
		assertLogExists(Level.WARN, "Unknown command;");
		clearTracker();

		// check whitespace case
		stop = client.handleCommand("   ");
		assertFalse(stop);
		assertLogExists(Level.INFO, "KV CLIENT HELP (Usage):");
		assertLogExists(Level.WARN, "Unknown command;");
	}

	/**
	 * Tests handling of an unsupported command.
	 */
	@Test
	public void testUnknownCommand() {
		boolean stop = client.handleCommand("delete");
		assertFalse(stop);
		assertLogExists(Level.INFO, "KV CLIENT HELP (Usage):");
		assertLogExists(Level.WARN, "Unknown command;");
	}

	/**
	 * Tests handling of the <code>quit</code> CLI command.
	 */
	@Test
	public void testQuit() {
		boolean stop = client.handleCommand("quit");
		assertTrue(stop);
		assertLastLogEquals(Level.INFO, "Disconnect successful");
	}

}
