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
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import app_kvClient.KVClient;
import client.KVCommInterface;
import common.messages.BasicKVMessage;
import common.messages.KVMessage.StatusType;
import testing.util.TrackingAppender;

/**
 * Tests the functionality of the client command-line interface.
 */
public class KVClientTest {

	private KVClient client;
	private KVCommInterface mockStore;
	private TrackingAppender logTracker;
	private Level origLevel;

	/**
	 * Instantiates the client object before each test.
	 */
	@Before
	public void setup() {
		origLevel = Logger.getRootLogger().getLevel(); // store original log level
		Logger.getRootLogger().setLevel(Level.ALL); // allow all logs

		client = new KVClient();

		mockStore = mock(KVCommInterface.class);
		client.setStore(mockStore);

		logTracker = new TrackingAppender();
		Logger.getLogger(KVClient.class).addAppender(logTracker);
	}

	/**
	 * Resets changes to global state made by these tests.
	 */
	@After
	public void teardown() {
		Logger.getLogger(KVClient.class).removeAppender(logTracker);
		Logger.getRootLogger().setLevel(origLevel); // restore original log level
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

			boolean stop = client.handleCommand("connect localhost 8033");
			assertFalse(stop);

			// if any exception occurs, an ERROR level message should be logged
			boolean noErrors = logTracker.getEventLog().stream()
					.allMatch(event -> !event.getLevel().equals(Level.ERROR));
			assertTrue(noErrors);

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
		boolean stop;
		LoggingEvent lastLog;

		stop = client.handleCommand("connect localhost 8000");
		assertFalse(stop);
		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Already connected to server; disconnect first", lastLog.getMessage());
	}

	/**
	 * Tests usage of the <code>connect</code> CLI command when provided an
	 * insufficient number of arguments.
	 */
	@Test
	public void testConnectMissingArgs() {
		boolean stop;
		LoggingEvent lastLog;

		client.setStore(null); // remove mock store

		// handle case with no args
		stop = client.handleCommand("connect");
		assertFalse(stop);
		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Insufficient arguments for connect command", lastLog.getMessage());
		logTracker.clear();

		// handle case with no hostname
		stop = client.handleCommand("connect 8000");
		assertFalse(stop);
		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Insufficient arguments for connect command", lastLog.getMessage());
		logTracker.clear();

		// handle case with no port
		stop = client.handleCommand("connect localhost");
		assertFalse(stop);
		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Insufficient arguments for connect command", lastLog.getMessage());
	}

	/**
	 * Tests usage of the <code>connect</code> CLI command with a non-numeric port
	 * value.
	 */
	@Test
	public void testConnectInvalidPort() {
		boolean stop;
		LoggingEvent lastLog;

		client.setStore(null); // remove mock store

		stop = client.handleCommand("connect localhost 8oOo");
		assertFalse(stop);
		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Could not parse port number as an integer: 8oOo", lastLog.getMessage());
	}

	/**
	 * Tests valid usage of the <code>disconnect</code> CLI command.
	 */
	@Test
	public void testDisconnect() {
		boolean stop;
		LoggingEvent lastLog;

		assertTrue(client.getStore() == mockStore);
		stop = client.handleCommand("disconnect");

		assertFalse(stop);
		assertNull(client.getStore());
		verify(mockStore).disconnect();

		lastLog = logTracker.getLastEvent();
		assertEquals(Level.INFO, lastLog.getLevel());
		assertEquals("Disconnect successful", lastLog.getMessage());
	}

	/**
	 * Tests usage of the <code>disconnect</code> CLI command while not currently
	 * connected to a server.
	 */
	@Test
	public void testDisconnectInvalidState() {
		boolean stop;
		LoggingEvent lastLog;

		client.setStore(null);
		stop = client.handleCommand("disconnect");

		assertFalse(stop);

		lastLog = logTracker.getLastEvent();
		assertEquals(Level.WARN, lastLog.getLevel());
		assertEquals("No current server connection; skipping disconnect", lastLog.getMessage());
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
		LoggingEvent lastLog;

		// check server exception case
		stop = client.handleCommand("put");
		assertFalse(stop);

		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Insufficient arguments for put command", lastLog.getMessage());
	}

	/**
	 * Tests usage of the <code>put</code> CLI command while not connected to a
	 * server.
	 */
	@Test
	public void testPutInvalidState() {
		boolean stop;
		LoggingEvent lastLog;

		client.setStore(null); // remove mock store

		stop = client.handleCommand("put foo bar");
		assertFalse(stop);

		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("FAILURE - not connected to server; cannot execute put", lastLog.getMessage());
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
		LoggingEvent lastLog;

		// check server exception case
		stop = client.handleCommand("get");
		assertFalse(stop);

		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Insufficient arguments for get command", lastLog.getMessage());
	}

	/**
	 * Tests usage of the <code>get</code> CLI command while not connected to a
	 * server.
	 */
	@Test
	public void testGetInvalidState() {
		boolean stop;
		LoggingEvent lastLog;

		client.setStore(null); // remove mock store

		stop = client.handleCommand("get foo");
		assertFalse(stop);

		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("FAILURE - not connected to server; cannot execute get", lastLog.getMessage());
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
		LoggingEvent lastLog;

		// set up mocking for the mock KV store
		when(mockStore.put(any(), any())).then(invoc -> new BasicKVMessage(invoc.getArgument(0), null, status));

		stop = client.handleCommand(command);
		assertFalse(stop);

		lastLog = logTracker.getLastEvent();
		assertEquals(logLevel, lastLog.getLevel());
		assertEquals(logMessage, lastLog.getMessage());
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
		LoggingEvent lastLog;

		when(mockStore.put(any(), any())).thenThrow(Exception.class);

		// check server exception case
		stop = client.handleCommand("put foo bar");
		assertFalse(stop);

		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Exception encountered while executing put", lastLog.getMessage());
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
		LoggingEvent lastLog;

		// set up mocking for the mock KV store
		when(mockStore.get(any())).then(invoc -> new BasicKVMessage(invoc.getArgument(0), value, status));

		stop = client.handleCommand(command);
		assertFalse(stop);

		lastLog = logTracker.getLastEvent();
		assertEquals(logLevel, lastLog.getLevel());
		assertEquals(logMessage, lastLog.getMessage());
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
	 * Tests client handling of a failure status in the server response for an
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
		boolean stop;
		LoggingEvent lastLog;

		when(mockStore.get(any())).thenThrow(Exception.class);

		// check server exception case
		stop = client.handleCommand("get foo");
		assertFalse(stop);

		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Exception encountered while executing get", lastLog.getMessage());
	}

	/**
	 * Tests the functionality of the <code>logLevel</code> CLI command when valid
	 * input is provided.
	 */
	@Test
	public void testLogLevel() {
		boolean stop;
		LoggingEvent lastLog;

		// check basic use case
		stop = client.handleCommand("logLevel INFO");
		assertFalse(stop);
		assertEquals(Level.INFO, Logger.getRootLogger().getLevel());
		lastLog = logTracker.getLastEvent();
		assertEquals(Level.INFO, lastLog.getLevel());
		assertEquals("Set log level: INFO", lastLog.getMessage());
	}

	/**
	 * Tests the functionality of the logLevel CLI command when extra tokens are
	 * provided in addition to valid input.
	 */
	@Test
	public void testLogLevelExtraArgs() {
		boolean stop;
		LoggingEvent lastLog;

		// check case where too many arguments are given (extra args should be ignored)
		stop = client.handleCommand("logLevel DEBUG FOO BAR");
		assertFalse(stop);
		assertEquals(Level.DEBUG, Logger.getRootLogger().getLevel());
		lastLog = logTracker.getLastEvent();
		assertEquals(Level.INFO, lastLog.getLevel());
		assertEquals("Set log level: DEBUG", lastLog.getMessage());
	}

	/**
	 * Tests the functionality of the <code>logLevel</code> CLI command when an
	 * incorrect number of inputs is provided.
	 */
	@Test
	public void testLogLevelMissingArgs() {
		boolean stop;
		LoggingEvent lastLog;

		// check empty string case
		stop = client.handleCommand("logLevel");
		assertFalse(stop);
		assertEquals(Level.ALL, Logger.getRootLogger().getLevel());
		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Insufficient arguments for logLevel command", lastLog.getMessage());
		logTracker.clear();

		// check multiple whitespace case
		stop = client.handleCommand("logLevel  ");
		assertFalse(stop);
		assertEquals(Level.ALL, Logger.getRootLogger().getLevel());
		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Insufficient arguments for logLevel command", lastLog.getMessage());
		logTracker.clear();

		// make sure command is not just setting the log level to ALL
		Logger.getRootLogger().setLevel(Level.TRACE);
		stop = client.handleCommand("logLevel");
		assertFalse(stop);
		assertEquals(Level.TRACE, Logger.getRootLogger().getLevel());
		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Insufficient arguments for logLevel command", lastLog.getMessage());
	}

	/**
	 * Tests the functionality of the <code>logLevel</code> CLI command when an
	 * invalid log level is provided.
	 */
	@Test
	public void testLogLevelBadArgs() {
		boolean stop;
		LoggingEvent lastLog;

		stop = client.handleCommand("logLevel foo");
		assertFalse(stop);
		assertEquals(Level.ALL, Logger.getRootLogger().getLevel());
		lastLog = logTracker.getLastEvent();
		assertEquals(Level.ERROR, lastLog.getLevel());
		assertEquals("Invalid log level: foo", lastLog.getMessage());
	}

	/**
	 * Checks whether the given log contains the specified logging event.
	 * 
	 * @param logs The logging events to search
	 * @param level The log level of the event
	 * @param query The message to check for
	 * @return <code>true</code> if there exists at least one log which matches the
	 *         specified level and message, <code>false</code> otherwise
	 */
	private boolean isMessageLogged(Collection<LoggingEvent> logs, Level level, String query) {
		return logs.stream()
				.filter(log -> log.getLevel().equals(level))
				.map(LoggingEvent::getMessage)
				.anyMatch(msg -> msg.equals(query));
	}

	/**
	 * Tests the functionality of the <code>help</code> CLI command.
	 */
	@Test
	public void testHelp() {
		boolean stop, helpShown;
		List<LoggingEvent> logs;

		stop = client.handleCommand("help");
		assertFalse(stop);

		logs = logTracker.getEventLog();
		helpShown = isMessageLogged(logs, Level.INFO, "KV CLIENT HELP (Usage):");
		assertTrue(helpShown);
	}

	/**
	 * Tests handling of an empty command.
	 */
	@Test
	public void testEmptyCommand() {
		boolean stop, helpShown, warnShown;
		List<LoggingEvent> logs;

		// check empty string case
		stop = client.handleCommand("");
		assertFalse(stop);
		logs = logTracker.getEventLog();
		helpShown = isMessageLogged(logs, Level.INFO, "KV CLIENT HELP (Usage):");
		assertTrue(helpShown);
		warnShown = isMessageLogged(logs, Level.WARN, "Unknown command;");
		assertTrue(warnShown);
		logTracker.clear();

		// check whitespace case
		stop = client.handleCommand("   ");
		assertFalse(stop);
		logs = logTracker.getEventLog();
		helpShown = isMessageLogged(logs, Level.INFO, "KV CLIENT HELP (Usage):");
		assertTrue(helpShown);
		warnShown = isMessageLogged(logs, Level.WARN, "Unknown command;");
		assertTrue(warnShown);
	}

	/**
	 * Tests handling of an unsupported command.
	 */
	@Test
	public void testUnknownCommand() {
		boolean stop, helpShown, warnShown;
		List<LoggingEvent> logs;

		stop = client.handleCommand("delete");
		assertFalse(stop);
		logs = logTracker.getEventLog();
		helpShown = isMessageLogged(logs, Level.INFO, "KV CLIENT HELP (Usage):");
		assertTrue(helpShown);
		warnShown = isMessageLogged(logs, Level.WARN, "Unknown command;");
		assertTrue(warnShown);
	}

	/**
	 * Tests handling of the <code>quit</code> CLI command.
	 */
	@Test
	public void testQuit() {
		boolean stop;
		LoggingEvent lastLog;

		stop = client.handleCommand("quit");
		assertTrue(stop);

		lastLog = logTracker.getLastEvent();
		assertEquals(Level.INFO, lastLog.getLevel());
		assertEquals("Disconnect successful", lastLog.getMessage());
	}

}
