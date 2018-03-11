package testing.util;

import java.io.IOException;

import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import app_kvServer.KVServer;
import logger.LogSetup;

/**
 * Provides setup and teardown methods for tests classes which require a server
 * to be running. The server is set up at port 50000 with a 10-entry FIFO cache.
 */
public abstract class ServerTest {

	private static KVServer server;

	/**
	 * Initializes the server used for this test class and sets up logging.
	 */
	@SuppressWarnings("deprecation")
	@BeforeClass
	public static void serverSetup() {
		try {
			LogSetup.initialize("logs/testing/test.log", Level.ERROR);
			server = new KVServer(50000, 10, "FIFO");
			server.clearCache();
			server.clearStorage();
		} catch (IOException e) {
			System.err.println("I/O exception while initializing logger");
			e.printStackTrace();
		}
	}

	/**
	 * Closes the server used by this class after all tests have been executed.
	 */
	@AfterClass
	public static void serverTeardown() {
		server.close();
	}

}
