package testing.app_kvECS;

import static org.junit.Assume.assumeTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import app_kvECS.ServerInitializationException;
import app_kvECS.SshServerInitializer;
import ecs.ECSNode;
import ecs.IECSNode;
import logger.LogSetup;

/**
 * Tests the functionality of the {@link SshServerInitializer} class.
 */
public class SshServerInitializerTest {

	private SshServerInitializer initializer = null;

	/**
	 * Sets up the test logger.
	 * 
	 * @throws IOException
	 */
	@BeforeClass
	public static void setupLogging() throws IOException {
		LogSetup.initialize("logs/tests", Level.INFO);
	}

	/**
	 * Cleans up the test logger.
	 */
	@AfterClass
	public static void teardownLogging() {
		LogSetup.teardown();
	}

	/**
	 * TODO
	 * 
	 * @throws UnknownHostException If the localhost hostname cannot be determined
	 */
	@Before
	public void setup() throws UnknownHostException {
		String localhost = InetAddress.getLocalHost().getHostName();

		// validate localhost and skip test if not on UG machines
		boolean isRunningOnUg = localhost.matches("ug\\d{3}");
		assumeTrue(isRunningOnUg);

		initializer = new SshServerInitializer(localhost, 2181);
		// TODO wipe/setup ZooKeeper nodes
	}

	/**
	 * Tests the {@link SshServerInitializer#initializeServer(IECSNode)} method.
	 * TODO describe checks
	 * 
	 * @throws ServerInitializationException If the server cannot be initialized
	 * @throws InterruptedException If the current thread is interrupted while
	 *             waiting for the server I/O threads to finish
	 */
	@Test
	public void testInitializeServer() throws ServerInitializationException, InterruptedException {

		IECSNode serverMd = new ECSNode("server-foo", "ug201.eecg.utoronto.ca", 50123, "FIFO", 5);
		initializer.initializeServer(serverMd);

		Process serverProcess = initializer.getProcess(serverMd);
		if (serverProcess.isAlive() || serverProcess.exitValue() == 0) {
			Thread outThread = new Thread(() -> {
				try (BufferedReader reader = new BufferedReader(
						new InputStreamReader(serverProcess.getInputStream()))) {
					String ln;
					while ((ln = reader.readLine()) != null) {
						System.out.println("OUT:" + ln); // TODO change to log
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			});

			Thread errThread = new Thread(() -> {
				try (BufferedReader reader = new BufferedReader(
						new InputStreamReader(serverProcess.getErrorStream()))) {
					String ln;
					while ((ln = reader.readLine()) != null) {
						System.out.println("ERR: " + ln); // TODO change to log
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
			});

			outThread.start();
			errThread.start();

			// TODO make appropriate assertions

			outThread.join();
			errThread.join();
		}

		serverProcess.destroyForcibly(); // kill server
	}

}
