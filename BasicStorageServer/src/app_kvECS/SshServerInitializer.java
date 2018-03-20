package app_kvECS;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import ecs.IECSNode;

/**
 * An initializer that starts up servers remotely using SSH.
 */
public class SshServerInitializer implements ServerInitializer {

	private static final Logger log = Logger.getLogger(ECSClient.class);

	private static final String SERVER_INITIALIZATION_COMMAND = "ssh -n %s nohup java -jar m2-server.jar %s %d &";
	private static final long SERVER_INITIALIZATION_TIMEOUT = 3 * 1000;

	private final String zkHostname;
	private final int zkPort;

	/**
	 * Creates an SSH server initializer that instantiates servers with the given
	 * ZooKeeper server information.
	 * 
	 * @param zkHostname The host on which the ZooKeeper service is running
	 * @param zkPort The port on which the ZooKeeper service is running
	 */
	public SshServerInitializer(String zkHostname, int zkPort) {
		this.zkHostname = zkHostname;
		this.zkPort = zkPort;
	}

	@Override
	public void initializeServer(IECSNode serverMetadata)
			throws ServerInitializationException, IllegalArgumentException {
		// TODO fix command
		String command = String.format(SERVER_INITIALIZATION_COMMAND, serverMetadata.getNodeHost(), zkHostname, zkPort);

		log.info("Starting server using SSH: " + command);
		try {
			Process process = Runtime.getRuntime().exec(command);

			// sanity check on exit code for server
			process.waitFor(SERVER_INITIALIZATION_TIMEOUT, TimeUnit.MILLISECONDS);
			if (!process.isAlive() && process.exitValue() != 0) {
				// TODO dump stdin/stderr of process
				throw new ServerInitializationException("SSH script exited with status " + process.exitValue());
			}

			log.info("Server now running on " + serverMetadata.getNodeHost() + ":" + serverMetadata.getNodePort());

		} catch (IOException e) {
			throw new ServerInitializationException("I/O exception while executing node startup command", e);
		} catch (InterruptedException e) {
			throw new ServerInitializationException("Interrupted while waiting for server initialization", e);
		}
	}

}
