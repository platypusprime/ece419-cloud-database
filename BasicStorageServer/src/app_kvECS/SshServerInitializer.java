package app_kvECS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import app_kvServer.KVServer;
import ecs.IECSNode;

/**
 * An initializer that starts up servers remotely using SSH. Assumes a shared
 * file system where all hosts have the {@link KVServer} jar file available in
 * the present working directory of the current process.
 * 
 * @see KVServer
 */
public class SshServerInitializer implements ServerInitializer {

	private static final Logger log = Logger.getLogger(ECSClient.class);

	private static final String SERVER_INITIALIZATION_COMMAND = "ssh -oStrictHostKeyChecking=no %s nohup java -jar %s/m2-server.jar %s %s %d";
	private static final long SERVER_INITIALIZATION_TIMEOUT = 3 * 1000;

	private final String zkHostname;
	private final int zkPort;

	private Map<String, Process> processes = new HashMap<>();

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
	public void initializeServer(IECSNode serverMetadata) throws ServerInitializationException {
		String pwd = System.getProperty("user.dir");
		log.info("Initializing server from pwd: " + pwd);

		String command = String.format(SERVER_INITIALIZATION_COMMAND, serverMetadata.getNodeHost(), pwd,
				serverMetadata.getNodeName(), zkHostname, zkPort);

		log.info("Starting server using SSH: " + command);
		try {
			Process process = Runtime.getRuntime().exec(command);

			// sanity check on exit code for server
			process.waitFor(SERVER_INITIALIZATION_TIMEOUT, TimeUnit.MILLISECONDS);
			if (!process.isAlive() && process.exitValue() != 0) {
				// dump stdout/stderr of process
				try (BufferedReader out = new BufferedReader(new InputStreamReader(process.getInputStream()));
						BufferedReader err = new BufferedReader(new InputStreamReader(process.getErrorStream()))) {
					String ln;
					while ((ln = out.readLine()) != null) {
						log.info("OUT: " + ln);
					}
					while ((ln = err.readLine()) != null) {
						log.warn("ERR: " + ln);
					}
				} catch (IOException e) {
					log.warn("IO exception while dumping stdout/stderr of failed process", e);
				}

				throw new ServerInitializationException("SSH script exited with status " + process.exitValue());
			}

			// store process for later access/manipulation
			processes.put(serverMetadata.getNodeName(), process);
			log.info("Server now running on " + serverMetadata.getNodeHost() + ":" + serverMetadata.getNodePort());

		} catch (IOException e) {
			throw new ServerInitializationException("I/O exception while executing node startup command", e);
		} catch (InterruptedException e) {
			throw new ServerInitializationException("Interrupted while waiting for server initialization", e);
		}
	}

	/**
	 * Returns the process associated with the given server metadata.
	 * 
	 * @param serverMetadata The server metadata for the process to retrieve
	 * @return The desired server process, or <code>null</code> if no process exists
	 *         for the given server. Note that the given process may have ended.
	 */
	public Process getProcess(IECSNode serverMetadata) {
		return getProcess(serverMetadata.getNodeName());
	}

	/**
	 * Returns the process associated with the given server name.
	 * 
	 * @param serverName The server identifier for the process to retrieve
	 * @return The desired server process, or <code>null</code> if no process exists
	 *         for the given server. Note that the given process may have ended.
	 */
	public Process getProcess(String serverName) {
		return processes.get(serverName);
	}

}
