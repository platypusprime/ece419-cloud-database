package app_kvECS;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import app_kvServer.KVServer;
import ecs.IECSNode;

/**
 * An initializer that starts up servers locally with M1 functionality.
 */
public class LocalServerInitializer implements ServerInitializer {

	private static final Logger log = Logger.getLogger(LocalServerInitializer.class);

	private final String zkHostname;
	private final int zkPort;

	/**
	 * Creates an SSH server initializer that instantiates servers with the given
	 * ZooKeeper server information.
	 * 
	 * @param zkHostname The host on which the ZooKeeper service is running
	 * @param zkPort The port on which the ZooKeeper service is running
	 */
	public LocalServerInitializer(String zkHostname, int zkPort) {
		this.zkHostname = zkHostname;
		this.zkPort = zkPort;
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws IllegalArgumentException If the hostname of the given server is not
	 *             <code>localhost</code>
	 */
	@Override
	public void initializeServer(IECSNode serverMetadata)
			throws ServerInitializationException, IllegalArgumentException {
		String nodeHost = serverMetadata.getNodeHost();

		if (!nodeHost.equals("localhost") && !nodeHost.equals("127.0.0.1")) {
			throw new IllegalArgumentException("Cannot instantiate remote server; please use SshServerInitializer");
		}

		try {
			new KVServer(serverMetadata.getNodeName(), zkHostname, zkPort);
		} catch (KeeperException | InterruptedException e) {
			log.error("Could not initialize server", e);
		}
	}

}
