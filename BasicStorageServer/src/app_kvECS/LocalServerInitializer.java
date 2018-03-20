package app_kvECS;

import java.net.InetAddress;
import java.net.UnknownHostException;

import app_kvServer.KVServer;
import ecs.IECSNode;

/**
 * An initializer that starts up servers locally with M1 functionality.
 */
public class LocalServerInitializer implements ServerInitializer {

	/**
	 * {@inheritDoc}
	 * 
	 * @throws IllegalArgumentException If the hostname of the given server is not
	 *             <code>localhost</code>
	 */
	@Override
	public void initializeServer(IECSNode serverMetadata)
			throws ServerInitializationException, IllegalArgumentException {
		try {
			if (!InetAddress.getLocalHost().equals(InetAddress.getByName(serverMetadata.getNodeHost()))) {
				throw new IllegalArgumentException("Cannot instantiate remote server; please use SshServerInitializer");
			}
		} catch (UnknownHostException e) {
			throw new ServerInitializationException("Invalid host: " + serverMetadata.getNodeHost(), e);
		}

		@SuppressWarnings("deprecation")
		KVServer server = new KVServer(serverMetadata.getNodePort(), serverMetadata.getCacheSize(),
				serverMetadata.getCacheStrategy());
		new Thread(server).start();
	}

}
