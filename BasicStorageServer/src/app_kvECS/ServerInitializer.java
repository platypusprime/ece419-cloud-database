package app_kvECS;

import ecs.IECSNode;

/**
 * The contract for classes which start up servers on behalf of the external
 * configuration service.
 */
public interface ServerInitializer {

	/**
	 * Initializes a given server.
	 * 
	 * @param serverMetadata The metadata for the server to initialize
	 * @throws ServerInitializationException If a problem occurs during
	 *             initialization
	 */
	public void initializeServer(IECSNode serverMetadata) throws ServerInitializationException;

}
