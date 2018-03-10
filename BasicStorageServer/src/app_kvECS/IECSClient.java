package app_kvECS;

import java.util.Collection;
import java.util.Map;

import app_kvServer.IKVServer;
import ecs.IECSNode;

/**
 * The contract for the ECS service specified in the assignment stub.
 */
public interface IECSClient {

	/**
	 * Starts the storage service by invoking {@link IKVServer#start()} on
	 * all {@link IKVServer} instances that participate in the service.
	 * 
	 * @return <code>true</code> on success, <code>false</code> on failure
	 * @throws Exception If an unhandled error occurs
	 */
	public boolean start() throws Exception;

	/**
	 * Stops the service; all participating KVServers are stopped for processing
	 * client requests but the processes remain running.
	 * 
	 * @return <code>true</code> on success, <code>false</code> on failure
	 * @throws Exception If an unhandled error occurs
	 */
	public boolean stop() throws Exception;

	/**
	 * Stops all server instances and exits the remote processes.
	 * 
	 * @return <code>true</code> on success, <code>false</code> on failure
	 * @throws Exception If an unhandled error occurs
	 */
	public boolean shutdown() throws Exception;

	/**
	 * Randomly choose a single server from the available machines and start its
	 * process by issuing an SSH call. This call launches the storage server with
	 * the specified cache size and replacement strategy. Equivalent to calling
	 * {@link #addNodes(int, String, int) addNodes(count, cacheStrategy,
	 * cacheSize)} with a <code>count</code> of 1.
	 * 
	 * @param cacheStrategy The cache strategy to use for the node
	 * @param cacheSize The cache size to use for the node
	 * @return The metadata for the created server
	 */
	public IECSNode addNode(String cacheStrategy, int cacheSize);

	/**
	 * Randomly choose <code>count</code> servers from the available machines and
	 * start their processes by issuing SSH calls. These calls launch the storage
	 * servers with the specified cache size and replacement strategy. All storage
	 * servers are initialized with the metadata and any persisted data, and remain
	 * in state stopped.
	 * <p>
	 * <b>NOTE</b>: Implementation must call {@link #setupNodes(int, String, int)}
	 * before the SSH calls to start the servers and must call
	 * {@link #awaitNodes(int, int)} before returning.
	 * 
	 * @param count The number of nodes to add
	 * @param cacheStrategy The cache strategy to use for the new nodes
	 * @param cacheSize The cache size to use for the new nodes
	 * @return set of strings containing the names of the nodes
	 */
	public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize);

	/**
	 * Randomly choose <code>count</code> servers from the bootstrapped ECS
	 * configuration file and adds them as potential machines for the ECS. Does not
	 * start up the processes for these servers. To do so, invoke
	 * {@link #addNode(String, int) addNode()} or {@link #addNodes(int, String, int)
	 * addNodes()}.
	 * 
	 * @param count The number of servers to select from the configuration file
	 * @param cacheStrategy The cache strategy to assign to the new servers
	 * @param cacheSize The cache size to assign to the new servers
	 * @return A collection containing metadata for the servers which were set up
	 *         within the ECS
	 */
	public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize);

	/**
	 * Waits for <code>count</code> nodes to connect to the ECS after launching
	 * their processes. Should be invoked by {@link #addNode(String, int) addNode()}
	 * and {@link #addNodes(int, String, int) addNodes()} after issuing SSH calls to
	 * start up the servers.
	 * 
	 * @param count The number of nodes to collect responses for
	 * @param timeout The timeout period in milliseconds
	 * @return <code>true</code> if all nodes reported successfully within the
	 *         timeout period, <code>false</code> otherwise
	 * @throws Exception If an exception occurs while collecting responses
	 */
	public boolean awaitNodes(int count, int timeout) throws Exception;

	/**
	 * Removes nodes with names matching those specified.
	 * 
	 * @param nodeNames A list containing the names of nodes to remove
	 * @return <code>true</code> on success, <code>false</code> otherwise
	 */
	public boolean removeNodes(Collection<String> nodeNames);

	/**
	 * Returns a map of all nodes.
	 * 
	 * @return A map containing the nodes loaded by the ECS
	 *         mapped to their node names
	 */
	public Map<String, IECSNode> getNodes();

	/**
	 * Returns the specific node responsible for the given key.
	 * 
	 * @param key The key to find the responsible node for
	 * @return The metadata for the responsible node, or <code>null</code> if no
	 *         nodes have been added
	 */
	public IECSNode getNodeByKey(String key);
}
