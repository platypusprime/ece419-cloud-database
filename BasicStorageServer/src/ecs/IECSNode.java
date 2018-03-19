package ecs;

import java.net.InetSocketAddress;

/**
 * Provides the contract for a node managed by an external configuration service
 * (ECS). Each node is associated with a server socket and hash range. It is
 * responsible solely for key-value pairs whose keys fall within that hash
 * range.
 */
public interface IECSNode {

	/** The JSON attribute name for the node name. */
	public static final String NODE_NAME_ATTR = "name";

	/** The JSON attribute name for the node hostname. */
	public static final String NODE_HOST_ATTR = "host";

	/** The JSON attribute name for the node port. */
	public static final String NODE_PORT_ATTR = "port";

	/** The JSON attribute name for the hash range start index. */
	public static final String NODE_RANGE_START_ATTR = "start";

	/** The JSON attribute name for the hash range end index. */
	public static final String NODE_RANGE_END_ATTR = "end";

	/** The JSON attribute name for the cache strategy. */
	public static final String NODE_CACHE_STRATEGY_ATTR = "cacheStrategy";

	/** The JSON attribute name for the cache capacity. */
	public static final String NODE_CACHE_SIZE_ATTR = "cacheSize";

	/**
	 * Returns a string identifier for this node.
	 * 
	 * @return the name of the node (e.g. "Server 8.8.8.8")
	 */
	public String getNodeName();

	/**
	 * Returns the path for the parent ZNode for all ZNodes specific to this server.
	 * 
	 * @return The ZNode path for the base ZNode
	 */
	public default String getBaseNodePath() {
		return "/" + getNodeName();
	}

	/**
	 * Returns the path for the ZNode used for communications between
	 * an arbitrary peer.
	 * 
	 * @param peer The name of the peer to be communicated with
	 * @return The ZNode path for the specified peer
	 */
	public default String getZNodePath(String peer) {
		return getBaseNodePath() + "/" + peer;
	}

	/**
	 * Returns the path for the ZNode used for receiving migrated data from other servers
	 *
	 * @return The ZNode path for migration
	 */
	public default String getMigrationNodePath() {
		return getZNodePath("migration");
	}

	public default String getMigrationNodePath(String peer) {
	    return getMigrationNodePath() + "/" + peer;
    }

	/**
	 * Returns the path for the ZNode used for receiving replicated data from other servers
	 *
	 * @return The ZNode path for replication
	 */
	public default String getReplicationNodePath() {
		return getZNodePath("replication");
	}

	/**
	 * Returns the path for the ZNode used for communications between the ECS and
	 * the associated server.
	 * 
	 * @return The ECS node path
	 */
	public default String getECSNodePath() {
		return getZNodePath("ecs");
	}

	/**
	 * Returns the hostname for this node.
	 * 
	 * @return The hostname of the node (e.g. "8.8.8.8")
	 */
	public String getNodeHost();

	/**
	 * Returns the listening port number for this node.
	 * 
	 * @return The port number of the node (e.g. 8080)
	 */
	public int getNodePort();

	/**
	 * Returns the socket address for client connections to the associated server.
	 * <p>
	 * <i>This method has been added on top on the original project stub.</i>
	 * 
	 * @return The client socket address
	 */
	public InetSocketAddress getNodeSocketAddress();

	/**
	 * Returns the start index of the associated hash range.
	 * 
	 * @return The start index (inclusive)
	 */
	public String getNodeHashRangeStart();

	/**
	 * Returns the end index of the associated hash range.
	 * 
	 * @return The end index (exclusive)
	 */
	public String getNodeHashRangeEnd();

	/**
	 * Returns this node's hash range.
	 * 
	 * @return array of two strings representing the low and high range of the
	 *         hashes that the given node is responsible for
	 */
	public default String[] getNodeHashRange() {
		return new String[] { getNodeHashRangeStart(), getNodeHashRangeEnd() };
	}

	/**
	 * Sets the end index of the associated hash range.
	 * 
	 * @param end The end index to set
	 * @throws IllegalArgumentException If the given index is not a valid MD5 hash
	 */
	public void setNodeHashRangeEnd(String end);

	/**
	 * Checks whether the given MD5 hash belongs within the associated hash range.
	 * <p>
	 * <i>This method has been added on top on the original project stub.</i>
	 * 
	 * @param hash The hash string to check
	 * @return <code>true</code> if <code>hash</code> lies within this range's start
	 *         index (inclusive) and end index (exclusive) with consideration for
	 *         wrap-around, <code>false</code> otherwise
	 */
	public boolean containsHash(String hash);

	/**
	 * Returns a string describing this node's cache eviction policy.
	 * 
	 * @return This node's cache strategy
	 */
	public String getCacheStrategy();

	/**
	 * Returns the maximum capacity of this node's cache.
	 * 
	 * @return This node's cache size
	 */
	public int getCacheSize();

}
