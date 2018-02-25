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

    /**
     * Returns a string identifier for this node.
     * 
     * @return the name of the node (e.g. "Server 8.8.8.8")
     */
    public String getNodeName();

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
     * Returns this node's hash range.
     * 
     * @return array of two strings representing the low and high range of the
     *         hashes that the given node is responsible for
     */
    public String[] getNodeHashRange();

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

}
