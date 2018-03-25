package common.zookeeper;

import ecs.IECSNode;

/**
 * Provides methods for finding paths for standard znodes within the
 * KV database service.
 */
public class ZKPathUtil {

	/** The znode containing shared metadata for the entire KV storage service. */
	public static final String KV_SERVICE_MD_NODE = "/kv-metadata";

	/** The znode containing the status of the overall KV storage service. */
	public static final String KV_SERVICE_STATUS_NODE = "/kv-status";

	/** The znode containing the logging level for servers to use. */
	public static final String KV_SERVICE_LOGGING_NODE = "/kv-logging";

	/**
	 * Returns the path prefix for the all znodes specific to the given server.
	 * 
	 * @param server The server associated with this node prefix
	 * @return The path for the base znode for this server
	 */
	private static String getBaseZnode(IECSNode server) {
		return "/" + server.getNodeName();
	}

	/**
	 * Returns the path for a specific znode related to the given server. This path
	 * takes the form "<code>/&lt;serverName&gt;-&lt;specifier&gt;</code>".
	 * 
	 * @param server The server associated with this node
	 * @param specifier The string to append to the base znode path
	 * @return The znode path
	 */
	public static String getZnode(IECSNode server, String specifier) {
		return getBaseZnode(server) + "-" + specifier;
	}

	/**
	 * Returns the path for the znode used for signaling completion of various tasks
	 * to the ECS. Namely, these tasks are starting up and migrating data.
	 * 
	 * @param server The server associated with this node
	 * @return The ECS node path
	 */
	public static String getStatusZnode(IECSNode server) {
		return getZnode(server, "status");
	}

	/**
	 * Returns the path for the znode which serves as a parent for all migration
	 * znodes for the given server.
	 * 
	 * @param server The server associated with this node
	 * @return The root migration znode path
	 */
	public static String getMigrationRootZnode(IECSNode server) {
		return getZnode(server, "migration");
	}

	/**
	 * Returns the path for the znode through which key-value pairs will be
	 * transferred between two servers. This path takes the form
	 * "<code>/&lt;receiverName&gt;-migration/&lt;senderName&gt;</code>".
	 * 
	 * @param receiver The server accepting key-value pairs
	 * @param sender The server from which key-value pairs are being received
	 * @return The migration znode path for migration from <code>peer</code> to this
	 *         server
	 */
	public static String getMigrationZnode(IECSNode receiver, IECSNode sender) {
		return getMigrationRootZnode(receiver) + "/" + sender.getNodeName();
	}

	/**
	 * Returns the path for the znode which serves as a parent for all replication
	 * znodes for this server.
	 *
	 * @param server The server associated with this node
	 * @return The root replication znode path
	 */
	public static String getReplicationRootZnode(IECSNode server) {
		return getZnode(server, "replication");
	}

	/**
	 * Returns the path for the znode which serves as a parent for all replication
	 * znodes for this server.
	 *
	 * @param server The server associated with this node
	 * @return The root replication znode path
	 */
	public static String getHeartbeatZnode(IECSNode server) {
		return getZnode(server, "heartbeat");
	}



}
