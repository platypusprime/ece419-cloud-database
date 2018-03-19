package common.zookeeper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import ecs.IECSNode;
import ecs.IECSNodeDeserializer;
import ecs.IECSNodeSerializer;

/**
 * Provides a wrapper around the public ZooKeeper API.
 */
public class ZKWrapper {

	/** The ZNode that serves as a parent to all system-global nodes. */
	public static final String KV_SERVICE_ROOT_NODE = "/ecs";

	/** The ZNode containing shared metadata for the entire KV storage service. */
	public static final String KV_SERVICE_MD_NODE = "/kv-metadata";

	/** The ZNode containing the status of the overall KV storage service. */
	public static final String KV_SERVICE_STATUS_NODE = "/kv-status";

	/** The ZNode containing the logging level for servers to use. */
	public static final String KV_SERVICE_LOGGING_NODE = "/kv-logging";

	/** The name of the UTF-8 character encoding, used throughout this project. */
	public static final String UTF_8 = "UTF-8";

	/**
	 * The status string for stopped service. Servers should not accept client
	 * requests in this state.
	 */
	public static final String STOPPED_STATUS = "STOPPED";

	/**
	 * The status string for running service. Servers can accept and serve client
	 * requests in this state.
	 */
	public static final String RUNNING_STATUS = "RUNNING";

	/**
	 * The status string indicating that a given server is ready for shutdown.
	 * Used for indicating that all cleanup operations such as data migration has been completed.
	 */
	public static final String READY_FOR_SHUTDOWN = "READY_FOR_SHUTDOWN";

	/**
	 * The status string indicating that a data migration operation has been completed.
	 */
	public static final String TRANSFER_COMPLETED = "TRANSFER_COMPLETED";

	/** The type for IECSNode lists. Used for deserialization. */
	public static final Type IECS_NODE_LIST_TYPE = new TypeToken<List<IECSNode>>() {}.getType();

	private static final Logger log = Logger.getLogger(ZKWrapper.class);

	// TODO tune as necessary
	private static final int ZK_INIT_TIMEOUT = 2;
	private static final int ZK_SESSION_TIMEOUT = 100 * 1000;

	// ZooKeeper fields
	private final String zkHostname;
	private final int zkPort;
	private ZooKeeper zookeeper = null;

	// Serialization fields
	private final Gson gson;

	/**
	 * Connects to the ZooKeeper service at the specified host and port and
	 * initializes the central metadata ZNode asynchronously.
	 * 
	 * @param zkHostname The hostname used by the ZooKeeper service
	 * @param zkPort The port number used by the ZooKeeper service
	 */
	public ZKWrapper(String zkHostname, int zkPort) {
		this.zkHostname = zkHostname;
		this.zkPort = zkPort;

		// attempt to connect to the (already-running) ZooKeeper service
		try {
			connect();
		} catch (IOException e) {
			log.fatal("Network failure while connecting to ZooKeeper at " + zkHostname + ":" + zkPort, e);
			// TODO throw new ZKInitializationException();
		} catch (InterruptedException e) {
			log.fatal("Interrupted while waiting for ZooKeeper connection", e);
			// TODO throw new ZKInitializationException();
		}

		if (zookeeper == null) {
			log.fatal("ZooKeeper connection failed");
			// TODO throw new ZKInitializationException();
		}

		// initialize GSON object
		gson = new GsonBuilder()
				.registerTypeAdapter(IECSNode.class, new IECSNodeSerializer())
				.registerTypeAdapter(IECSNode.class, new IECSNodeDeserializer())
				.create();
	}

	/**
	 * Initializes this object's {@link ZooKeeper} object using stored
	 * ZooKeeper service information.
	 * 
	 * @throws IOException If a network failure occurs while connecting to ZooKeeper
	 * @throws InterruptedException If the current thread is interrupted while
	 *             waiting for connection to complete
	 */
	public void connect() throws IOException, InterruptedException {

		String connectStr = zkHostname + ":" + zkPort;
		CountDownLatch connectionLatch = new CountDownLatch(1);

		ZooKeeper zookeeper = new ZooKeeper(connectStr, ZK_SESSION_TIMEOUT,
				watchedEvent -> {
					if (watchedEvent.getState() == KeeperState.SyncConnected)
						connectionLatch.countDown();
				});
		connectionLatch.await(ZK_INIT_TIMEOUT, TimeUnit.SECONDS);

		if (zookeeper.getState() == ZooKeeper.States.CONNECTED) {
			log.info("Connection to ZooKeeper service successful");
			this.zookeeper = zookeeper;
		}
	}

	/**
	 * Closes the ZooKeeper connection.
	 */
	public void close() {
		try {
			zookeeper.close();
			zookeeper = null;
		} catch (InterruptedException e) {
			log.error("Interrupted while closing ZooKeeper connection", e);
		}
	}

	/**
	 * Crates an empty ZNode at the specified path.
	 * 
	 * @param path The path of the ZNode to create
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public void createNode(String path) throws KeeperException, InterruptedException {
		createNode(path, new byte[0]);
	}

	/**
	 * Creates an ephemeral ZNode at the specified path with initial data.
	 * 
	 * @param path The path of the ZNode to create
	 * @param data The data to initialize the ZNode to
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public void createNode(String path, byte[] data) throws KeeperException, InterruptedException {
		// TODO do a better job with ACLs
		zookeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	/**
	 * Creates the central server metadata ZNode at the default path.
	 * 
	 * @param nodes A map containing metadata for all servers participating
	 *            in the KV service
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public void createMetadataNode(Map<String, IECSNode> nodes) throws KeeperException, InterruptedException {
		createNode(KV_SERVICE_MD_NODE, serializeMetadataMap(nodes));
	}

	public List<String> getChildNodes(String path, Watcher watcher) {
		List<String> children = null;
		try {
			children = zookeeper.getChildren(path, watcher);
		} catch (KeeperException | InterruptedException e) {
			log.error(String.format("Error while reading child nodes for '%s'", path), e);
		}

		return children;
	}

	/**
	 * Retrieves the data contained in the specified ZNode.
	 * 
	 * @param path The path of the ZNode to read
	 * @return The ZNode data decoded from UTF-8
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public String getNodeData(String path) throws KeeperException, InterruptedException {
		return getNodeData(path, null);
	}

	/**
	 * Retrieves the data contained in the specified ZNode, placing a watcher.
	 * 
	 * @param path The path of the ZNode to read
	 * @param callback The watcher to place on the ZNode
	 * @return The ZNode data decoded from UTF-8
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public String getNodeData(String path, Watcher callback) throws KeeperException, InterruptedException {

		Stat stat = zookeeper.exists(path, false);
		if (stat == null) {
			log.error("No node found at path: " + path);
			return null;
		}

		Stat getDataStat = new Stat();
		byte[] b = zookeeper.getData(path, callback, getDataStat);
		log.debug(getDataStat.toString());

		try {
			return new String(b, UTF_8);
		} catch (UnsupportedEncodingException e) {
			log.error("Exception occured while decoding data", e);
			return null;
		}
	}

	/**
	 * Retrieves and deserializes the data contained in the central metadata ZNode,
	 * setting a watcher.
	 * 
	 * @param watcher The callback to set
	 * @return The contents of the central metadata ZNodes deserialized as a list of
	 *         server metadata objects
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public List<IECSNode> getMetadataNodeData(Watcher watcher) throws KeeperException, InterruptedException {
		String metadataString = getNodeData(KV_SERVICE_MD_NODE, watcher);
		return gson.fromJson(metadataString, IECS_NODE_LIST_TYPE);
	}

	/**
	 * Retrieves and deserializes the data contained in the central metadata ZNode.
	 * 
	 * @return The contents of the central metadata ZNodes deserialized as a list of
	 *         server metadata objects
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public List<IECSNode> getMetadataNodeData() throws KeeperException, InterruptedException {
		return getMetadataNodeData(null);
	}

	/**
	 * Updates the data of the specified ZNode.
	 *
	 * @param path The path of the ZNode to update
	 * @param data The UTF-8 string data to set
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public void updateNode(String path, String data) {
		try {
			updateNode(path, data.getBytes(UTF_8));
		} catch (Exception e) {
			log.error("Exception while attempting to update ECS node: " + path, e);
		}
	}

	/**
	 * Updates the data of the specified ZNode.
	 * 
	 * @param path The path of the ZNode to update
	 * @param data The data to set
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public void updateNode(String path, byte[] data) throws KeeperException, InterruptedException {
		Stat stat = zookeeper.exists(path, false);
		if (stat != null) {
			int version = stat.getVersion();
			zookeeper.setData(path, data, version);
		}
	}

	/**
	 * Updates the central server metadata ZNode at the default path.
	 * 
	 * @param nodes A map containing updated metadata for all servers
	 *            participating in the KV service
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public void updateMetadataNode(Map<String, IECSNode> nodes) throws KeeperException, InterruptedException {
		updateNode(KV_SERVICE_MD_NODE, serializeMetadataMap(nodes));
	}

	/**
	 * Deletes the specified ZNode.
	 * 
	 * @param path The path of the ZNode to delete
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public void deleteNode(String path) throws KeeperException, InterruptedException {
		Stat stat = zookeeper.exists(path, false);
		if (stat != null) {
			int version = stat.getVersion();
			zookeeper.delete(path, version);
		}

	}

	/**
	 * Serializes a map containing server metadata information for the KV store
	 * service as JSON and encodes the resulting string in UTF-8.
	 * 
	 * @param nodes The server metadata to serialize
	 * @return UTF-8 bytes of the metadata JSON
	 */
	private byte[] serializeMetadataMap(Map<String, IECSNode> nodes) {
		List<IECSNode> nodesList = nodes.entrySet().stream()
				.map(Map.Entry::getValue)
				.collect(Collectors.toList());
		String serializedNodes = gson.toJson(nodesList);
		try {
			return serializedNodes.getBytes(UTF_8);
		} catch (UnsupportedEncodingException e) {
			log.error("Could not encode serialized metadata", e);
			return null;
		}
	}

}
