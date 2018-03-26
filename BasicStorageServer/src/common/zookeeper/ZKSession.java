package common.zookeeper;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import common.KVServiceTopology;
import ecs.IECSNode;
import ecs.IECSNodeDeserializer;
import ecs.IECSNodeSerializer;

/**
 * Provides a wrapper around the public ZooKeeper API.
 */
public class ZKSession {

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
	 * Used for indicating that all cleanup operations such as data migration has
	 * been completed.
	 */
	public static final String READY_FOR_SHUTDOWN = "READY_FOR_SHUTDOWN";

	/** The status string indicating completion of a transfer task. */
	public static final String FINISHED = "FINISHED";

	/** The type for IECSNode lists. Used for deserialization. */
	public static final Type IECS_NODE_LIST_TYPE = new TypeToken<List<IECSNode>>() {}.getType();

	private static final Logger log = Logger.getLogger(ZKSession.class);

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
	 * initializes the central metadata znode asynchronously.
	 * 
	 * @param zkHostname The hostname used by the ZooKeeper service
	 * @param zkPort The port number used by the ZooKeeper service
	 */
	public ZKSession(String zkHostname, int zkPort) {
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
	 * Crates an empty znode at the specified path.
	 * 
	 * @param path The path of the znode to create
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public void createNode(String path) throws KeeperException, InterruptedException {
		createNode(path, new byte[0]);
	}

	/**
	 * Creates an ephemeral znode at the specified path with initial data.
	 * 
	 * @param path The path of the znode to create
	 * @param data The data to initialize the znode to
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public void createNode(String path, byte[] data) throws KeeperException, InterruptedException {
		// TODO do a better job with ACLs
		try {
			zookeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			if (e.code() == Code.NODEEXISTS) {
				log.debug("znode " + path + " already exists; updating existing node with data \"" + new String(data) + "\"");
				updateNode(path, data);
			} else {
				throw e;
			}
		}
		log.debug("Created znode: " + path);
	}

	/**
	 * Retrieves the children of the specified znode and leaves a watcher
	 * 
	 * @param path The path to find children for
	 * @param watcher The watcher to leave on the znode
	 * @return An arbitrarily ordered list of children,
	 *         or <code>null</code> if they could not be retrieved
	 */
	public List<String> getChildNodes(String path, Watcher watcher) {
		List<String> children = null;
		try {
			children = zookeeper.getChildren(path, watcher);
			log.debug("Got " + children.size() + " child(ren) for znode '" + path + "'");
		} catch (KeeperException e) {
			if (e.code() == Code.NONODE) {
				log.warn("Cannot get children; missing znode '" + path + "'");
			} else {
				log.error("ZooKeeper error while getting children for '" + path + "'", e);
			}
		} catch (InterruptedException e) {
			log.error("Interrupted while getting children for '" + path + "'", e);
		}

		return children;
	}

	/**
	 * Checks if a znode exists at the given path.
	 * 
	 * @param path The path of the znode to check existence for
	 * @param watcher TODO
	 * @return <code>true</code> if the znode exists, <code>false</code> otherwise
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public boolean checkNodeExists(String path, Watcher watcher) throws KeeperException, InterruptedException {
		return zookeeper.exists(path, watcher) != null;
	}

	/**
	 * Determines whether a given server is alive by checking its migration root
	 * znode for existence. In the case of a crash, the migration root znode is
	 * deleted by the ECS.
	 * 
	 * @param server The server to check
	 * @return <code>true</code> if the server's migration root znode exists,
	 *         <code>false</code> otherwise
	 */
	public boolean checkServerAlive(IECSNode server) {
		String migrationRootZnode = ZKPathUtil.getMigrationRootZnode(server);
		try {
			return zookeeper.exists(migrationRootZnode, null) != null;
		} catch (KeeperException | InterruptedException e) {
			log.warn("Exception while checking existence for transfer znode: " + migrationRootZnode);
			return false;
		}
	}

	/**
	 * Retrieves the data contained in the specified znode.
	 * 
	 * @param path The path of the znode to read
	 * @return The znode data decoded from UTF-8
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public String getNodeData(String path) throws KeeperException, InterruptedException {
		return getNodeData(path, null);
	}

	/**
	 * Retrieves the data contained in the specified znode, placing a watcher.
	 * 
	 * @param path The path of the znode to read
	 * @param callback The watcher to place on the znode
	 * @return The znode data decoded from UTF-8
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public String getNodeData(String path, Watcher callback) throws KeeperException, InterruptedException {

		Stat stat = zookeeper.exists(path, false);
		if (stat == null) {
			log.warn("No node found at path: " + path);
			return null;
		}

		Stat getDataStat = new Stat();
		byte[] b = zookeeper.getData(path, callback, getDataStat);
		log.trace(path + " stat: " + getDataStat.toString());

		return new String(b, UTF_8);
	}

	/**
	 * Retrieves and deserializes the data contained in the central metadata znode,
	 * setting a watcher.
	 * 
	 * @param watcher The callback to set
	 * @return The contents of the central metadata ZNodes deserialized as a list of
	 *         server metadata objects
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public List<IECSNode> getMetadataNodeData(Watcher watcher) throws KeeperException, InterruptedException {
		String metadataString = getNodeData(ZKPathUtil.KV_SERVICE_MD_NODE, watcher);
		if (metadataString == null) return null;
		return gson.fromJson(metadataString, IECS_NODE_LIST_TYPE);
	}

	/**
	 * Retrieves and deserializes the data contained in the central metadata znode.
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
	 * Updates the data of the specified znode with the specified string. Encodes
	 * the string using UTF-8.
	 *
	 * @param path The path of the znode to update
	 * @param data The string data to set
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public void updateNode(String path, String data) throws KeeperException, InterruptedException {
		updateNode(path, data.getBytes(UTF_8));
	}

	/**
	 * Updates the data of the specified znode with the specified bytes.
	 * 
	 * @param path The path of the znode to update
	 * @param data The data to set. Should be encoded as UTF-8
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public void updateNode(String path, byte[] data) throws KeeperException, InterruptedException {
		Stat stat = zookeeper.exists(path, false);
		if (stat != null) {
			int version = stat.getVersion();
			zookeeper.setData(path, data, version);
			log.debug("Updated znode " + path + " with data \"" + new String(data) + "\"");
		} else {
			log.error("znode at " + path + " does not exist; could not update");
		}
	}

	/**
	 * Updates the central server metadata znode at the default path.
	 * 
	 * @param topology The updated service topology object
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public void updateMetadataNode(KVServiceTopology topology) throws KeeperException, InterruptedException {
		updateNode(ZKPathUtil.KV_SERVICE_MD_NODE, serializeMetadataMap(topology.getNodeSet()));
	}

	/**
	 * Deletes the specified znode.
	 * 
	 * @param path The path of the znode to delete
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
	private byte[] serializeMetadataMap(Collection<IECSNode> nodes) {
		List<IECSNode> nodesList = new ArrayList<>(nodes);
		String serializedNodes = gson.toJson(nodesList);
		return serializedNodes.getBytes(UTF_8);
	}

	/**
	 * Constructs a znode for transferring key-value pairs between two servers.
	 * 
	 * @param src The name of the transferrer server
	 * @param target The name of the receiving server
	 * @return The path of the newly created transfer znode
	 * @throws KeeperException If the ZooKeeper server signals an error
	 * @throws InterruptedException If the transaction is interrupted
	 */
	public String createMigrationZnode(String src, String target) throws KeeperException, InterruptedException {
		String path = String.format("/%s-migration/%s", target, src);
		createNode(path);
		return path;
	}
}
