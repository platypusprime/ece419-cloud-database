package app_kvECS;

import static common.zookeeper.ZKPathUtil.KV_SERVICE_LOGGING_NODE;
import static common.zookeeper.ZKPathUtil.KV_SERVICE_MD_NODE;
import static common.zookeeper.ZKPathUtil.KV_SERVICE_STATUS_NODE;
import static common.zookeeper.ZKSession.FINISHED;
import static common.zookeeper.ZKSession.RUNNING_STATUS;
import static common.zookeeper.ZKSession.STOPPED_STATUS;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.zookeeper.CreateMode.EPHEMERAL;
import static org.apache.zookeeper.CreateMode.PERSISTENT;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import common.HashUtil;
import common.KVServiceTopology;
import common.zookeeper.ZKPathUtil;
import common.zookeeper.ZKSession;
import ecs.ECSNode;
import ecs.IECSNode;

/**
 * Implementation of {@link IECSClient}, which manages the server configurations
 * for the key-value storage service. Uses ZooKeeper to broadcast global
 * metadata updates, as well as to communicate with individual servers.
 */
public class ECSClient implements IECSClient {

	private static final Logger log = Logger.getLogger(ECSClient.class);

	// ECS bootstrap configuration file constants
	private static final String ECS_CONFIG_FILENAME = "ecs.config";
	private static final String ECS_CONFIG_DELIMITER = "[ ]+";

	/** The amount of time to wait for a node to start up. */
	private static final int NODE_STARTUP_TIMEOUT = 15 * 1000;

	/** The amount of time to wait for node removal. */
	private static final int SERVICE_RESIZE_TIMEOUT = 30 * 1000;

	private final String configFilename;

	/** Used to interact with ZooKeeper */
	private ZKSession zkSession;

	/** Used to instantiate service. */
	private final ServerInitializer serverInitializer;

	/** The ECS's internal representation of the service topology. */
	private KVServiceTopology topology = new KVServiceTopology();

	/** Heartbeat listening threads map. */
	private final Map<String, Thread> heartbeatThreads;

	/**
	 * Initializes an ECS service and connects it to the specified
	 * ZooKeeper service using the default wrapper.
	 * 
	 * @param zkHostname The hostname used by the ZooKeeper service
	 * @param zkPort The port number used by the ZooKeeper service
	 */
	public ECSClient(String zkHostname, int zkPort) {
		this(ECS_CONFIG_FILENAME, new ZKSession(zkHostname, zkPort), new SshServerInitializer(zkHostname, zkPort));
	}

	/**
	 * Initializes an ECS service and connects it to the specified ZooKeeper service
	 * using the specified wrapper. Provided in order to allow for mocking.
	 * 
	 * @param configFilename The ECS config file, containing available server
	 *            information
	 * @param zkSession The wrapper class used to access ZooKeeper functionality
	 * @param serverInitializer The initializer responsible for starting up servers
	 */
	public ECSClient(String configFilename, ZKSession zkSession, ServerInitializer serverInitializer) {
		log.info("Initializing ECSClient");
		
		this.zkSession = zkSession;
		this.serverInitializer = serverInitializer;
		this.configFilename = configFilename;
		this.heartbeatThreads = new HashMap<>();

		try {
			log.debug("Cleaning up leftover znodes");
			this.zkSession.deleteNode("/");

			log.debug("Creating ECS znodes");
			this.zkSession.createNode(KV_SERVICE_MD_NODE, EPHEMERAL);
			this.zkSession.createNode(KV_SERVICE_STATUS_NODE, STOPPED_STATUS.getBytes(UTF_8), EPHEMERAL);
			this.zkSession.createNode(KV_SERVICE_LOGGING_NODE, Level.ERROR.toString().getBytes(UTF_8), EPHEMERAL);

		} catch (KeeperException | InterruptedException e) {
			log.error("Exception while setting up ECS znodes", e);
		}

	}

	@Override
	public boolean start() {
		log.info("Starting all servers in the KV store service");
		try {
			zkSession.updateNode(KV_SERVICE_STATUS_NODE, RUNNING_STATUS.getBytes(UTF_8));
			return true;
		} catch (KeeperException | InterruptedException e) {
			log.error("Could not update status node", e);
		}
		return false;
	}

	@Override
	public boolean stop() {
		log.info("Stopping all servers in the KV store service");
		try {
			zkSession.updateNode(KV_SERVICE_STATUS_NODE, STOPPED_STATUS.getBytes(UTF_8));
			return true;
		} catch (KeeperException | InterruptedException e) {
			log.error("Could not update status node", e);
		}
		return false;
	}

	@Override
	public boolean shutdown() {
		log.info("Shutting down all servers in the KV store service");
		try {
			log.debug("Remove ECS znodes");
			zkSession.deleteNode(KV_SERVICE_MD_NODE);
			zkSession.deleteNode(KV_SERVICE_STATUS_NODE);
			zkSession.deleteNode(KV_SERVICE_LOGGING_NODE);

			heartbeatThreads.values().forEach(Thread::interrupt);

			zkSession.close();
			return true;

		} catch (KeeperException | InterruptedException e) {
			log.error("Could not remove ECS node", e);
		}
		return false;
	}

	@Override
	public synchronized IECSNode addNode(String cacheStrategy, int cacheSize) {
		Collection<IECSNode> nodes = addNodes(1, cacheStrategy, cacheSize);
		if (nodes != null && nodes.size() == 1) {
			return nodes.iterator().next();
		} else {
			return null;
		}
	}

	@Override
	public synchronized Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
		log.info("Adding " + count + " node(s) with cache strategy \"" + cacheStrategy + "\""
				+ " and cache size " + cacheSize);

		Collection<IECSNode> newNodes = setupNodes(count, cacheStrategy, cacheSize);
		if (newNodes == null) return null;

		try {
			log.debug("Updating metadata znode");
			zkSession.updateMetadataNode(topology);
		} catch (KeeperException | InterruptedException e) {
			log.error("Could not update metadata znode data", e);
		}

		Set<IECSNode> successorNodes = topology.findSuccessors(newNodes);

		// start up each server process using SSH commands
		for (IECSNode node : newNodes) {
			log.info("Initializing server: " + node);
			try {
				serverInitializer.initializeServer(node);

			} catch (ServerInitializationException e) {
				log.warn("Could not initialize server: " + node, e);
			}
		}

		// await server responses for startup
		boolean gotStartupResponse = false;
		try {
			gotStartupResponse = awaitNodes(newNodes, NODE_STARTUP_TIMEOUT);
		} catch (Exception e) {
			log.warn("Exception occured while awaiting response from " + newNodes.size() + " nodes", e);
		}
		if (!gotStartupResponse) {
			log.warn("Did not receive enough startup responses within " + NODE_STARTUP_TIMEOUT + " ms");
		}

		if (gotStartupResponse) {
			log.info("Got startup responses for startup");
		}
		
		for (IECSNode newNode : newNodes) {
			Thread t = new Thread(new ServerWatcher(newNode, this, zkSession));
			heartbeatThreads.put(newNode.getNodeName(), t);
			t.start();
		}

		// await server responses for migration
		if (!successorNodes.isEmpty()) {
			Set<IECSNode> changedNodes = Stream.concat(newNodes.stream(), successorNodes.stream())
					.collect(Collectors.toSet());
			boolean gotMigrationResponse = false;
			try {
				gotMigrationResponse = awaitNodes(changedNodes, SERVICE_RESIZE_TIMEOUT);
			} catch (Exception e) {
				log.warn("Exception occured while awaiting response from " + changedNodes.size() + " nodes", e);
			}
			if (!gotMigrationResponse) {
				log.warn("Did not receive enough migration responses within " + SERVICE_RESIZE_TIMEOUT + " ms");
			}
		}

		return newNodes;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * Nodes are selected from the default ECS configuration file located at
	 * "<code>ecs.config</code>". This file contains line-delimited server
	 * information in the format "<code>&lt;server-name&gt; &lt;hostname&gt; 
	 * &lt;port&gt;</code>", as described in the milestone specification.
	 * 
	 * @see <a href=
	 *      "https://docs.google.com/document/d/1kRy7wJzPFwvJ-03_sSDwnKS64KkxvI95QzZzCKjpP7E/edit#heading=h.hqt37rh3kfve">ECE
	 *      419 Milestone 2 - External Configuration Service (ECS)</a>
	 */
	@Override
	public synchronized Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
		log.info("Loading " + count + " node(s) from config: " + this.configFilename);

		List<IECSNode> availableNodes = new ArrayList<>();

		try (FileReader fileReader = new FileReader(this.configFilename);
				BufferedReader bufferedReader = new BufferedReader(fileReader)) {

			String line;
			while ((line = bufferedReader.readLine()) != null) {
				// each line contains information for a single server
				log.trace("Processing config line: " + line);

				try {
					// each line is formatted as "<server-name> <hostname> <port>"
					String[] tokens = line.trim().split(ECS_CONFIG_DELIMITER);
					String name = tokens[0];
					String host = tokens[1];
					int port = Integer.parseInt(tokens[2]);

					if (topology.containsNodeOfName(name)) {
						log.trace("ECS already loaded " + name + "; skipping");
					} else {
						IECSNode node = new ECSNode(name, host, port, cacheStrategy, cacheSize);
						log.debug("Loaded server from config: " + node);
						availableNodes.add(node);
					}

				} catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
					log.warn("Error parsing line: \"" + line + "\"");
				}
			}

		} catch (FileNotFoundException e) {
			log.error("Unable to find ECS config", e);
			return null;

		} catch (IOException e) {
			log.error("I/O exception while reading ECS config", e);
			return null;
		}
		log.debug(availableNodes.size() + " available machines remaining from " + this.configFilename);

		if (availableNodes.size() < count) {
			log.error("Insufficient remaining nodes available in the config for setup");
			return null;
		}

		Collection<IECSNode> selectedNodes = new ArrayList<>(availableNodes.subList(0, count));
		log.debug("Selected " + count + " nodes from " + availableNodes.size() + " available");

		// add new nodes to ECS
		log.debug("Updating ECS topology object");
		topology.addNodes(selectedNodes);
		selectedNodes.stream().forEach(node -> {
			try {
				log.debug("Creating znodes for server " + node.getNodeName());
				zkSession.createNode(ZKPathUtil.getStatusZnode(node), EPHEMERAL);
				zkSession.createNode(ZKPathUtil.getMigrationRootZnode(node), PERSISTENT);
				zkSession.createNode(ZKPathUtil.getHeartbeatZnode(node), EPHEMERAL);

			} catch (KeeperException | InterruptedException e) {
				log.warn("Could not create server znode: " + ZKPathUtil.getStatusZnode(node), e);
			}
		});

		return selectedNodes;
	}

	@Override
	public synchronized boolean removeNodes(Collection<String> nodeNames) {
		// update the topology object
		Set<IECSNode> removedNodes = topology.removeNodesByName(nodeNames);
		Set<IECSNode> successorNodes = topology.findSuccessors(removedNodes);

		/* Removed nodes will know so because they will be unable to find themselves in
		 * the updated metadata. Successor nodes will know so because they will be
		 * unable to find their immediate predecessor(s) in the updated metadata */
		try {
			zkSession.updateMetadataNode(topology);
		} catch (KeeperException | InterruptedException e) {
			log.error("Could not update metadata znode data", e);
		}

		// await responses from both removed (transferrers) and successor (receiver)
		// nodes
		Set<IECSNode> changedNodes = Stream.concat(removedNodes.stream(), successorNodes.stream())
				.collect(Collectors.toSet());
		boolean gotResponses = false;
		try {
			gotResponses = awaitNodes(changedNodes, SERVICE_RESIZE_TIMEOUT);
		} catch (InterruptedException e) {
			log.error("Interrupted while awaiting responses from " + changedNodes.size() + " nodes", e);
			return false;
		}
		if (!gotResponses) {
			log.error("Did not receive enough responses");
		}

		// signal shutdown to removed nodes
		for (IECSNode removedNode : removedNodes) {
			try {
				zkSession.deleteNode(ZKPathUtil.getStatusZnode(removedNode));
				zkSession.deleteNode(ZKPathUtil.getMigrationRootZnode(removedNode));
				zkSession.deleteNode(ZKPathUtil.getHeartbeatZnode(removedNode));
			} catch (KeeperException | InterruptedException e) {
				log.error("Could not delete znode for node " + removedNode, e);
			}
		}

		// remove heartbeat listeners for the removed nodes
		for (String nodeName : nodeNames) {
			Optional.ofNullable(heartbeatThreads.remove(nodeName)).ifPresent(Thread::interrupt);
		}

		return true;
	}

	@Override
	public boolean awaitNodes(int count, int timeout) throws Exception {
		return true;
	}

	/**
	 * Waits for responses for the given nodes. This is done by polling their
	 * individual status znodes. Once a response is received, it is cleared so that
	 * future responses can be received.
	 * 
	 * @param awaitedNodes The nodes to await responses from
	 * @param timeout The maximum amount of time to wait, in milliseconds
	 * @return <code>true</code> if all expected responses were received before the
	 *         timeout, <code>false</code> otherwise
	 * @throws InterruptedException If the calling thread is interrupted while
	 *             waiting for responses
	 */
	public boolean awaitNodes(Collection<IECSNode> awaitedNodes, int timeout) throws InterruptedException {
		final int target = awaitedNodes.size();
		AtomicInteger numResponses = new AtomicInteger(0);

		// Log info message
		StringBuilder infoMessage = new StringBuilder("Waiting for response from nodes: ");
		awaitedNodes.forEach(n -> infoMessage.append(n.getNodeName()).append(" "));
		log.info(infoMessage.toString());

		synchronized (numResponses) {
			// check each node and leave a watch
			for (IECSNode awaitedNode : awaitedNodes) {
				try {
					String statusZnode = ZKPathUtil.getStatusZnode(awaitedNode);
					IncrementWatcher watcher = new IncrementWatcher(statusZnode, zkSession, numResponses, target);
					String status = zkSession.getNodeData(statusZnode, watcher);
					if (Objects.equals(status, FINISHED)) {
						watcher.cancel();
						zkSession.updateNode(statusZnode, new byte[0]);
						numResponses.incrementAndGet();
					}
				} catch (KeeperException | InterruptedException e) {
					log.warn("Exception while reading server-ECS node", e);
				}
			}

			if (numResponses.get() < target) {
				numResponses.wait(timeout);
			}
		}

		return numResponses.get() >= target;
	}

	@Override
	public Map<String, IECSNode> getNodes() {
		return topology.getNodeMap();
	}

	@Override
	public IECSNode getNodeByKey(String key) {
		String keyHash = HashUtil.toMD5(key);
		return getNodeByHash(keyHash);
	}

	private IECSNode getNodeByHash(String hash) {
		return topology.findResponsibleServer(hash);
	}

	/**
	 * The entry point for the ECS user application.
	 * 
	 * @param args hostname, port number
	 * @see ECSAdminConsole#fromArgs(String[])
	 */
	public static void main(String[] args) {
		ECSAdminConsole ecsConsole = ECSAdminConsole.fromArgs(args);
		ecsConsole.run();
	}

}
