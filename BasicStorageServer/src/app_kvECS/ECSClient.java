package app_kvECS;

import static app_kvECS.ECSAdminConsole.CONSOLE_PATTERN;
import static common.zookeeper.ZKSession.FINISHED;
import static common.zookeeper.ZKSession.RUNNING_STATUS;
import static common.zookeeper.ZKSession.STOPPED_STATUS;
import static common.zookeeper.ZKSession.UTF_8;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import logger.LogSetup;

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

	/** heartbeatWatcher thread */
	private Thread heartbeatWatcher;
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
		this.zkSession = zkSession;
		this.serverInitializer = serverInitializer;
		this.configFilename = configFilename;

		try {
			this.zkSession.createNode(ZKPathUtil.KV_SERVICE_MD_NODE);
			this.zkSession.createNode(ZKPathUtil.KV_SERVICE_STATUS_NODE, STOPPED_STATUS.getBytes(UTF_8));
			this.zkSession.createNode(ZKPathUtil.KV_SERVICE_LOGGING_NODE, Level.ERROR.toString().getBytes(UTF_8));
			this.heartbeatWatcher = new Thread(new ServerWatcher(this.zkSession, this));

		} catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
			log.error("Exception while creating global znodes", e);
		}

	}

	@Override
	public boolean start() {
		log.info("Starting all servers in the KV store service");
		try {
			heartbeatWatcher.start();
			zkSession.updateNode(ZKPathUtil.KV_SERVICE_STATUS_NODE, RUNNING_STATUS.getBytes(UTF_8));
			return true;
		} catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
			log.error("Could not update status node", e);
		}
		return false;
	}

	@Override
	public boolean stop() {
		log.info("Stopping all servers in the KV store service");
		try {
			zkSession.updateNode(ZKPathUtil.KV_SERVICE_STATUS_NODE, STOPPED_STATUS.getBytes(UTF_8));
			return true;
		} catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
			log.error("Could not update status node", e);
		}
		return false;
	}

	@Override
	public boolean shutdown() {
		log.info("Shutting down all servers in the KV store service");
		try {
			// each server is responsible for removing its own ZNodes at this point
			zkSession.deleteNode(ZKPathUtil.KV_SERVICE_MD_NODE);
			zkSession.deleteNode(ZKPathUtil.KV_SERVICE_STATUS_NODE);
			zkSession.deleteNode(ZKPathUtil.KV_SERVICE_LOGGING_NODE);
			heartbeatWatcher.interrupt();
			zkSession.close();
			return true;

		} catch (KeeperException | InterruptedException e) {
			log.error("Could not remove ECS node", e);
		}
		return false;
	}

	@Override
	public IECSNode addNode(String cacheStrategy, int cacheSize) {
		Collection<IECSNode> nodes = addNodes(1, cacheStrategy, cacheSize);
		if (nodes != null && nodes.size() == 1) {
			return nodes.iterator().next();
		} else {
			return null;
		}
	}

	@Override
	public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
		Collection<IECSNode> newNodes = setupNodes(count, cacheStrategy, cacheSize);
		if (newNodes == null) return null;

		try {
			zkSession.updateMetadataNode(topology);
			log.info("Updated metadata node");
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
		
		if (successorNodes.isEmpty()) {
			for (IECSNode newNode : newNodes) {
				try {
					String migrationNode = zkSession.createMigrationZnode("ecs", newNode.getNodeName());
					zkSession.updateNode(migrationNode, FINISHED);
				} catch (KeeperException | InterruptedException e) {
					log.error("Could not signal initial migration completion to new nodes");
				}
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
			log.warn("Did not receive enough responses within " + NODE_STARTUP_TIMEOUT + " ms");
		}

		// await server responses for migration
		Set<IECSNode> changedNodes = Stream.concat(newNodes.stream(), successorNodes.stream())
				.collect(Collectors.toSet());
		boolean gotMigrationResponse = false;
		try {
			gotMigrationResponse = awaitNodes(changedNodes, NODE_STARTUP_TIMEOUT);
		} catch (Exception e) {
			log.warn("Exception occured while awaiting response from " + changedNodes.size() + " nodes", e);
		}
		if (!gotMigrationResponse) {
			log.warn("Did not receive enough responses within " + NODE_STARTUP_TIMEOUT + " ms");
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
	public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
		List<IECSNode> availableNodes = new ArrayList<>();

		log.info("Loading " + count + " node(s) with cache strategy \"" + cacheStrategy + "\" and cache size "
				+ cacheSize + " from config: " + this.configFilename);

		try (FileReader fileReader = new FileReader(this.configFilename);
				BufferedReader bufferedReader = new BufferedReader(fileReader)) {

			String line;
			while ((line = bufferedReader.readLine()) != null) {
				// each line contains information for a single server
				log.debug("Processing config line: " + line);

				try {
					// each line is formatted as "<server-name> <hostname> <port>"
					String[] tokens = line.trim().split(ECS_CONFIG_DELIMITER);
					String name = tokens[0];
					String host = tokens[1];
					int port = Integer.parseInt(tokens[2]);

					if (topology.containsNodeOfName(name)) {
						log.debug("ECS already loaded " + name + "; skipping");
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
		log.info(availableNodes.size() + " machines available from ECS config");

		if (availableNodes.size() < count) {
			log.error("Insufficient remaining nodes available in the config for setup");
			return null;
		}

		Collection<IECSNode> selectedNodes = new ArrayList<>(availableNodes.subList(0, count));
		log.info("Selected " + count + " nodes from " + availableNodes.size() + " available");

		// add new nodes to ECS
		topology.addNodes(selectedNodes);
		selectedNodes.stream().forEach(node -> {
			try {
				// Create a node for communications between the ECS and this node
				zkSession.createNode(ZKPathUtil.getStatusZnode(node));

				// Create nodes for migrating and replicating data to and from other servers
				zkSession.createNode(ZKPathUtil.getMigrationRootZnode(node));
				zkSession.createNode(ZKPathUtil.getReplicationRootZnode(node));

				// Create nodes for heartbeat message
				zkSession.createNode(ZKPathUtil.getHeartbeatZnode(node));

			} catch (KeeperException | InterruptedException e) {
				log.error("Could not create znode with path " + ZKPathUtil.getStatusZnode(node), e);
			}
		});

		return selectedNodes;
	}

	@Override
	public boolean removeNodes(Collection<String> nodeNames) {
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
				zkSession.deleteNode(ZKPathUtil.getReplicationRootZnode(removedNode));
			} catch (KeeperException | InterruptedException e) {
				log.error("Could not delete znode for node " + removedNode, e);
			}
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
	 * Initializes the ESC with the ZooKeeper service specified in the command line
	 * arguments and launches the KV Service admin console.
	 * 
	 * @param args Expects 2 arguments, with the first being the hostname of the
	 *            ZooKeeper service and the second being the port number of the
	 *            ZooKeeper service
	 */
	public static void main(String[] args) {
		String zkHostname;
		int zkPort;

		// read command-line arguments
		if (args.length != 2) {
			System.out.println("Error! Invalid number of arguments!");
			System.out.println("Usage: m2-server <zkHostname> <zkPort>");
			System.exit(1);
		}
		zkHostname = args[0];
		if (!args[1].matches("\\d+")) {
			System.out.println("<zkPort> is not an integer");
			System.exit(1);
		}
		zkPort = Integer.parseInt(args[1]);

		// initialize logging
		try {
			LogSetup.initialize("logs/ecs.log", Level.INFO, CONSOLE_PATTERN);
		} catch (IOException e) {
			System.out.println("ERROR: unable to initialize logger");
			e.printStackTrace();
			System.exit(1);
		}

		ECSClient ecsClient = new ECSClient(zkHostname, zkPort);
		ECSAdminConsole ecsConsole = new ECSAdminConsole(ecsClient);
		ecsConsole.run();
	}

}
