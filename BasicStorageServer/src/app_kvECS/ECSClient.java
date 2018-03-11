package app_kvECS;

import static common.zookeeper.ZKWrapper.KV_SERVICE_LOGGING_NODE;
import static common.zookeeper.ZKWrapper.KV_SERVICE_ROOT_NODE;
import static common.zookeeper.ZKWrapper.KV_SERVICE_STATUS_NODE;
import static common.zookeeper.ZKWrapper.RUNNING_STATUS;
import static common.zookeeper.ZKWrapper.STOPPED_STATUS;
import static common.zookeeper.ZKWrapper.UTF_8;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import common.HashUtil;
import common.zookeeper.ZKWrapper;
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

	// Logger constants
	private static final String PROMPT = "kvECS> ";
	private static final String CONSOLE_PATTERN = PROMPT + "%m%n";

	// ECS bootstrap configuration file constants
	private static final String ECS_CONFIG_FILENAME = "ecs.config";
	private static final String ECS_CONFIG_DELIMITER = " ";

	// Server startup script
	private static final String SERVER_INITIALIZATION_COMMAND = "ssh -n %s nohup java -jar ms2-server.jar %s %d &";

	// Timeout constants
	private static final int NODE_STARTUP_TIMEOUT = 15 * 1000;
	private static final int SERVICE_RESIZE_TIMEOUT = 30 * 1000;

	// ZooKeeper fields
	private final String zkHostname;
	private final int zkPort;
	private ZKWrapper zkWrapper;

	// Server metadata fields
	private Map<String, IECSNode> nodes = null;
	private NavigableMap<String, IECSNode> hashRing = new TreeMap<>();

	/**
	 * Initializes an ECS service and connects it to the specified
	 * ZooKeeper service using the default wrapper.
	 * 
	 * @param zkHostname The hostname used by the ZooKeeper service
	 * @param zkPort The port number used by the ZooKeeper service
	 */
	public ECSClient(String zkHostname, int zkPort) {
		this(zkHostname, zkPort, new ZKWrapper(zkHostname, zkPort));
	}

	/**
	 * Initializes an ECS service and connects it to the specified ZooKeeper service
	 * using the specified wrapper. Provided in order to allow for mocking.
	 * 
	 * @param zkHostname The hostname used by the ZooKeeper service
	 * @param zkPort The port number used by the ZooKeeper service
	 * @param zkWrapper The wrapper class used to access ZooKeeper functionality
	 */
	public ECSClient(String zkHostname, int zkPort, ZKWrapper zkWrapper) {
		this.zkHostname = zkHostname;
		this.zkPort = zkPort;
		this.zkWrapper = zkWrapper;

		try {
			this.zkWrapper.createNode(KV_SERVICE_ROOT_NODE);
			this.zkWrapper.createNode(KV_SERVICE_STATUS_NODE, STOPPED_STATUS.getBytes(UTF_8));
			this.zkWrapper.createNode(KV_SERVICE_LOGGING_NODE, "ERROR".getBytes(UTF_8));
			this.zkWrapper.createMetadataNode(nodes);

		} catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
			log.error("Could not create ECS nodes", e);
		}

	}

	/**
	 * Uses SSH to instantiate a server process using the given server metadata.
	 * 
	 * @param node The metadata for the server to initialize
	 */
	private void startupNode(IECSNode node) {
		String command = String.format(SERVER_INITIALIZATION_COMMAND, node.getNodeHost(), zkHostname, zkPort);

		log.info("Starting server using SSH: " + command);
		try {
			Runtime.getRuntime().exec(command);
			log.info("KVServer process started");
		} catch (IOException e) {
			log.error("I/O exception while executing node startup command", e);
		}
	}

	@Override
	public boolean start() {
		log.info("Starting all servers in the KV store service");
		try {
			zkWrapper.updateNode(KV_SERVICE_STATUS_NODE, RUNNING_STATUS.getBytes(UTF_8));
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
			zkWrapper.updateNode(KV_SERVICE_STATUS_NODE, STOPPED_STATUS.getBytes(UTF_8));
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
			zkWrapper.deleteNode(KV_SERVICE_ROOT_NODE);
			zkWrapper.close();
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
		Collection<IECSNode> nodes = setupNodes(count, cacheStrategy, cacheSize);
		if (nodes == null) return null;

		// start up each server process using SSH commands
		for (IECSNode node : nodes) {
			startupNode(node);
		}

		// await server response
		boolean gotResponse = false;
		try {
			gotResponse = awaitNodes(count, NODE_STARTUP_TIMEOUT);
		} catch (Exception e) {
			log.warn("Exception occured while awaiting response from " + count + " nodes", e);
		}
		if (!gotResponse) {
			log.warn("Did not receive enough responses within " + NODE_STARTUP_TIMEOUT + " ms");
		}

		return nodes;
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

		log.info("Loading " + count + " nodes with cache strategy " + cacheStrategy + " and cache size " + cacheSize
				+ " from config: " + ECS_CONFIG_FILENAME);

		try (FileReader fileReader = new FileReader(ECS_CONFIG_FILENAME);
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

					if (nodes.containsKey(name)) {
						log.debug("ECS already loaded " + name + "; skipping");
					} else {
						IECSNode node = new ECSNode(name, host, port, cacheStrategy, cacheSize);
						log.info("Loaded server from config: " + node);
						availableNodes.add(node);
					}

				} catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
					log.warn("Error parsing line; skipping to next line");
				}
			}

		} catch (FileNotFoundException e) {
			log.error("Unable to find ECS config", e);
			return null;
		} catch (IOException e) {
			log.error("I/O exception while reading ECS config", e);
			return null;
		}
		log.info("Loaded information for " + availableNodes.size() + " machines from ECS config");

		if (availableNodes.size() < count) {
			log.error("Insufficient remaining nodes available in the config for setup");
			return null;
		}

		// note that node selection is not deterministic due to using default source of
		// randomness
		Collections.shuffle(availableNodes);
		Collection<IECSNode> selectedNodes = new ArrayList<>(availableNodes.subList(0, count));
		log.info("Selected " + count + " nodes randomly from " + availableNodes.size() + " available");

		// add new nodes to ECS
		selectedNodes.stream().forEach(node -> {
			nodes.put(node.getNodeName(), node);
			hashRing.put(node.getNodeHashRangeStart(), node);

			// create a node for communications between the ECS and this node
			try {
				zkWrapper.createNode(node.getBaseNodePath());
				zkWrapper.createNode(node.getECSNodePath());
			} catch (KeeperException | InterruptedException e) {
				log.error("Could not create ZNode with path " + node.getECSNodePath(), e);
			}
		});
		updateHashRanges();

		return selectedNodes;
	}

	@Override
	public boolean awaitNodes(int count, int timeout) throws Exception {
		AtomicInteger numResponses = new AtomicInteger(0);
		List<IECSNode> awaitedNodes = new ArrayList<>(
				nodes.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList()));

		// TODO improve parallelism (maybe with watches?)
		Thread awaitThread = new Thread(() -> {
			// poll nodes for status
			while (awaitedNodes.size() > 0 && numResponses.get() < count) {
				Iterator<IECSNode> it = awaitedNodes.iterator();
				while (it.hasNext()) {
					IECSNode node = it.next();
					String ecsNodePath = node.getECSNodePath();
					try {
						String data = zkWrapper.getNodeData(ecsNodePath);
						if (Objects.equals(data, "FINISHED")) {
							numResponses.incrementAndGet();
							zkWrapper.updateNode(ecsNodePath, new byte[0]);
							it.remove();
						}
					} catch (KeeperException | InterruptedException e) {
						log.warn("Exception while reading server-ECS node", e);
					}
				}
			}
		});
		awaitThread.start();
		awaitThread.join(timeout);

		return numResponses.get() >= count;
	}

	@Override
	public boolean removeNodes(Collection<String> nodeNames) {
		Set<IECSNode> removedNodes = new HashSet<>();
		Set<IECSNode> successorNodes = new HashSet<>();

		for (String nodeName : nodeNames) {

			// check if node is loaded in the ECS
			if (!nodes.containsKey(nodeName)) {
				log.warn("No node with name \"" + nodeName + "\" found");
				continue;
			}

			// remove node from the ECS
			IECSNode node = nodes.remove(nodeName);
			String nodeHash = node.getNodeHashRangeStart();
			hashRing.remove(nodeHash);
			successorNodes.remove(node);
			removedNodes.add(node);

			IECSNode successorNode = getNodeByHash(nodeHash);
			if (successorNode != null) {
				successorNodes.add(successorNode);
			}
		}

		/* Removed nodes will know so because they will be unable to find themselves in
		 * the updated metadata. Successor nodes will know so because they will be
		 * unable to find their immediate predecessor(s) in the updated metadata */
		updateHashRanges();

		// await responses from removed and successor nodes
		int numNodesChanged = removedNodes.size() + successorNodes.size();
		boolean gotResponses = false;
		try {
			gotResponses = awaitNodes(numNodesChanged, SERVICE_RESIZE_TIMEOUT);
		} catch (Exception e) {
			log.error("Exception occured while awaiting responses from " + numNodesChanged, e);
			return false;
		}
		if (!gotResponses) {
			log.error("Did not receive enough responses");
		}

		// signal shutdown to removed nodes
		for (IECSNode removedNode : removedNodes) {
			try {
				zkWrapper.deleteNode(removedNode.getBaseNodePath());
			} catch (KeeperException | InterruptedException e) {
				log.error("Could not delete ZNode for node " + removedNode, e);
			}
		}

		return true;

	}

	@Override
	public Map<String, IECSNode> getNodes() {
		return nodes;
	}

	@Override
	public IECSNode getNodeByKey(String key) {
		String keyHash = HashUtil.toMD5(key);
		return getNodeByHash(keyHash);
	}

	private IECSNode getNodeByHash(String hash) {
		if (hashRing.isEmpty()) return null;

		return Optional
				// find the first server past the key in the hash ring
				.ofNullable(hashRing.ceilingEntry(hash))
				.map(Map.Entry::getValue)

				// otherwise use the first server (guaranteed to exist because of above check)
				.orElse(hashRing.firstEntry().getValue());
	}

	/**
	 * Updates the end values for the hash ranges of all active nodes in the ECS and
	 * records this in the central metadata ZNode. The end value of any given node
	 * is simply the start value of the preceding node on the hash ring.
	 */
	private void updateHashRanges() {
		if (hashRing.isEmpty()) return; // nothing to do

		String prevStart = hashRing.lastEntry().getValue().getNodeHashRangeStart();
		for (Map.Entry<String, IECSNode> hashRingEntry : hashRing.entrySet()) {

			/* This node is the same node that is referenced in the nodes map,
			 * thus changes made to it here are reflected both there and in hashRing */
			IECSNode node = hashRingEntry.getValue();

			node.setNodeHashRangeEnd(prevStart);
			prevStart = node.getNodeHashRangeStart();
		}

		try {
			zkWrapper.updateMetadataNode(nodes);
		} catch (KeeperException | InterruptedException e) {
			log.error("Could not update metadata ZNode data", e);
		}
	}

	private void runAdminConsole() {
		// TODO Auto-generated method stub
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

		ECSClient clientApplication = new ECSClient(zkHostname, zkPort);
		clientApplication.runAdminConsole();
	}

}
