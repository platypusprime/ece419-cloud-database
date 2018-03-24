package app_kvECS;

import static common.zookeeper.ZKSession.KV_SERVICE_LOGGING_NODE;
import static common.zookeeper.ZKSession.KV_SERVICE_MD_NODE;
import static common.zookeeper.ZKSession.KV_SERVICE_STATUS_NODE;
import static common.zookeeper.ZKSession.RUNNING_STATUS;
import static common.zookeeper.ZKSession.STOPPED_STATUS;
import static common.zookeeper.ZKSession.UTF_8;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import common.HashUtil;
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

	// Logger constants
	private static final String PROMPT = "kvECS> ";
	private static final String CONSOLE_PATTERN = PROMPT + "%m%n";

	// ECS bootstrap configuration file constants
	private static final String ECS_CONFIG_FILENAME = "ecs.config";
	private static final String ECS_CONFIG_DELIMITER = " ";

	// Timeout constants
	private static final int NODE_STARTUP_TIMEOUT = 15 * 1000;
	private static final int SERVICE_RESIZE_TIMEOUT = 30 * 1000;

	private ZKSession zkSession;
	private final ServerInitializer serverInitializer;

	// Server metadata fields
	private Map<String, IECSNode> nodes = new HashMap<>();
	private NavigableMap<String, IECSNode> hashRing = new TreeMap<>();

	/**
	 * Initializes an ECS service and connects it to the specified
	 * ZooKeeper service using the default wrapper.
	 * 
	 * @param zkHostname The hostname used by the ZooKeeper service
	 * @param zkPort The port number used by the ZooKeeper service
	 */
	public ECSClient(String zkHostname, int zkPort) {
		this(new ZKSession(zkHostname, zkPort), new SshServerInitializer(zkHostname, zkPort));
	}

	/**
	 * Initializes an ECS service and connects it to the specified ZooKeeper service
	 * using the specified wrapper. Provided in order to allow for mocking.
	 * 
	 * @param zkWrapper The wrapper class used to access ZooKeeper functionality
	 * @param serverInitializer The initializer responsible for starting up servers
	 */
	public ECSClient(ZKSession zkWrapper, ServerInitializer serverInitializer) {
		this.zkSession = zkWrapper;
		this.serverInitializer = serverInitializer;

		try {
			this.zkSession.createNode(KV_SERVICE_STATUS_NODE, STOPPED_STATUS.getBytes(UTF_8));
			this.zkSession.createNode(KV_SERVICE_LOGGING_NODE, "ERROR".getBytes(UTF_8));
			this.zkSession.createMetadataNode(nodes);

		} catch (KeeperException | InterruptedException | UnsupportedEncodingException e) {
			log.error("Exception while creating global ZNodes", e);
		}

	}

	@Override
	public boolean start() {
		log.info("Starting all servers in the KV store service");
		try {
			zkSession.updateNode(KV_SERVICE_STATUS_NODE, RUNNING_STATUS.getBytes(UTF_8));
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
			zkSession.updateNode(KV_SERVICE_STATUS_NODE, STOPPED_STATUS.getBytes(UTF_8));
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
			zkSession.deleteNode(KV_SERVICE_LOGGING_NODE);
			zkSession.deleteNode(KV_SERVICE_STATUS_NODE);
			zkSession.deleteNode(KV_SERVICE_MD_NODE);
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
		Collection<IECSNode> nodes = setupNodes(count, cacheStrategy, cacheSize);
		if (nodes == null) return null;

		// start up each server process using SSH commands
		for (IECSNode node : nodes) {
			try {
				serverInitializer.initializeServer(node);
			} catch (ServerInitializationException e) {
				log.warn("Could not initialize server", e);
			}
		}

		// await server response
		boolean gotResponse = false;
		try {
			gotResponse = awaitNodes(nodes, NODE_STARTUP_TIMEOUT);
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

		Collection<IECSNode> selectedNodes = new ArrayList<>(availableNodes.subList(0, count));
		log.info("Selected " + count + " nodes from " + availableNodes.size() + " available");

		// add new nodes to ECS
		selectedNodes.stream().forEach(node -> {
			nodes.put(node.getNodeName(), node);
			hashRing.put(node.getNodeHashRangeStart(), node);

			try {
				// Create a node for communications between the ECS and this node
				zkSession.createNode(node.getBaseNodePath());
				zkSession.createNode(node.getECSNodePath());

				// Create nodes for migrating and replicating data to and from other servers
				zkSession.createNode(node.getMigrationNodePath());
				zkSession.createNode(node.getReplicationNodePath());

			} catch (KeeperException | InterruptedException e) {
				log.error("Could not create ZNode with path " + node.getECSNodePath(), e);
			}
		});
		updateHashRanges();

		return selectedNodes;
	}

	@Override
	public boolean awaitNodes(int count, int timeout) throws Exception {
		return true;
	}

	public boolean awaitNodes(Collection<IECSNode> awaitedN, int timeout) throws Exception {
		// Create copy in order to not modify the original one
		Collection<IECSNode> awaitedNodes = new HashSet<>(awaitedN);

		int count = awaitedNodes.size();
		AtomicInteger numResponses = new AtomicInteger(0);

		// Log info message
		StringBuilder infoMessage = new StringBuilder("Waiting for response from nodes: ");
		awaitedNodes.forEach(n -> infoMessage.append(n.getNodeName()).append(" "));
		log.info(infoMessage.toString());

		// TODO improve parallelism (maybe with watches?)
		Thread awaitThread = new Thread(() -> {
			// poll nodes for status
			while (awaitedNodes.size() > 0 && numResponses.get() < count) {
				Iterator<IECSNode> it = awaitedNodes.iterator();
				while (it.hasNext()) {
					IECSNode node = it.next();
					String ecsNodePath = node.getBaseNodePath();
					try {
						String data = zkSession.getNodeData(ecsNodePath);
						if (Objects.equals(data, "FINISHED")) {
							numResponses.incrementAndGet();
							log.info("Received response from node " + node.getNodeName());
							zkSession.updateNode(ecsNodePath, new byte[0]);
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
			gotResponses = awaitNodes(removedNodes, SERVICE_RESIZE_TIMEOUT);
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
				zkSession.deleteNode(removedNode.getMigrationNodePath());
				zkSession.deleteNode(removedNode.getReplicationNodePath());
				zkSession.deleteNode(removedNode.getECSNodePath());
				zkSession.deleteNode(removedNode.getBaseNodePath());
				log.info("Removed ZNode " + removedNode.getBaseNodePath());
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
			zkSession.updateMetadataNode(nodes);
		} catch (KeeperException | InterruptedException e) {
			log.error("Could not update metadata ZNode data", e);
		}
	}

	/**
	 * Starts the admin CLI standard input loop.
	 */
	private void runAdminConsole() {
		try (BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in))) {
			log.info("Welcome to kvECS application");
			boolean stop = false;
			while (!stop) {
				System.out.print(PROMPT);

				try {
					String ln = stdin.readLine();
					stop = handleCommand(ln);

				} catch (IOException e) {
					stop = true;
					log.warn("IO exception while reading from standard input", e);
				}
			}

		} catch (IOException e) {
			log.fatal("IO exception while closing standard input reader", e);
		}

		log.info("KVECS shutting down");
		shutdown();
	}

	/**
	 * Interprets a single command from the admin CLI.
	 * 
	 * @param ln The command to process
	 * @return <code>true</code> if execution should stop after the current command,
	 *         <code>false</code> otherwise
	 */
	public boolean handleCommand(String ln) {
		if (ln == null || ln.isEmpty()) {
			log.warn("Please input a command;");
			return false;
		}

		String[] tokens = ln.trim().split("\\s+");

		if (tokens.length < 1) {
			log.warn("Empty command");
			printHelp();

		} else if (tokens[0].equals("start")) {
			start();

		} else if (tokens[0].equals("stop")) {
			stop();

		} else if (tokens[0].equals("shutdown")) {
			return true;

		} else if (tokens[0].equals("add")) {
			if (tokens.length == 3) {
				addNode(tokens[1], Integer.parseInt(tokens[2]));
			} else if (tokens.length == 4) {
				addNodes(Integer.parseInt(tokens[1]), tokens[2], Integer.parseInt(tokens[3]));
			} else {
				log.error("Invalid number of arguments (usage: add <count>(optional) <cacheStrategy> <cacheSize>)");
			}

		} else if (tokens[0].equals("remove")) {
			if (tokens.length > 1) {
				String[] names = Arrays.copyOfRange(tokens, 1, tokens.length);
				removeNodes(Arrays.asList(names));
			} else {
				log.error("Invalid number of arguments (usage: remove <serverName1> <serverName2> ...)");
			}

		} else if (tokens[0].equals("help")) {
			printHelp();

		} else {
			log.warn("Unknown command");
			printHelp();
		}

		return false;
	}

	/**
	 * Prints the help message for the CLI.
	 */
	private void printHelp() {
		log.info("");
		log.info("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
		log.info("KV ECS HELP (Usage):");
		log.info("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
		log.info("");
		log.info("Procedure: add -> start -> add/remove/stop/shutdown");
		log.info("");
		log.info("add <count>(optional) <cacheStrategy> <cacheSize>");
		log.info("\t\tStarts up <count> servers (or 1, if <count> is not specified)");
		log.info("");
		log.info("remove <serverName1> <serverName2> ...");
		log.info("\t\tRemoves nodes with given names");
		log.info("");
		log.info("start");
		log.info("\t\tStarts all storage servers, opening them for client requests");
		log.info("");
		log.info("stop");
		log.info("\t\tStops all storage servers, but does not end their processes");
		log.info("");
		log.info("shutdown");
		log.info("\t\tShuts down the storage service, ending all processes and quitting the application");
		log.info("");
		log.info("help");
		log.info("\t\tShows this message");
		log.info("");
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
