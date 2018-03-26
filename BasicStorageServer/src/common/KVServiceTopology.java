package common;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import ecs.IECSNode;

/**
 * Represents the a configuration for all servers within a key-value store
 * service. The topology is shaped as a hash ring, with each server situated on
 * the hash ring in a location determined by the MD5 hash of their listening
 * port address.
 */
public class KVServiceTopology {

	private static final Logger log = Logger.getLogger(KVServiceTopology.class);

	private Map<String, IECSNode> nodeMap = new HashMap<>();
	private NavigableMap<String, IECSNode> hashring = new TreeMap<>();

	/**
	 * Creates an empty service topology.
	 */
	public KVServiceTopology() {}

	/**
	 * Creates a service topology containing the specified nodes.
	 * 
	 * @param nodes The initial servers to include in the topology
	 */
	public KVServiceTopology(Collection<IECSNode> nodes) {
		addNodes(nodes);
	}

	/**
	 * Retrieves the server metadata associated with the specified start hash.
	 * 
	 * @param name The name of the server to retrieve
	 * @return The server associated with the given name,
	 *         or <code>null</code> if no such server exists
	 */
	public IECSNode getNodeOfName(String name) {
		return nodeMap.get(name);
	}

	/**
	 * Checks whether this topology contains a server with the given name.
	 * 
	 * @param name The name of the server to check for
	 * @return <code>true</code> if a server with name <code>name</code> exists in
	 *         the topology, <code>false</code> otherwise
	 */
	public boolean containsNodeOfName(String name) {
		return nodeMap.containsKey(name);
	}

	/**
	 * Retrieves the server metadata associated with the specified start hash.
	 * 
	 * @param hash The MD5 hash of the server to retrieve
	 * @return The server associated with the given hash,
	 *         or <code>null</code> if no such server exists
	 */
	public IECSNode getNodeOfHash(String hash) {
		return hashring.get(hash);
	}

	/**
	 * Retrieves metadata for all servers in the service as a set.
	 * 
	 * @return A set containing metadata for all servers
	 */
	public Set<IECSNode> getNodeSet() {
		return new HashSet<>(hashring.values());
	}

	/**
	 * Retrieves metadata for all servers in the service as a map.
	 * 
	 * @return A map containing metadata for each server mapped to their name
	 */
	public Map<String, IECSNode> getNodeMap() {
		return nodeMap;
	}

	/**
	 * Adds or updates metadata for the given server to the topology without
	 * updating hash ranges.
	 * 
	 * @param node The node to add or updated
	 */
	public void updateNode(IECSNode node) {
		nodeMap.put(node.getNodeName(), node);
		hashring.put(node.getNodeHashRangeStart(), node);
	}

	/**
	 * Adds the given nodes to the topology and updates hash ranges.
	 * 
	 * @param nodes The metadata for the nodes to add
	 */
	public void addNodes(Collection<IECSNode> nodes) {
		if (nodes == null) return;
		
		int prevSize = nodeMap.size();
		for (IECSNode node : nodes) {
			updateNode(node);
		}
		updateHashRanges();
		if (log.isDebugEnabled()) {
			int numAdded = nodeMap.size() - prevSize;
			int numUpdated = nodes.size() - numAdded;
			log.trace("Added " + numAdded + " server(s); updated " + numUpdated + " server(s)");
		}
	}

	/**
	 * Removes the given node from the topology without updating hash ranges.
	 * 
	 * @param node The node to remove
	 */
	public void invalidateNode(IECSNode node) {
		nodeMap.remove(node.getNodeName());
		hashring.remove(node.getNodeHashRangeStart());
	}

	/**
	 * Removes the nodes corresponding to the given names from the topology and
	 * updates hash ranges.
	 * 
	 * @param nodeNames The names of the nodes to remove
	 * @return The metadata for the nodes that were actually removed
	 */
	public Set<IECSNode> removeNodesByName(Collection<String> nodeNames) {
		Set<IECSNode> removedNodes = new HashSet<>();

		for (String nodeName : nodeNames) {
			if (!nodeMap.containsKey(nodeName)) {
				log.warn("No node with name " + nodeName + " exists in the service; could not remove");
				break;
			}
			IECSNode node = nodeMap.remove(nodeName);
			String nodeHash = node.getNodeHashRangeStart();
			hashring.remove(nodeHash);
			removedNodes.add(node);
		}
		updateHashRanges();
		return removedNodes;
	}

	/**
	 * Identifies the immediate successor for the specified hash value according to
	 * the cached service topology.
	 * 
	 * @param hash The MD5 hash of the object to find the successor for
	 * @return The successor for the given hash value,
	 *         or <code>null</code> if the topology is empty
	 */
	public IECSNode findSuccessor(String hash) {
		if (hashring.isEmpty())
			return null;

		return Optional.ofNullable(hashring.higherEntry(hash))
				.orElse(hashring.firstEntry())
				.getValue();
	}

	/**
	 * Identifies the set of immediate successors for the given nodes. Results do
	 * not include the given nodes themselves.
	 * 
	 * @param nodes The nodes to find successors for
	 * @return A set containing the immediate successors for each of the nodes given
	 */
	public Set<IECSNode> findSuccessors(Collection<IECSNode> nodes) {
		Set<IECSNode> successors = new HashSet<>();
		Set<IECSNode> differenceSet = this.getNodeSet();
		differenceSet.removeAll(nodes);
		KVServiceTopology differenceTopology = new KVServiceTopology(differenceSet);

		for (IECSNode node : nodes) {
			String hash = node.getNodeHashRangeStart();
			IECSNode successor = differenceTopology.findSuccessor(hash);
			if (successor != null) successors.add(successor);
		}
		
		return successors;
	}

	/**
	 * Identifies the immediate predecessor for the specified hash value according
	 * to the cached service topology.
	 * 
	 * @param hash The MD5 hash of the object to find the predecessor for
	 * @return The predecessor for the given hash value
	 */
	public IECSNode findPredecessor(String hash) {
		if (hashring.isEmpty())
			return null;

		return Optional.ofNullable(hashring.lowerEntry(hash))
				.orElse(hashring.lastEntry())
				.getValue();
	}

	/**
	 * Identifies the server which is responsible for the given hash, given the
	 * information available to this cache.
	 * 
	 * @param keyHash The MD5 hash for the key to find a server for
	 * @return The server matching the key,
	 *         or <code>null</code> if the cache is empty
	 */
	public IECSNode findResponsibleServer(String keyHash) {
		if (hashring.isEmpty())
			return null;

		return Optional
				// find the first server past the key in the hash ring
				.ofNullable(hashring.ceilingEntry(keyHash))

				// otherwise use the first server (guaranteed to exist because of above check)
				.orElse(hashring.firstEntry()).getValue();
	}

	/**
	 * Updates the end values for the hash ranges of all active nodes in the ECS and
	 * records this in the central metadata znode. The end value of any given node
	 * is simply the start value of the preceding node on the hash ring.
	 */
	protected void updateHashRanges() {
		if (hashring.isEmpty()) return; // nothing to do

		String prevStart = hashring.lastEntry().getValue().getNodeHashRangeStart();
		for (Map.Entry<String, IECSNode> hashRingEntry : hashring.entrySet()) {

			/* This node is the same node that is referenced in the nodes map,
			 * thus changes made to it here are reflected both there and in hashRing */
			IECSNode node = hashRingEntry.getValue();

			node.setNodeHashRangeEnd(prevStart);
			prevStart = node.getNodeHashRangeStart();
		}

	}

}
