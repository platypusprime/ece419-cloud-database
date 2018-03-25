package app_kvServer;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import common.KVServiceTopology;
import ecs.IECSNode;

/**
 * Provides the functionality for identifying changes in topology between two
 * snapshots of the cloud database service configuration.
 */
public class TopologyChange {

	/** The change in the number of elements from the old to new node set. */
	public final int delta;

	/** The set of added/removed nodes. */
	private final Set<IECSNode> diffSet;

	/**
	 * Creates a change object from one set of nodes to another. Assumes that nodes
	 * are strictly added or removed, never both.
	 * 
	 * @param previousNodes The cached node set
	 * @param currentNodes The updated node set
	 */
	public TopologyChange(Set<IECSNode> previousNodes, Set<IECSNode> currentNodes) {
		this.delta = currentNodes.size() - previousNodes.size();

		if (this.delta == 0) {
			this.diffSet = Collections.emptySet();

		} else if (this.delta < 0) {
			this.diffSet = findDifference(previousNodes, currentNodes);

		} else {
			this.diffSet = findDifference(currentNodes, previousNodes);
		}
	}

	/**
	 * Finds the nodes which are present in the larger set but absent in the
	 * smaller set.
	 * 
	 * @param superset The larger set
	 * @param subset The smaller set
	 * @return The diff set
	 */
	private Set<IECSNode> findDifference(Set<IECSNode> superset, Set<IECSNode> subset) {
		return superset.stream()
				.filter(node -> !subset.contains(node))
				.collect(Collectors.toSet());
	}

	/**
	 * Checks whether the given node is part of the change diff.
	 * 
	 * @param node The node to check
	 * @return <code>true</code> if the given node is contained in exactly one of
	 *         either the new or old node sets, <code>false</code> otherwise
	 */
	public boolean diffContains(IECSNode node) {
		return diffSet.contains(node);
	}

	/**
	 * Finds the added/deleted nodes that are direct predecessors to the given node.
	 * 
	 * @param topology The topology of the node set still containing the diff set.
	 *            For added nodes, this would be the new topology; for deleted
	 *            nodes, the old topology.
	 * @param node The node to find changed predecessors for
	 * @return A set containing changed predecessors
	 */
	public Set<IECSNode> getChangedPredecessors(KVServiceTopology topology, IECSNode node) {
		String hash = node.getNodeHashRangeStart();
		Set<IECSNode> changedPredecessors = new HashSet<>();

		IECSNode currentPredecessor = topology.findPredecessor(hash);
		while (diffSet.contains(currentPredecessor)) {
			changedPredecessors.add(currentPredecessor);
			currentPredecessor = topology.findPredecessor(currentPredecessor.getNodeHashRangeStart());
		}

		return changedPredecessors;
	}

}