package app_kvServer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import common.zookeeper.ZKWrapper;
import ecs.IECSNode;

/**
 * Document me.
 */
public class ServiceTopologyWatcher implements Watcher {

	private static final Logger log = Logger.getLogger(ServiceTopologyWatcher.class);

	private final KVServer server;
	private final ZKWrapper zkSession;
	private boolean cancelled = false;

	/**
	 * Instantiates a service configuration change watcher for the specified server.
	 * 
	 * @param server The server to act upon in the event of a configuration change
	 * @param zkSession The wrapper for the ZooKeeper client session associated with
	 *            the specified server
	 */
	public ServiceTopologyWatcher(KVServer server, ZKWrapper zkSession) {
		this.server = server;
		this.zkSession = zkSession;
	}

	/**
	 * Cancels this callback, such that its {@link #process(WatchedEvent)} method
	 * becomes a NOP.
	 */
	public void cancel() {
		this.cancelled = true;
	}

	@Override
	public void process(WatchedEvent event) {
		if (cancelled) return;

		// retrieve updated information and set another watch
		ServiceTopologyWatcher nextWatcher = new ServiceTopologyWatcher(server, zkSession);
		Set<IECSNode> updatedMetadata;
		NavigableMap<String, IECSNode> existingMetadata;
		try {
			updatedMetadata = new HashSet<>(zkSession.getMetadataNodeData(nextWatcher));
			existingMetadata = server.getCachedMetadata();
		} catch (KeeperException | InterruptedException e) {
			log.error("Could not retrieve updated metadata", e);
			return;
		}

		// find which nodes have been removed
		// (i.e. are present in existing metadata but not in updated metadata)
		Set<IECSNode> removedNodes = new HashSet<>(existingMetadata.values().stream()
				.filter(node -> !updatedMetadata.contains(node))
				.collect(Collectors.toSet()));

		// check whether self is included among removed nodes
		if (removedNodes.contains(server.getCurrentConfig())) {
			String successorName = server.findSuccessor().getNodeName();
			try {
				server.moveData(server.getCurrentConfig().getNodeHashRange(), successorName);
			} catch (Exception e) {
				log.error("Could not move data from pending server deletion", e);
				return;
			}
			server.close();
			return;
		}

		// check whether direct predecessors are included among removed nodes
		// (i.e. should expect a transfer from those nodes)
		List<IECSNode> predecessors = new ArrayList<>();
		IECSNode currentPredecessor = server.findPredecessor();
		while (removedNodes.contains(currentPredecessor)) {
			predecessors.add(currentPredecessor);
			currentPredecessor = server.findPredecessor(currentPredecessor.getNodeHashRangeStart());
		}
		if (!predecessors.isEmpty()) {
			// TODO accept transfers from predecessors
			return;
		}

		server.setCachedMetadata(updatedMetadata);
		
		// find which nodes have been added
		// (i.e. are present in existing metadata but not in updated metadata)
		Set<IECSNode> addedNodes = new HashSet<>(updatedMetadata.stream()
				.filter(node -> !existingMetadata.containsKey(node.getNodeHashRangeStart()))
				.collect(Collectors.toSet()));

		// check whether direct predecessors are included among new nodes
		// (i.e. should transfer some portion of keys to those nodes)
		predecessors = new ArrayList<>();
		currentPredecessor = server.findPredecessor();
		while (addedNodes.contains(currentPredecessor)) {
			predecessors.add(currentPredecessor);
			currentPredecessor = server.findPredecessor(currentPredecessor.getNodeHashRangeStart());
		}
		
		if (!predecessors.isEmpty()) {
			// TODO set up (partial) transfers to predecessors
			return;
		}

	}

}
