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

		// Newly added nodes (i.e. nodes that are not present in existing metadata but are present in updated metadata)
		Set<IECSNode> addedNodes = new HashSet<>(updatedMetadata.stream()
				.filter(node -> !existingMetadata.containsKey(node.getNodeHashRangeStart()))
				.collect(Collectors.toSet()));

		// Removed nodes (i.e. nodes that are present in existing metadata but not in updated metadata)
		Set<IECSNode> removedNodes = new HashSet<>(existingMetadata.values().stream()
				.filter(node -> !updatedMetadata.contains(node))
				.collect(Collectors.toSet()));


		// Set the server's copy of the metadata to the updated one
		server.setCachedMetadata(updatedMetadata);

		IECSNode self = server.getCurrentConfig();

		// All servers have been removed, nowhere to move data to. Thus, shutdown
		if (updatedMetadata.isEmpty()) {
			// Notify ECS that server is ready for shutdown
			zkSession.updateNode(self.getBaseNodePath(), ZKWrapper.READY_FOR_SHUTDOWN);
		}

		// Check whether self is included among removed nodes
		if (removedNodes.contains(server.getCurrentConfig())) {
			try {
                // Migrate data to successor server
                server.lockWrite();
				IECSNode successor = server.findSuccessor();
				server.moveData(self.getNodeHashRange(), successor.getNodeName());

				// Notify ECS with about transfer completion
                zkSession.updateNode(self.getBaseNodePath(), "FINISHED");

			} catch (Exception e) {
				log.error("Could not move data from pending server deletion", e);
			}

			// Close server (i.e. stop listening for further connections)
			server.close();
			return;
		}

		// Check whether direct predecessors are included among new nodes (i.e. should transfer some portion of keys to those nodes)
		List<IECSNode> predecessors = new ArrayList<>();
		IECSNode currentPredecessor = server.findPredecessor();
		while (addedNodes.contains(currentPredecessor)) {
			predecessors.add(currentPredecessor);
			currentPredecessor = server.findPredecessor(currentPredecessor.getNodeHashRangeStart());
		}

		if (!predecessors.isEmpty()) {
            // Send (partial) transfers to the newly added direct predecessors
            server.lockWrite();
            for (IECSNode predecessor : predecessors) {
                // TODO: Do in individual threads
                try {
                    server.moveData(predecessor.getNodeHashRange(), predecessor.getNodeName());
                } catch (Exception e) {
                    log.error("Failed to transfer data to " + predecessor, e);
                }
            }
            server.unlockWrite();
        }

        // TODO: Remove this
        zkSession.updateNode(self.getBaseNodePath(), "FINISHED");

	}
}
