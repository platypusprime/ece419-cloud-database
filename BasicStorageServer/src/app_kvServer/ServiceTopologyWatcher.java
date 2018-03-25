package app_kvServer;

import static common.zookeeper.ZKSession.FINISHED;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import app_kvServer.migration.MigrationReceiveTask;
import common.KVServiceTopology;
import common.zookeeper.ZKPathUtil;
import common.zookeeper.ZKSession;
import ecs.IECSNode;

/**
 * This class provides a callback for KV store service topology changes, as
 * broadcast through the ECS central metadata znode.
 */
public class ServiceTopologyWatcher implements Watcher {

	private static final Logger log = Logger.getLogger(ServiceTopologyWatcher.class);

	private final KVServer server;
	private final ZKSession zkSession;

	private AtomicBoolean isCancelled = new AtomicBoolean(false);

	/**
	 * Instantiates a service configuration change watcher for the specified server.
	 * 
	 * @param server The server to act upon in the event of a configuration change
	 * @param zkSession The wrapper for the ZooKeeper client session associated with
	 *            the specified server
	 */
	public ServiceTopologyWatcher(KVServer server, ZKSession zkSession) {
		this.server = server;
		this.zkSession = zkSession;
	}

	/**
	 * Cancels the callback of this watcher, such that its
	 * {@link #process(WatchedEvent)} method becomes a NOP. This breaks the chain of
	 * watches that this class perpetuates.
	 */
	public void cancel() {
		this.isCancelled.set(true);
		;
	}

	@Override
	public void process(WatchedEvent event) {
		if (isCancelled.get()) return;

		String serverName = server.getServerConfig().getNodeName();
		log.debug("Service topology watcher for server " + serverName + " triggered");

		IECSNode config = server.getServerConfig();
		KVServiceTopology oldTopology = server.getServiceConfig();
		Set<IECSNode> oldMetadata = oldTopology.getNodeSet();
		log.trace("Old topology for server " + serverName + ": " + oldMetadata);
		Set<IECSNode> newMetadata = retrieveAndWatchMetadata(); // next watcher set here
		log.trace("New topology for server " + serverName + ": " + newMetadata);

		// All servers have been removed, nowhere to move data to. Thus, shutdown
		if (newMetadata == null || newMetadata.isEmpty()) {
			cancel();
			handleAllNodesRemoved();
			return;
		}

		server.setServiceConfig(newMetadata);
		KVServiceTopology newTopology = server.getServiceConfig();

		TopologyChange change = new TopologyChange(oldMetadata, newMetadata);

		if (change.delta == 0) {
			log.warn("No topology change detected");

		} else if (change.delta < 0) {
			if (change.diffContains(config)) {
				handleSelfRemoved();
				return;
			}

			Collection<IECSNode> transferrers = change.getChangedPredecessors(newTopology, config);
			if (!transferrers.isEmpty()) {
				handlePredecessorsRemoved(transferrers);
			}

		} else {
			Collection<IECSNode> receivers = change.getChangedPredecessors(oldTopology, config);
			if (!receivers.isEmpty()) {
				handlePredecessorsAdded(receivers);
			}
		}
	}

	/**
	 * Retrieves updated information from the central metadata node and sets another
	 * watch to handle the next update.
	 * 
	 * @return The updated service metadata,
	 *         or <code>null</code> if it could not be retrieved
	 */
	private Set<IECSNode> retrieveAndWatchMetadata() {
		try {
			return new HashSet<>(zkSession.getMetadataNodeData(this));
		} catch (KeeperException | InterruptedException e) {
			// TODO handle case where the next watcher has not been placed properly
			log.error("Could not retrieve updated metadata", e);
			return null;
		} catch (NullPointerException e) {
			// logging handled by caller
			return null;
		}
	}

	/**
	 * Handles the case when no servers remain in the service topology. In this
	 * case, the associated server may simply shut down without any transfer of
	 * data.
	 */
	private void handleAllNodesRemoved() {
		IECSNode config = server.getServerConfig();
		log.info("No servers remaining in service configuration;"
				+ " shutting down " + config.getNodeName() + " without any transfer");
		// Notify ECS that server is ready for shutdown
		try {
			// TODO server needs to handle this
			zkSession.updateNode(ZKPathUtil.getStatusZnode(config), FINISHED);
		} catch (KeeperException | InterruptedException e) {
			log.warn("Could not signal shutdown to ECS", e);
		}

		server.close();
	}

	/**
	 * Handles the case when this watcher's associated server is removed from the
	 * service topology. In this case, the associated server must transfer all of
	 * its keys to its immediate successor.
	 */
	private void handleSelfRemoved() {
		this.cancel();

		IECSNode config = server.getServerConfig();
		KVServiceTopology topology = server.getServiceConfig();
		log.info(config.getNodeName() + " no longer in service topology;"
				+ " transferring all key-value pairs to immediate successor");

		// Migrate data to successor server
		server.lockWrite();
		IECSNode successor = topology.findSuccessor(config.getNodeHashRangeStart());
		server.moveData(config.getNodeHashRange(), successor.getNodeName());

		try {
			// Notify ECS of transfer completion
			zkSession.updateNode(ZKPathUtil.getStatusZnode(config), FINISHED);
		} catch (KeeperException | InterruptedException e) {
			log.warn("Could not signal transfer completion to ECS", e);
		}

		server.close();
	}

	/**
	 * Handles the case when server(s) that were previously located directly
	 * preceding this watcher's associated server are removed from the service
	 * topology. In this case the associated server must accept key-value pairs from
	 * each removed server.
	 * 
	 * @param transferrers The predecessors from which K/V pairs are expected
	 * @see MigrationReceiveTask
	 */
	private void handlePredecessorsRemoved(Collection<IECSNode> transferrers) {
		IECSNode config = server.getServerConfig();
		log.info(transferrers.size() + " nodes removed directly pre1ceding " + config.getNodeName() + ";"
				+ " accepting key-value pairs from former predecessors");

		server.lockWrite();

		List<Thread> transferThreads = new ArrayList<>();
		for (IECSNode transferrer : transferrers) {
			String transferNode = ZKPathUtil.getMigrationZnode(config, transferrer);
			MigrationReceiveTask receiveTask = new MigrationReceiveTask(transferNode, zkSession, server);
			Thread transferThread = new Thread(receiveTask);
			transferThread.start();
			transferThreads.add(transferThread);
		}

		// block until all transfers have completed
		for (Thread transferThread : transferThreads) {
			try {
				transferThread.join();
			} catch (InterruptedException e) {
				log.warn("Interrupted while waiting for transfer", e);
			}
		}

		server.unlockWrite();
	}

	/**
	 * Handles the case when new server(s) are added directly preceding this
	 * watcher's associated server in the service topology. In this case, the
	 * associated server must transfer a portion of its key-value pairs to each new
	 * server.
	 * 
	 * @param receivers The predecessors to transfer K/V pairs to
	 * @see KVServer#moveData(String[], String)
	 */
	private void handlePredecessorsAdded(Collection<IECSNode> receivers) {
		IECSNode config = server.getServerConfig();
		log.info(receivers.size() + " nodes added directly preceding " + config.getNodeName() + ";"
				+ " transferring key-value pairs to new predecessors");

		server.lockWrite();
		for (IECSNode receiver : receivers) {
			// String transferNode = receiver.getMigrationNodePath(config.getNodeName());
			server.moveData(receiver.getNodeHashRange(), receiver.getNodeName());
		}
		server.unlockWrite();

		try {
			// Notify ECS of transfer completion
			zkSession.updateNode(ZKPathUtil.getStatusZnode(config), FINISHED);
		} catch (KeeperException | InterruptedException e) {
			log.warn("Could not signal transfer completion to ECS", e);
		}
	}

}
