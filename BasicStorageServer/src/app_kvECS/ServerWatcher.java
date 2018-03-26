package app_kvECS;

import java.util.Arrays;
import java.util.Objects;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import common.zookeeper.ZKPathUtil;
import common.zookeeper.ZKSession;
import ecs.IECSNode;

/**
 * Tracks the liveness of a single server using heartbeats transmitted with
 * ZooKeeper znodes.
 */
public class ServerWatcher implements Runnable {

	private static final Logger log = Logger.getLogger(ServerWatcher.class);

	private static final int HEARTBEAT_CHECK_INTERVAL = 5000;

	private final ECSClient ecsClient;
	private final ZKSession zkSession;
	private final IECSNode server;

	/**
	 * Creates a heartbeat listener for the given server.
	 * 
	 * @param server The server to listen for heartbeats for
	 * @param ecsClient The ECS client managing the given server
	 * @param zkSession The ZooKeeper session to use to check the heartbeat znode
	 */
	public ServerWatcher(IECSNode server, ECSClient ecsClient, ZKSession zkSession) {
		this.server = server;
		this.ecsClient = ecsClient;
		this.zkSession = zkSession;
	}

	@Override
	public void run() {
		String lastData, data;
		try {
			lastData = zkSession.getNodeData(ZKPathUtil.getHeartbeatZnode(server));
		} catch (KeeperException | InterruptedException e) {
			log.error("Exception while reading heartbeat node " + ZKPathUtil.getHeartbeatZnode(server), e);
			return;
		}

		while (!Thread.interrupted()) {
			try {
				Thread.sleep(HEARTBEAT_CHECK_INTERVAL);

				data = zkSession.getNodeData(ZKPathUtil.getHeartbeatZnode(server));

				if (!Objects.equals(data, lastData)) {
					lastData = data;

				} else {
					log.info("Did not receive heartbeat from " + server.getNodeName()
							+ " in " + HEARTBEAT_CHECK_INTERVAL + " ms");
					handleFailure();
					break;
				}

			} catch (KeeperException e) {
				log.warn("Exception while listening for heartbeat", e);
			} catch (InterruptedException e) {
				log.info("Heartbeat listening thread interrupted; ending");
				break;
			}
		}
	}

	private void handleFailure() {
		// remove znodes associated with the failed node
		try {
			zkSession.deleteNode(ZKPathUtil.getStatusZnode(server));
			zkSession.deleteNode(ZKPathUtil.getMigrationRootZnode(server));
			zkSession.deleteNode(ZKPathUtil.getHeartbeatZnode(server));
		} catch (KeeperException | InterruptedException e) {
			log.error("Could not delete znodes for server: " + server.getNodeName(), e);
		}

		// Since migration znode is missing, successor will know not to try and
		// communicate with the crashed node for migration.
		ecsClient.removeNodes(Arrays.asList(server.getNodeName()));

		// Replace the crashed node with a new one. Usually, this will be the same one
		// that crashed. This will start a new heartbeat listener.
		ecsClient.addNode(server.getCacheStrategy(), server.getCacheSize());
	}

}