package app_kvServer;

import static common.zookeeper.ZKSession.RUNNING_STATUS;
import static common.zookeeper.ZKSession.STOPPED_STATUS;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

import common.zookeeper.ZKPathUtil;
import common.zookeeper.ZKSession;

/**
 * This class provides a watcher for synchronizing an individual server's status
 * with the overall service status. This watcher is also responsible for
 * signally server shutdown, by detecting deletion of the status znode.
 */
public class ServiceStatusWatcher implements Watcher {

	private static final Logger log = Logger.getLogger(ServiceStatusWatcher.class);

	private final IKVServer server;
	private final ZKSession zkSession;

	/**
	 * Creates a watcher for the service status node which provides updates to the
	 * given server.
	 * 
	 * @param server The server to update status for
	 * @param zkSession The ZooKeeper session to use
	 */
	public ServiceStatusWatcher(IKVServer server, ZKSession zkSession) {
		this.server = server;
		this.zkSession = zkSession;
	}

	@Override
	public void process(WatchedEvent event) {

		// Shut down server if its node has been deleted
		if (event.getType() == EventType.NodeDeleted) {
			log.info("Service status znode deleted; shutting down server " + server.getName());
			server.close();

		} else {
			String kvStatus = null;
			try {
				kvStatus = zkSession.getNodeData(ZKPathUtil.KV_SERVICE_STATUS_NODE, this);
			} catch (KeeperException | InterruptedException e) {
				log.error("Exception while checking service status", e);
			}

			if (kvStatus == null) {
				// null check
			} else if (kvStatus.equals(RUNNING_STATUS)) {
				server.start();
			} else if (kvStatus.equals(STOPPED_STATUS)) {
				server.stop();
			}
		}
	}

}
