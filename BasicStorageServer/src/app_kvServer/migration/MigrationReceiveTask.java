package app_kvServer.migration;

import static common.zookeeper.ZKSession.FINISHED;

import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import app_kvServer.KVServer;
import common.zookeeper.ChangeNotificationWatcher;
import common.zookeeper.ZKPathUtil;
import common.zookeeper.ZKSession;

/**
 * A task for receiving key-value pairs in a migration event. This
 * implementation uses a ZooKeeper znode to transfer key-value data. Because of
 * the limited capacity of znode (1,048,576 bytes), it is necessary to break
 * data into chunks.
 */
public class MigrationReceiveTask implements Runnable {

	private static final Logger log = Logger.getLogger(MigrationReceiveTask.class);

	private final String transferNode;
	private final ZKSession zkSession;
	private final KVServer kvServer;

	/**
	 * Creates a new task for the given server.
	 * 
	 * @param transferNode The znode path from which to retrieve incoming data
	 * @param zkSession The ZooKeeper session to use
	 * @param kvServer The server instance receiving data
	 */
	public MigrationReceiveTask(String transferNode, ZKSession zkSession, KVServer kvServer) {
		log.debug("Created task for receiving migration data from znode: " + transferNode);
		this.transferNode = transferNode;
		this.zkSession = zkSession;
		this.kvServer = kvServer;
	}

	@Override
	public void run() {
		boolean finished = false;

		log.info("Starting migration receive task");

		// wait for the transfer node to be created
		Object existenceMonitor = new Object();
		ChangeNotificationWatcher existenceNotifier = new ChangeNotificationWatcher(existenceMonitor);
		synchronized (existenceMonitor) {
			try {
				boolean nodeExists = zkSession.checkNodeExists(transferNode, existenceNotifier);
				while (!nodeExists) {
					existenceMonitor.wait();
					nodeExists = zkSession.checkNodeExists(transferNode, existenceNotifier);
				}
				existenceNotifier.cancel();

			} catch (KeeperException | InterruptedException e) {
				log.warn("Exception while waiting for creating of transfer node", e);
			}
		}

		// poll for transfer data
		while (!finished) {
			String monitor = transferNode;
			ChangeNotificationWatcher updateNotifier = new ChangeNotificationWatcher(monitor);
			synchronized (monitor) {
				String data = null;
				try {
					log.debug("Retrieving data from transfer node " + transferNode);
					data = zkSession.getNodeData(transferNode, updateNotifier);
					if (data == null || data.isEmpty()) {
						log.debug("Transfer node currently empty; blocking until change on " + transferNode);
						monitor.wait(); // wait until the watcher gets an event
						data = zkSession.getNodeData(transferNode);
						log.debug("Change detected; retrieved transfer data: " + data);
					} else {
						log.debug("Data present; retrieved transfer data: " + data);
						updateNotifier.cancel();
					}

					// add data
					finished = processData(data);

					// Write empty data on znode to indicate acknowledgement
					log.debug("Emptying transfer znode: " + transferNode);
					zkSession.updateNode(transferNode, new byte[0]);

				} catch (KeeperException e) {
					log.warn("Exception while reading transfer znode", e);

				} catch (InterruptedException e) {
					log.warn("Interrupted while receiving data", e);
				}
			}
		}

		// finish transfer
		try {
			log.info("Deleting transfer znode");
			zkSession.deleteNode(transferNode);

			log.info("Notifying ECS of transfer completion");
			zkSession.updateNode(ZKPathUtil.getStatusZnode(kvServer.getServerConfig()), FINISHED);

		} catch (KeeperException | InterruptedException e) {
			log.warn("Could not signal transfer completion to ECS");
		}
	}

	/**
	 * Processes incoming transfer node data.
	 * 
	 * @param data The data to process
	 * @return <code>true</code> if the transfer is complete, <code>false</code>
	 *         otherwise
	 */
	private boolean processData(String data) {
		if (data == null || data.isEmpty()) {
			return false;
		} else if (data.equals(FINISHED)) {
			return true;
		}

		MigrationMessage message = MigrationMessage.fromJSON(data);
		Map<String, String> kvPairs = message.getData();

		// Insert the K/V pairs into persistence
		log.info("Inserting " + kvPairs.size() + " into the persistence");
		kvPairs.forEach(kvServer::putKV);

		return false;
	}
}
