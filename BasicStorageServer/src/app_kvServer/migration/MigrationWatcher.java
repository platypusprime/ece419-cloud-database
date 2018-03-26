package app_kvServer.migration;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import app_kvServer.KVServer;
import common.zookeeper.ZKPathUtil;
import common.zookeeper.ZKSession;

/**
 * Watches for changes to a server's migration root node and handles reception
 * of key-value pairs being copied from other servers.
 */
public class MigrationWatcher implements Watcher {

	private static final Logger log = Logger.getLogger(MigrationWatcher.class);

	private final KVServer server;
	private final ZKSession zk;
	private final String migrationRoot;

	/**
	 * Creates a migration watcher for the given server.
	 * 
	 * @param server The server to handle migrations for
	 * @param zk The ZooKeeper instance to use
	 */
	public MigrationWatcher(KVServer server, ZKSession zk) {
		this.server = server;
		this.zk = zk;
		this.migrationRoot = ZKPathUtil.getMigrationRootZnode(server.getServerConfig());
	}

	@Override
	public void process(WatchedEvent event) {
		log.debug("Change detected on migration root znode" + migrationRoot);

		List<String> childNodes = zk.getChildNodes(migrationRoot, this);
		if (childNodes == null || childNodes.isEmpty()) {
			log.debug("No pending migrations detected for server " + server.getName());
			return;
		}
		log.debug("Detected pending migrations from " + childNodes + " to " + server.getName());

		server.lockWrite();

		List<Thread> transferThreads = new ArrayList<>();
		for (String childNode : childNodes) {
			String transferNode = migrationRoot + "/" + childNode;

			// run migration task on separate thread
			MigrationReceiveTask receiveTask = new MigrationReceiveTask(transferNode, zk, server);
			Thread transferThread = new Thread(receiveTask);
			transferThread.start();
			transferThreads.add(transferThread);
		}

		// block until all transfers have completed or timed out
		for (Thread transferThread : transferThreads) {
			try {
				transferThread.join();
			} catch (InterruptedException e) {
				log.warn("Interrupted while waiting for transfer", e);
			}
		}

		server.unlockWrite();
	}

}
