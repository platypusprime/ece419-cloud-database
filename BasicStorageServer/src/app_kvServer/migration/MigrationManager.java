package app_kvServer.migration;

import app_kvServer.KVServer;
import app_kvServer.persistence.KVPersistence;
import common.HashUtil;
import common.zookeeper.ZKWrapper;
import ecs.IECSNode;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MigrationManager {

    private static final Logger log = Logger.getLogger(MigrationManager.class);

    private static Map<String, MigrationReceiveTask> receiveTasks = new HashMap<>();

    public static synchronized void receiveData(List<String> srcServerNames, ZKWrapper zkWrapper, KVServer server) {
        IECSNode config = server.getCurrentConfig();

        for (String serverName: srcServerNames) {
            if (!receiveTasks.containsKey(serverName)) {
                String zNodePath = config.getMigrationNodePath(serverName);
                MigrationReceiveTask task = new MigrationReceiveTask(zNodePath, zkWrapper, server);
                receiveTasks.put(serverName, task);
            }

            log.info("Receiving data from " + serverName);

            // Receive migrated data by reading on the znode
            receiveTasks.get(serverName).receiveData();
        }
    }

    public static synchronized boolean sendData(String[] hashRange, String targetName, ZKWrapper zkWrapper, KVServer server) {
        log.info("Sending data to " + targetName);

        Map<String, String> kvPairs = server.getAllKV();

        // Filter out keys that are not within the given hash range
        kvPairs.keySet().removeIf(key -> !HashUtil.containsHash(HashUtil.toMD5(key), hashRange));

        // Prepare message
        MigrationMessage message = new MigrationMessage(null, null, null, 0, 0, kvPairs);
        String data = message.toJSON();

        String targetNode = zkWrapper.getZNodePathForMigration(server.getCurrentConfig().getNodeName(), targetName);

        try {
            // Send message via znode
            zkWrapper.createNode(targetNode);
            zkWrapper.updateNode(targetNode, data);

            // Busy-wait for response
            String response = zkWrapper.getNodeData(targetNode);
            while (response != null && !response.isEmpty()) {
                response = zkWrapper.getNodeData(targetNode);
            }
            zkWrapper.deleteNode(targetNode);

            // Remove sent data from own persistence
            kvPairs.forEach((k, v) -> server.putKV(k, null));
            log.info("Data transfer completed. Deleted from self.");

        } catch (KeeperException | InterruptedException e) {
            log.error("Error while sending data to " + targetNode, e);
        }

        return true;
    }
}
