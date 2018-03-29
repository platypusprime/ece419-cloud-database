package app_kvServer.replication;

import app_kvServer.KVServer;
import app_kvServer.migration.ReplicationReceiveTask;
import common.zookeeper.ZKSession;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplicationManager {

    private static final Logger log = Logger.getLogger(ReplicationManager.class);

    private static Map<String, ReplicationReceiveTask> receiveTasks = new HashMap<>();

    public static synchronized void receiveData(List<String> srcServerNames, ZKSession zkSession, KVServer server) {
//        IECSNode config = server.getServerConfig();

        for (String serverName : srcServerNames) {
            if (!receiveTasks.containsKey(serverName)) {
                String zNodePath = zkSession.getReplicationNodePath(serverName, server.getName());
                ReplicationReceiveTask task = new ReplicationReceiveTask(zNodePath, zkSession, server);
                receiveTasks.put(serverName, task);
            }

            log.info("Receiving data from " + serverName);

            // Receive replicated data by reading on the znode
            receiveTasks.get(serverName).receiveData();
        }
    }
}