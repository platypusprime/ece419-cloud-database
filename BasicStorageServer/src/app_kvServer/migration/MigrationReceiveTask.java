package app_kvServer.migration;

import app_kvServer.KVServer;
import common.zookeeper.ZKSession;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

import java.util.Map;

public class MigrationReceiveTask {

    private final String znodePath;
    private final ZKSession zkSession;
    private final KVServer kvServer;

    private static final Logger log = Logger.getLogger(MigrationReceiveTask.class);

    public MigrationReceiveTask(String znodePath, ZKSession zkSession, KVServer kvServer) {
        this.znodePath = znodePath;
        this.zkSession = zkSession;
        this.kvServer = kvServer;
    }

    public synchronized void receiveData() {
        try {
            String data = zkSession.getNodeData(znodePath, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getType() == Event.EventType.NodeDataChanged) {
                        receiveData();
                    }
                    else if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                        zkSession.updateNode(kvServer.getCurrentConfig().getBaseNodePath(), "FINISHED");
                    }
                }
            });

            if (data != null && !data.isEmpty()) {
                // Write empty data on znode to indicate acknowledgement
                zkSession.updateNode(znodePath, new byte[0]);

                // Save to data to persistence
                processAndSaveData(data);
            }

        } catch (KeeperException | InterruptedException e) {
            log.error("Error while receiving migration data from " + znodePath, e);
        }
    }

    private void processAndSaveData(String data) {
        if (data == null || data.isEmpty()) {
            return;
        }

        MigrationMessage message = MigrationMessage.fromJSON(data);
        Map<String, String> kvPairs = message.getData();

        // Insert the kv pairs into persistence
        kvPairs.forEach(kvServer::putKV);
    }
}
