package app_kvECS;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.lang.Thread;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;

import common.zookeeper.ZKPathUtil;
import common.zookeeper.ZKSession;
import common.KVServiceTopology;
import ecs.ECSNode;
import ecs.IECSNode;

import logger.LogSetup;

public class ServerWatcher implements Runnable {
    private static final Logger log = Logger.getLogger(ServerWatcher.class);
    public static final String PROMPT = "serverWatcher> ";
    public static final String CONSOLE_PATTERN = PROMPT + "%m%n";

    private ZKSession zkSession;
    private Collection<IECSNode> runningNodes;
    private KVServiceTopology topology;

    public void ServerWatcher(String zkHostname, int zkPort, Collection<IECSNode> nodes, KVServiceTopology topology) {
        this.zkSession = new ZKSession(zkHostname, zkPort);
        this.runningNodes = nodes;
        this.topology = topology;
    }
    public void run() {
        // initialize logging
        try {
            LogSetup.initialize("logs/ecs.log", Level.INFO, CONSOLE_PATTERN);
        } catch (IOException e) {
            System.out.println("ERROR: unable to initialize logger");
            e.printStackTrace();
            System.exit(1);
        }

        String lastData = null;
        String data = null;

        while(true)
        {
            List<String> failedNodes = new ArrayList<>();
            for(IECSNode node : runningNodes){
                try {
                    lastData = zkSession.getNodeData(ZKPathUtil.getHeartbeatZnode(node));
                    Thread.sleep(2000);
                    data = zkSession.getNodeData(ZKPathUtil.getHeartbeatZnode(node));
                } catch (KeeperException | InterruptedException e) {
                    log.warn("Exception while reading server-ECS node", e);
                }
                if(lastData.equals(data)){
                    failedNodes.add(node.getNodeName());
                }
            }
            if(failedNodes.size() > 0) {
                handleServerFailures(failedNodes);
            }
        }
    }

    public void handleServerFailures(Collection<String> nodeNames){
        // update the topology object
        Set<IECSNode> removedNodes = topology.removeNodesByName(nodeNames);
        Set<IECSNode> successorNodes = topology.findSuccessors(removedNodes);

        try {
            zkSession.updateMetadataNode(topology);
        } catch (KeeperException | InterruptedException e) {
            log.error("Could not update metadata znode data", e);
        }

        // signal shutdown to removed nodes
        for (IECSNode removedNode : removedNodes) {
            try {
                zkSession.deleteNode(ZKPathUtil.getStatusZnode(removedNode));
                zkSession.deleteNode(ZKPathUtil.getMigrationRootZnode(removedNode));
                zkSession.deleteNode(ZKPathUtil.getReplicationRootZnode(removedNode));
                zkSession.deleteNode(ZKPathUtil.getHeartbeatZnode(removedNode));
            } catch (KeeperException | InterruptedException e) {
                log.error("Could not delete znode for node " + removedNode, e);
            }
        }

        // TODO: Addnode after remove

    }

}