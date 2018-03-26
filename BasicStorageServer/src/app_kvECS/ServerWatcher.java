package app_kvECS;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.Arrays;
import java.lang.Thread;
import java.util.concurrent.ConcurrentHashMap;
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
    private Map<String, IECSNode> runningNodes;
    private ECSClient ecsClient;
    private boolean stop;
    private boolean running;
    private Set<IECSNode> currentFailedNodes;
    public ServerWatcher(ZKSession zkSession, ECSClient ecsClient) {
        this.zkSession = zkSession;
        this.ecsClient = ecsClient;
        this.currentFailedNodes = new HashSet<>();
    }

    public void run() {

        this.running = true;

        try {
            LogSetup.initialize("logs/serverwatcher.log", Level.INFO, CONSOLE_PATTERN);
        } catch (IOException e) {
            System.out.println("ERROR: unable to initialize logger");
            e.printStackTrace();
            System.exit(1);
        }

        String lastData = null;
        String data = null;
        this.stop = false;

        while(this.stop != true)
        {
            System.out.println("Stop value in loop: " + stop);
            List<IECSNode> failedNodes = new ArrayList<>();
            runningNodes = new ConcurrentHashMap(ecsClient.getNodes());
            for(Map.Entry<String, IECSNode> entry : runningNodes.entrySet()){
                IECSNode node = entry.getValue();
                try {
                    lastData = zkSession.getNodeData(ZKPathUtil.getHeartbeatZnode(node));
                    Thread.sleep(2000);
                    data = zkSession.getNodeData(ZKPathUtil.getHeartbeatZnode(node));
                } catch (KeeperException | InterruptedException e) {
                    log.warn("Exception while reading server-ECS node", e);
                }
                if(lastData.equals(data)){
                    log.info("Server: " + node.getNodeName() + " down");
                    failedNodes.add(node);
                }
            }
            if(failedNodes.size() > 0) {
                handleServerFailures(failedNodes);
            }
        }
        System.out.println("Thread Stopped...");
        this.running = false;
        return;
    }

    public void handleServerFailures(Collection<IECSNode> nodes){
        // TODO: add failure detection
        for(IECSNode node:nodes){
            this.currentFailedNodes.add(node);
            ecsClient.removeNodes(Arrays.asList(node.getNodeName()));
            if(ecsClient.addNode(node.getCacheStrategy(), node.getCacheSize()) != null){
                this.currentFailedNodes.remove(node);
            }
        }

        return;
    }

    public void stop(){
        this.stop = true;
        System.out.println("Stop function called, stop:" + this.stop);
    }

    public boolean getStat(){
        return this.running;
    }

    public Set<IECSNode> getFailedNodes(){
        return this.currentFailedNodes;
    }


}