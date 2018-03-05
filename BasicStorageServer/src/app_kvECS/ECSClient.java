package app_kvECS;

import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import ecs.ECSNode;
import ecs.IECSNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import logger.LogSetup;
import java.io.IOException;
import common.HashUtil;
import app_kvECS.ZKManager;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import common.messages.KVMessage.StatusType;
public class ECSClient implements IECSClient {

    private static final Logger log = Logger.getLogger(ECSClient.class);
    private static final String PROMPT = "kvECS> ";
    private static final String CONSOLE_PATTERN = PROMPT + "%m%n";
    private static String path = "/serverMetaData";
    private List<ECSNode> serverPool;
    private int serverPoolCount;
    private int activeServerPoolCount;
    private static ZKManager zkManager;
    private static MetadataUpdateMessage metaDataTable;
    public ECSClient(String fileName){
        log.info("initialization started...");
        File file = new File(fileName);
        try( FileReader fileReader = new FileReader(file);
             BufferedReader bufferedReader = new BufferedReader(fileReader);)
        {
            String line;
            serverPool = new ArrayList<ECSNode>();
            activeServerPoolCount = 0;
            int i = 0;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
                System.out.println(Integer.toString(i));
                String[] parts = line.trim().split(" ");
                String hostName = parts[0];
                String ip = parts[1];
                int port = Integer.parseInt(parts[2]);
                String start = HashUtil.toMD5(ip.concat(":").concat(Integer.toString(port)));
                ECSNode node = new ECSNode(ip, port, start, HashUtil.MIN_MD5);
                serverPool.add(node);
                i = i + 1;
            }

            this.serverPoolCount = i;
            Collections.sort(serverPool, new Comparator<ECSNode>(){
                public int compare(ECSNode node1, ECSNode node2){
                    return node1.getStart().compareTo(node2.getStart());
                }
            });
            for (int j = 0; j < i-1; j++) {
                serverPool.get(j).setEnd(serverPool.get(j+1).getStart());
                System.out.println(serverPool.get(j).getStart() + " to " + serverPool.get(j).getEnd());
            }
            serverPool.get(serverPoolCount-1).setEnd(serverPool.get(0).getStart());
            System.out.println(serverPool.get(serverPoolCount-1).getStart() + " to " + serverPool.get(serverPoolCount-1).getEnd());
            zkManager = new ZKManager();
            metaDataTable = new MetadataUpdateMessage();
        }
        catch (IOException e) {
            log.fatal("Error! Unable to open file!",e);
        }
        log.info("initialization done...");

    }


    public void initializeKVServer(ECSNode node){
        try {
            String command = "ssh -n " + node.getNodeHost() + " nohup java -jar ms2-server.jar " + node.getNodePort() +" ERROR &";
            // print a message
            System.out.println("Executing ssh command...");

            Process process = Runtime.getRuntime().exec(command);

            // print another message
            System.out.println("Command sent to KVServer...");

        } catch (Exception ex) {
            log.fatal("Unable to execute command",ex);
        }

    }

    @Override
    public boolean start() {
        log.info("STARTING ALL SERVERS...");
        for (int i = 0; i < serverPoolCount; i++) {
            if (serverPool.get(i).busy == true) {
                metaDataTable.modifyNode(serverPool.get(i), StatusType.START);
                byte[] data = metaDataTable.toMessageString().getBytes();
                try {
                    zkManager.update(path, data);
                    String getData = zkManager.getZNodeData(path, false);
                }
                catch(KeeperException | InterruptedException e){
                    log.warn(e.getMessage());
                }
            }
        }
        return true;
    }

    @Override
    public boolean stop() {
        log.info("STOPPING ALL SERVERS...");
        for (int i = 0; i < serverPoolCount; i++) {
            if (serverPool.get(i).busy == true) {
                metaDataTable.modifyNode(serverPool.get(i), StatusType.STOP);
                byte[] data = metaDataTable.toMessageString().getBytes();
                try {
                    zkManager.update(path, data);
                }
                catch(KeeperException | InterruptedException e){
                    log.warn(e.getMessage());
                }
            }
        }
        return true;
    }

    @Override
    public boolean shutdown() {
        log.info("SHUTTING DOWN ALL SERVERS...");
        for (int i = 0; i < serverPoolCount; i++) {
            if (serverPool.get(i).busy == true) {
                metaDataTable.modifyNode(serverPool.get(i), StatusType.SHUTDOWN);
                byte[] data = metaDataTable.toMessageString().getBytes();
                try {
                    zkManager.update(path, data);
                    String getData = zkManager.getZNodeData(path, false);
                }
                catch(KeeperException | InterruptedException e){
                    log.warn(e.getMessage());
                }
            }
        }
        return true;
    }

    @Override
    public IECSNode addNode(String cacheStrategy, int cacheSize) {
        if(activeServerPoolCount >= serverPoolCount){
            return null;
        }
        byte[] data;
        for(int i = 0; i < serverPoolCount; i++){
            if(serverPool.get(i).busy == false){
                serverPool.get(i).busy = true;
                activeServerPoolCount += 1;

                for(int j = 1; j < serverPoolCount; j++){
                    if(serverPool.get((i+j) % serverPoolCount).busy == true) {
                        serverPool.get(i).setEnd(serverPool.get((i+j) % serverPoolCount).getStart());
                        metaDataTable.addNode(serverPool.get(i), StatusType.START, cacheStrategy, Integer.toString(cacheSize));
                        break;
                    }
                }
                for(int j = 1; j < serverPoolCount; j++){
                    int modPosition = (i + serverPoolCount - j) % serverPoolCount;
                    if(serverPool.get(modPosition).busy == true) {
                        serverPool.get(modPosition).setEnd(serverPool.get(i).getStart());
                        metaDataTable.modifyNode(serverPool.get(modPosition), StatusType.START);
                        data = metaDataTable.toMessageString().getBytes();
                        try {
                            zkManager.update(path, data);
                        }
                        catch(KeeperException | InterruptedException e){
                            System.out.println(e.getMessage());
                        }
                        break;
                    }
                }

                initializeKVServer(serverPool.get(i));
                return serverPool.get(i);
            }
        }
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        Collection<IECSNode> activePool = new ArrayList<IECSNode>();
        if((activeServerPoolCount + count) > serverPoolCount){
            return null;
        }

        // changing state of the serverPool nodes
        for(int i = 0; i < count; i++){
            for(int j = 0; j < serverPoolCount; j++){
                if(serverPool.get(j).busy == false) {
                    serverPool.get(j).busy = true;
                    activeServerPoolCount += 1;
                    activePool.add(serverPool.get(j));
                    break;
                }
            }
        }

        for(int i = 0; i < serverPoolCount; i++){
            if(serverPool.get(i).busy == true) {
                for(int j = 1; j < serverPoolCount; j++){
                    if(serverPool.get((i+j) % serverPoolCount).busy == true) {
                        serverPool.get(i).setEnd(serverPool.get((i+j) % serverPoolCount).getStart());
                        break;
                    }
                }
            }
        }
        for(int i = 0; i < serverPoolCount; i++){
            if(serverPool.get(i).busy == true) {
                System.out.println("active server " + Integer.toString(i) + ": "+ serverPool.get(i).getStart() + " to " + serverPool.get(i).getEnd());
            }
        }
        setupNodes(count, cacheStrategy, cacheSize);
        return activePool;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {

        for (int i = 0; i < serverPoolCount; i++) {
            if (serverPool.get(i).busy == true) {
                metaDataTable.addNode(serverPool.get(i), StatusType.INIT, cacheStrategy, Integer.toString(cacheSize));
            }
        }

        byte[] data = metaDataTable.toMessageString().getBytes();

        try {
            zkManager.create(path, data);
            Stat stat = zkManager.getZNodeStats(path);
        }
        catch(KeeperException | InterruptedException e){
            System.out.println(e.getMessage());
        }
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        for(String node:nodeNames){
            for (int i = 0; i < serverPoolCount; i++) {
                if(serverPool.get(i).getNodeName().equals(node))
                {
                    serverPool.get(i).busy = false;
                    activeServerPoolCount -= 1;
                    serverPool.get(i).setEnd(serverPool.get((i+1) % serverPoolCount).getStart());
                    metaDataTable.removeNode(node);

                    for(int j = 1; j < serverPoolCount; j++)
                    {
                        int modPosition = (i + serverPoolCount - j) % serverPoolCount;
                        if(serverPool.get(modPosition).busy == true)
                        {
                            for(int k = 1; k < serverPoolCount; k++) {
                                if (serverPool.get((i + k) % serverPoolCount).busy == true) {
                                    serverPool.get(modPosition).setEnd(serverPool.get((i + k) % serverPoolCount).getStart());
                                    metaDataTable.modifyNode(serverPool.get(modPosition), StatusType.START);
                                    byte[] data = metaDataTable.toMessageString().getBytes();
                                    try {
                                        zkManager.update(path, data);
                                    } catch (KeeperException | InterruptedException e) {
                                        System.out.println(e.getMessage());
                                    }
                                    break;
                                }
                            }
                            log.info("server: " + serverPool.get(i).getNodeName() + " got removed, remaining servers: " + Integer.toString(activeServerPoolCount));
                            break;
                        }
                    }
                }
            }
        }
        return true;
    }

    @Override
    public Map<String, IECSNode> getNodes() {
        // TODO
        return null;
    }

    @Override
    public IECSNode getNodeByKey(String Key) {
        // TODO
        return null;
    }

    public static void main(String[] args) {
        if(args.length != 1) {
            System.out.println("Error! Invalid number of arguments!");
            System.out.println("Usage: Config File <name>");
        } else {
            try {
                LogSetup.initialize("logs/ecs.log", Level.INFO, CONSOLE_PATTERN);
            } catch (IOException e) {
                System.out.println("ERROR: unable to initialize logger");
                e.printStackTrace();
                System.exit(1);
            }
            ECSClient clientApplication = new ECSClient(args[0]);
            clientApplication.addNodes(5, "FIFO",200);
            clientApplication.addNode("FIFO", 200);
            clientApplication.start();
            clientApplication.removeNodes(new ArrayList<String>(
                    Arrays.asList("Server 127.0.0.1:50005","Server 127.0.0.1:50002")));
            clientApplication.stop();

        }
    }
}
