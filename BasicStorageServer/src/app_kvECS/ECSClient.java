package app_kvECS;

import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Comparator;
import ecs.ECSNode;
import ecs.IECSNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import logger.LogSetup;
import java.math.BigInteger;
import java.io.IOException;
import common.HashUtil;
import app_kvECS.ZKManager;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException;
public class ECSClient implements IECSClient {

    private List<ECSNode> serverPool;
//    private ArrayList<ECSNode> activeServerPool;
    private int serverPoolCount;
    private int activeServerPoolCount;
    private static ZKManager zkManager;

    public ECSClient(String fileName){
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
                System.out.println(serverPool.get(i).getNodeName());
                System.out.println(serverPool.get(i).getNodeHost());
                System.out.println(serverPool.get(i).getNodePort());
                i = i + 1;
            }

            this.serverPoolCount = i;
            Collections.sort(serverPool, new Comparator<ECSNode>(){
                public int compare(ECSNode node1, ECSNode node2){
//                    BigInteger bi1 = new BigInteger(node1.getStart(), 16);
//                    BigInteger bi2 = new BigInteger(node2.getStart(), 16);
                    return node1.getStart().compareTo(node2.getStart());
                }
            });
            for (int j = 0; j < i; j++) {
//                System.out.println(item.getNodeName());
//                System.out.println(item.getNodeHost());
//                System.out.println(item.getNodePort());
                if(j == (i-1)){
                    serverPool.get(j).setEnd(serverPool.get(0).getStart());
                }
                else{
                    serverPool.get(j).setEnd(serverPool.get(j+1).getStart());
                }
                System.out.println(serverPool.get(j).getStart() + " to " + serverPool.get(j).getEnd());
            }
            zkManager = new ZKManager();
        }
        catch (IOException e) {
            System.out.println("Error! Unable to open file!");
        }

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
            ex.printStackTrace();
        }

    }

    @Override
    public boolean start() {
        // TODO
        return false;
    }

    @Override
    public boolean stop() {
        // TODO
        return false;
    }

    @Override
    public boolean shutdown() {
        // TODO
        return false;
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
                    }
                }
                for(int j = 1; j < serverPoolCount; j++){
                    int modPosition = (i*2-j) % serverPoolCount;
                    if(serverPool.get(modPosition).busy == true) {
                        serverPool.get(modPosition).setEnd(serverPool.get(i).getStart());
                        String path = serverPool.get(modPosition).getNodeName();
                        data = ("STATE:MOVEDATA" + " HSTART:" + serverPool.get(i).getStart() + " IP:"
                                + serverPool.get(i).getNodeHost()).getBytes();
                        try {
                            zkManager.update(path, data);
                            String response = zkManager.getZNodeData(path, false);
                            if (response.equals("SUCCESS")) {
                                data = ("STATE:UPDATE" + " HSTART:" + serverPool.get((i * 2 - j) % serverPoolCount).getStart()
                                        + " HEND:" + serverPool.get((i * 2 - j) % serverPoolCount).getEnd()).getBytes();
                                zkManager.update(path, data);
                            }
                        }
                        catch(KeeperException | InterruptedException e){
                            System.out.println(e.getMessage());
                        }
                    }
                }

                String path = "/" + serverPool.get(i).getNodeName();
                data = ("STATE:UPDATE" + " HSTART:" + serverPool.get(i).getStart() + " HEND:"
                        + serverPool.get(i).getEnd()).getBytes();
                try {
                    zkManager.create(path, data);
                    Stat stat = zkManager.getZNodeStats(path);
                    System.out.println("ZNode Stat: " + stat.toString());
                }
                catch(KeeperException | InterruptedException e){
                    System.out.println(e.getMessage());
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
                }
            }
        }

        for(int i = 0; i < serverPoolCount; i++){
            if(serverPool.get(i).busy == true) {
                for(int j = 1; j < serverPoolCount; j++){
                    if(serverPool.get((i+j) % serverPoolCount).busy == true) {
                        serverPool.get(i).setEnd(serverPool.get((i+j) % serverPoolCount).getStart());
                    }
                }
            }
        }
        setupNodes(count, cacheStrategy, cacheSize);
        return activePool;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // ZNode Path
        for (int i = 0; i < serverPoolCount; i++) {
            if (serverPool.get(i).busy == true) {
                String path = "/" + serverPool.get(i).getNodeName();
                byte[] data = ("STATE:INIT CACHE:" + cacheStrategy + " CACHESIZE:" + cacheSize + " HSTART:" +
                        serverPool.get(i).getStart() + " HEND:" + serverPool.get(i).getEnd()).getBytes();
                try {
                    zkManager.create(path, data);
                    Stat stat = zkManager.getZNodeStats(path);
                    System.out.println("ZNode Stat: " + stat.toString());
                }
                catch(KeeperException | InterruptedException e){
                    System.out.println(e.getMessage());
                }
                initializeKVServer(serverPool.get(i));
                return null;
            }
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
                    for(int j = 1; j < serverPoolCount; j++)
                    {
                        int modPosition = (i*2-j) % serverPoolCount;
                        if(serverPool.get(modPosition).busy == true)
                        {
                            for(int k = 1; k < serverPoolCount; k++) {
                                if (serverPool.get((i + k) % serverPoolCount).busy == true) {
                                    serverPool.get(modPosition).setEnd(serverPool.get((i + k) % serverPoolCount).getStart());
                                    String path = serverPool.get(modPosition).getNodeName();
                                    byte[] data = ("STATE:UPDATE" + " HSTART:" + serverPool.get((i * 2 - j) % serverPoolCount).getStart()
                                            + " HEND:" + serverPool.get((i * 2 - j) % serverPoolCount).getEnd()).getBytes();
                                    try {
                                        zkManager.update(path, data);
                                    } catch (KeeperException | InterruptedException e) {
                                        System.out.println(e.getMessage());
                                    }
                                }
                            }
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
            ECSClient clientApplication = new ECSClient(args[0]);
            clientApplication.addNode("FIFO", 200);
        }
    }
}
