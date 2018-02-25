package app_kvECS;

import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Comparator;
import ecs.ECSNode;
import app_kvECS.ZKManager;
import ecs.IECSNode;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import logging.LogSetup;
import java.math.BigInteger;
import java.io.IOException;

import org.apache.zookeeper;

public class ECSClient implements IECSClient {

    private ArrayList<ECSNode> serverPool;
//    private ArrayList<ECSNode> activeServerPool;
    private int serverPoolCount;
    private int activeServerPoolCount;
    private static ZKManager zkManager;


    public ECSClient(String fileName){
        try
        {
            File file = new File(fileName);
            FileReader fileReader = new FileReader(file);
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            serverPool = new ArrayList<ECSNode>();
            int i = 0;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
                System.out.println(Integer.toString(i));
                String[] parts = line.trim().split(" ");
                String hostName = parts[0];
                String ip = parts[1];
                int port = Integer.parseInt(parts[2]);
                ECSNode node = new ECSNode(hostName, ip, port);
                serverPool.add(node);
//                System.out.println(serverPool.get(i).getNodeName());
//                System.out.println(serverPool.get(i).getNodeHost());
//                System.out.println(serverPool.get(i).getNodePort());
                i = i + 1;
            }

            this.serverPoolCount = i;
            Collections.sort(serverPool, new Comparator<ECSNode>(){
                public int compare(ECSNode node1, ECSNode node2){
                    BigInteger bi1 = new BigInteger(node1.selfHash, 16);
                    BigInteger bi2 = new BigInteger(node2.selfHash, 16);
                    return bi1.compareTo(bi2);
                }
            });
            for (int j = 0; j < i; j++) {
//                System.out.println(item.getNodeName());
//                System.out.println(item.getNodeHost());
//                System.out.println(item.getNodePort());
                if(j == (i-1)){
                    serverPool.get(j).boundHash = serverPool.get(0).selfHash;
                }
                else{
                    serverPool.get(j).boundHash = serverPool.get(j+1).selfHash;
                }
                System.out.println(serverPool.get(j).selfHash + " to " + serverPool.get(j).boundHash);
            }
            zkmanager = new ZKManager();
        }
        catch (Exception e) {
            System.out.println("Error! Unable to open file!");
        }

    }


    public void initializeKVServer(ECSNode node){
        try {
            String command = "ssh -n " + node.getNodeHost() + " nohup java -jar <path>/ms2-server.jar " + node.getNodePort() +" ERROR &";
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
        for(int i = 0; i < serverPoolCount; i++){
            if(serverPool.get(i).used == 0){
                serverPool.get(i).used = 1;
                serverPool.get(i).cacheStrategy = cacheStrategy;
                serverPool.get(i).cacheSize = cacheSize;
                initializeKVServer(serverPool.get(i));
                activeServerPoolCount += 1;
                // ZNode Path
                String path = "/" + serverPool.get(i).getNodeName();
                byte[] data = "COMMAND:START STATUS:BLCOKED".getBytes();
                zkmanager.create(path, data);
                Stat stat = zkmanager.getZNodeStats(path);
                System.out.println("ZNode Stat: " + stat.toString());
                return serverPool.get(i);
            }
        }
        return null;
    }

    @Override
    public Collection<IECSNode> addNodes(int count, String cacheStrategy, int cacheSize) {
        if((activeServerPoolCount+count) > serverPoolCount){
            return null;
        }
        for(int i = 0; i < serverPoolCount; i++){
            if(serverPool.get(i).used == 0){
                serverPool.get(i).used = 1;
                serverPool.get(i).cacheStrategy = cacheStrategy;
                serverPool.get(i).cacheSize = cacheSize;
                initializeKVServer(serverPool.get(i));
                return serverPool.get(i);
            }
        }
        return null;
    }

    @Override
    public Collection<IECSNode> setupNodes(int count, String cacheStrategy, int cacheSize) {
        // TODO
        return null;
    }

    @Override
    public boolean awaitNodes(int count, int timeout) throws Exception {
        // TODO
        return false;
    }

    @Override
    public boolean removeNodes(Collection<String> nodeNames) {
        // TODO
        return false;
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
        }
    }
}
