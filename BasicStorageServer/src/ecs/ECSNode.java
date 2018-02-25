package ecs;
import java.util.Collection;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.nio.charset.Charset;

public class ECSNode implements IECSNode{

    private String hostName;
    private int port;
    private String ip;
    public String selfHash;
    public String boundHash;
    public int used = 0;
    public String cacheStrategy;
    public int cacheSize;

    public ECSNode(String hostName, String ip, int port){
        this.hostName = hostName;
        this.ip = ip;
        this.port = port;
        this.selfHash = hash();
    }
    /**
     * @return  the name of the node (ie "Server 8.8.8.8")
     */
    private String hash(){
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            String msg = this.ip.concat(":").concat(Integer.toString(this.port));
            md.update(msg.getBytes());
            byte[] messageDigestMD5 = md.digest();
            StringBuffer stringBuffer = new StringBuffer();
            for (byte bytes : messageDigestMD5) {
                stringBuffer.append(String.format("%02x", bytes & 0xff));
            }
            System.out.println("data:" + msg);
            System.out.println("digestedMD5(hex):" + stringBuffer.toString());
            return stringBuffer.toString();
        } catch (NoSuchAlgorithmException exception) {
            exception.printStackTrace();
            return null;
        }
    }
    public String getNodeName(){
        return this.hostName;
    }

    /**
     * @return  the hostname of the node (ie "8.8.8.8")
     */
    public String getNodeHost(){
        return ip;
    }

    /**
     * @return  the port number of the node (ie 8080)
     */
    public int getNodePort(){
        return port;
    }

    /**
     * @return  array of two strings representing the low and high range of the hashes that the given node is responsible for
     */
    public String[] getNodeHashRange(){
        return new String[]{this.selfHash, this.boundHash};
    }

}
