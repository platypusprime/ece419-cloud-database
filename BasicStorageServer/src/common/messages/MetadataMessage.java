package common.messages;

import ecs.IECSNode;
import common.HashUtil;
import java.lang.String;
import java.lang.StringBuilder;
/**
 * A message for communicating metadata updates.
 */
public class MetadataMessage implements KVMessage {

    /**
     * TODO document me
     *
     * @param metadata TODO document me
     * @param status TODO document me
     */

    private StatusType status;
    private String hashStart;
    private String hashEnd;
    private String cacheStrat;
    private String cacheSize;


    public MetadataMessage (String metaData){
        String line[] = metaData.split(" ");
        this.status = StatusType.valueOf(line[0]);
        this.setStart(line[1]);
        this.setEnd(line[2]);
        this.cacheStrat = line[3];
        this.cacheSize = line[4];
    }

    public MetadataMessage (StatusType status, String hashStart, String hashEnd, String cacheStrat, String cacheSize){
        this.status = status;
        this.setStart(hashStart);
        this.setEnd(hashEnd);
        this.cacheStrat = cacheStrat;
        this.cacheSize = cacheSize;
    }
    @Override
    public StatusType getStatus() {
        return status;
    }

    public void setStatus(StatusType status) {
        this.status = status;
    }

    public void setStart(String start) throws IllegalArgumentException {
        if (!HashUtil.validateHash(start))
            throw new IllegalArgumentException("Start value \"" + start + "\" is not a valid MD5 hash");
        this.hashStart = start;
    }

    public void setEnd(String end) throws IllegalArgumentException {
        if (!HashUtil.validateHash(end))
            throw new IllegalArgumentException("End value \"" + end + "\" is not a valid MD5 hash");
        this.hashEnd = end;
    }

    @Override
    public String toMessageString() {
        if (status == null) {
            return null;
        }
        StringBuilder msgBuilder = new StringBuilder();
        msgBuilder.append(status.name());
        if (hashStart != null && !hashStart.isEmpty()) {
            msgBuilder.append(" ").append(hashStart);
        }
        if (hashEnd != null && !hashEnd.isEmpty()) {
            msgBuilder.append(" ").append(hashEnd);
        }
        if (cacheStrat != null && !cacheStrat.isEmpty()) {
            msgBuilder.append(" ").append(cacheStrat);
        }
        if (cacheSize != null && !cacheSize.isEmpty()) {
            msgBuilder.append(" ").append(cacheSize);
        }

        return msgBuilder.append("\n").toString();
    }

    @Override
    public IECSNode getResponsibleServer() {
        // TODO Auto-generated method stub
        return null;
    }

}
