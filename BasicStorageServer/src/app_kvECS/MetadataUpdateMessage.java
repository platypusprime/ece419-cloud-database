package app_kvECS;

import ecs.ECSNode;
import java.util.Map;
import java.util.HashMap;
import common.messages.MetadataMessage;
import common.messages.KVMessage.StatusType;
import java.lang.StringBuilder;
/**
 * A message for communicating metadata updates.
 */
public class MetadataUpdateMessage {

	/**
	 * TODO document me
	 * 
	 * @param metadata TODO document me
	 * @param status TODO document me
	 */
	private Map<String, MetadataMessage>metaDataTable;
	public MetadataUpdateMessage() {
		metaDataTable = new HashMap<String, MetadataMessage>();
	}

	public void addNode(ECSNode node, StatusType status, String cacheStrat, String cacheSize){
		MetadataMessage metaDataMsg = new MetadataMessage(status, node.getStart(), node.getEnd(), cacheStrat, cacheSize);
		metaDataTable.put(node.getNodeName(), metaDataMsg);
	}

	public void modifyNode(ECSNode node, StatusType status){
		MetadataMessage metaDataMsg = metaDataTable.get(node.getNodeName());
		metaDataMsg.setStatus(status);
		metaDataMsg.setStart(node.getStart());
		metaDataMsg.setEnd(node.getEnd());
		metaDataTable.put(node.getNodeName(), metaDataMsg);
	}

	public void removeNode(String serverName) {
		metaDataTable.remove(serverName);
	}

	public String toMessageString() {
		StringBuilder msgBuilder = new StringBuilder();
		for (Map.Entry<String, MetadataMessage> entry : metaDataTable.entrySet())
		{
			msgBuilder.append(entry.getKey());
			msgBuilder.append("|").append(entry.getValue().toMessageString()).append("\n");

		}
		return msgBuilder.toString();
	}

}
