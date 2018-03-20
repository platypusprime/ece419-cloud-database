package common.messages;

import ecs.IECSNode;

/**
 * A message for communicating metadata updates.
 */
public class MetadataUpdateMessage implements KVMessage {
	
	private IECSNode responsibleNode;
	private StatusType status = StatusType.SERVER_NOT_RESPONSIBLE;

	/**
	 * TODO document me
	 * 
	 * @param metadata TODO document me
	 * @param status TODO document me
	 */
	public MetadataUpdateMessage(IECSNode responsibleNode) {
		this.responsibleNode = responsibleNode;
	}

	@Override
	public StatusType getStatus() {
		return this.status;
	}

	@Override
	public IECSNode getResponsibleServer() {
		return this.responsibleNode;
	}
	
	@Override
	public String toString() {
		StringBuilder msgBuilder = new StringBuilder("MetadataUpdateMessage{ ")
				.append("status=\"").append(status == null ? "null" : status.name()).append("\" ")
				.append("node_name=\"").append(responsibleNode.getNodeName() == null ? "null" : responsibleNode.getNodeName()).append("\" ")
				.append("node_host=\"").append(responsibleNode.getNodeHost() == null ? "null" : responsibleNode.getNodeHost()).append("\" ")
				.append("node_port=\"").append(responsibleNode.getNodePort()).append("\" ");

		return msgBuilder.toString();
	}

}
