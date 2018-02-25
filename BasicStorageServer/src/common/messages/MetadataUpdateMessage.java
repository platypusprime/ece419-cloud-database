package common.messages;

import ecs.IECSNode;

/**
 * A message for communicating metadata updates.
 */
public class MetadataUpdateMessage implements KVMessage {

	/**
	 * TODO document me
	 * 
	 * @param metadata TODO document me
	 * @param status TODO document me
	 */
	public MetadataUpdateMessage(IECSNode metadata, StatusType status) {
		// TODO Auto-generated method stub
	}

	@Override
	public StatusType getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toMessageString() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IECSNode getResponsibleServer() {
		// TODO Auto-generated method stub
		return null;
	}

}
