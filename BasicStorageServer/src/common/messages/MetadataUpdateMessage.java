package common.messages;

import java.util.Set;

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
	public MetadataUpdateMessage(Set<ServerMetadata> metadata, StatusType status) {
		// TODO Auto-generated method stub
	}

	@Override
	public StatusType getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<ServerMetadata> getServerMetadata() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toMessageString() {
		// TODO Auto-generated method stub
		return null;
	}

}
