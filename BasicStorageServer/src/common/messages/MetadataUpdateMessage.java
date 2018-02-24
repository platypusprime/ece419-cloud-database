package common.messages;

import java.net.InetSocketAddress;
import java.util.Map;

/**
 * A message for communicating metadata updates.
 */
public class MetadataUpdateMessage implements KVMessage {

	/**
	 * TODO document me
	 */
	public MetadataUpdateMessage() {
		// TODO Auto-generated method stub
	}

	@Override
	public StatusType getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<HashRange, InetSocketAddress> getHashRanges() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String toMessageString() {
		// TODO Auto-generated method stub
		return null;
	}

}
