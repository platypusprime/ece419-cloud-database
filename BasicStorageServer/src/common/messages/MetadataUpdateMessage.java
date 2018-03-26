package common.messages;

import java.util.Objects;

import ecs.IECSNode;

/**
 * A message for responding to client requests sent to the wrong server. This
 * message provides the client with updated metadata for the server which is
 * currently responsible for the request.
 */
public class MetadataUpdateMessage implements KVMessage {

	private static final StatusType MD_UPDATE_STATUS = StatusType.SERVER_NOT_RESPONSIBLE;

	private final IECSNode responsibleServer;

	/**
	 * Creates a message for the given responsible server information.
	 * 
	 * @param responsibleServer The server information to pass on to the client
	 * @throws NullPointerException If the given metadata is <code>null</code>
	 */
	public MetadataUpdateMessage(IECSNode responsibleServer) throws NullPointerException {
		this.responsibleServer = Objects.requireNonNull(responsibleServer);
	}

	@Override
	public StatusType getStatus() {
		return MD_UPDATE_STATUS;
	}

	@Override
	public IECSNode getResponsibleServer() {
		return this.responsibleServer;
	}

	@Override
	public String toString() {
		StringBuilder msgBuilder = new StringBuilder("MetadataUpdateMessage{ ")
				.append("status=\"").append(MD_UPDATE_STATUS).append("\" ")
				.append("responsibleServer=").append(responsibleServer);

		return msgBuilder.toString();
	}

}
