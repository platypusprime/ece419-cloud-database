package common.messages;

import ecs.IECSNode;

/**
 * Provides the interface for a network communications message which contains a
 * status message and optional key, value, and/or metadata information.
 */
public interface KVMessage {

	/** Contains all allowable status types for {@link KVMessage}. */
	public static enum StatusType {
		/** Get - request */
		GET,
		/** requested tuple (i.e. value) not found */
		GET_ERROR,
		/** requested tuple (i.e. value) found */
		GET_SUCCESS,
		/** Put - request */
		PUT,
		/** Put - request successful, tuple inserted */
		PUT_SUCCESS,
		/** Put - request successful, i.e. value updated */
		PUT_UPDATE,
		/** Put - request not successful */
		PUT_ERROR,
		/** Delete - request successful */
		DELETE_SUCCESS,
		/** Delete - request successful */
		DELETE_ERROR,

		/** Server is stopped, no requests are processed */
		SERVER_STOPPED,
		/** Server locked for out, only get possible */
		SERVER_WRITE_LOCK,
		/** Request not successful, server not responsible for key */
		SERVER_NOT_RESPONSIBLE
	}

	/** The JSON attribute name for the status. */
	public static final String STATUS_ATTR = "status";

	/** The JSON attribute name for the key. */
	public static final String KEY_ATTR = "key";

	/** The JSON attribute name for the value. */
	public static final String VALUE_ATTR = "value";

	/** The JSON attribute name for the responsible node. */
	public static final String RESPONSIBLE_NODE_ATTR = "responsibleNode";

	/**
	 * Retrieves the key associated with this message.
	 * 
	 * @return The key, or <code>null</code> if no key is associated
	 */
	public default String getKey() {
		return null;
	}

	/**
	 * Retrieves the value associated with this message.
	 * 
	 * @return The value, or <code>null</code> if no value is associated
	 */
	public default String getValue() {
		return null;
	}

	/**
	 * Retrieves the status associated with this message. Used to identify
	 * request/response/error types.
	 * 
	 * @return The status
	 */
	public StatusType getStatus();

	/**
	 * Retrieves the responsible server information associated with this message.
	 * 
	 * @return The responsible server node
	 */
	public default IECSNode getResponsibleServer() {
		return null;
	}

	/**
	 * Serializes this KV message as a simple string.
	 * 
	 * @return The serialized message string
	 */
	public String toMessageString();

}
