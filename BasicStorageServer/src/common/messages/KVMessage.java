package common.messages;

/**
 * Provides the interface for a network communications message which can contain
 * a key, value, and status message.
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
		DELETE_ERROR
	}

	/**
	 * Retrieves the key associated with this message.
	 * 
	 * @return The key, or <code>null</code> if no key is associated
	 */
	public String getKey();

	/**
	 * Retrieves the value associated with this message.
	 * 
	 * @return The value, or <code>null</code> if no value is associated
	 */
	public String getValue();

	/**
	 * Retrieves the status associated with this message. Used to identify
	 * request/response/error types.
	 * 
	 * @return The status
	 */
	public StatusType getStatus();

	/**
	 * Serializes this KV message as a simple string.
	 * 
	 * @return The serialized message string
	 */
	public String toMessageString();

}
