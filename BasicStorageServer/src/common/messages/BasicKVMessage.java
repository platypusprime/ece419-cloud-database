package common.messages;

/**
 * A simple implementation of a message containing key, value,
 * and status information.
 */
public class BasicKVMessage implements KVMessage {

	private final String key;
	private final String value;
	private final StatusType status;

	/**
	 * Creates a KV message with the specified key, value and status. Any of these
	 * values can be <code>null</code>.
	 * 
	 * @param key The key to set
	 * @param value The value to set
	 * @param status The message status to set
	 */
	public BasicKVMessage(String key, String value, StatusType status) {
		this.key = key;
		this.value = value;
		this.status = status;
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public String getValue() {
		return value;
	}

	@Override
	public StatusType getStatus() {
		return status;
	}

	@Override
	public String toString() {
		StringBuilder msgBuilder = new StringBuilder("BasicKVMessage{ ")
				.append("status=\"").append(status == null ? "null" : status.name()).append("\" ")
				.append("key=\"").append(key == null ? "null" : key).append("\" ")
				.append("value=\"").append(value == null ? "null" : value).append("\" ");

		return msgBuilder.toString();
	}

}
