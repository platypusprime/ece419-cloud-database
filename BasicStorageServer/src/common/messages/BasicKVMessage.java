package common.messages;

public class BasicKVMessage implements SerializableKVMessage {

	private String key = null;
	private String value = null;
	private StatusType status = null;

	public BasicKVMessage(String key, String value, StatusType status) {
		this.key = key;
		this.value = value;
		this.status = status;
	}
	
	public BasicKVMessage(String stringMessage) {
		String[] tokens = stringMessage.split(",");
		this.key = tokens[1].replace("KEY:", "");
		this.value = tokens[2].replace("VALUE:", "");
		this.status = StatusType.valueOf(tokens[0].replace("STATUS:", ""));
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
	public String toMessageString() {
		return new StringBuilder()
				.append("STATUS:").append(status.name()).append(",")
				.append("KEY:").append(status.name()).append(",")
				.append("VALUE:").append(status.name())
				.toString();
	}

}
