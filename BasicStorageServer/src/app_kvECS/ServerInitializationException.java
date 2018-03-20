package app_kvECS;

/**
 * Thrown when a problem occurs during server initialization.
 */
@SuppressWarnings("javadoc")
public class ServerInitializationException extends Exception {
	private static final long serialVersionUID = -6831517968851645050L;

	public ServerInitializationException() {
		super();
	}

	public ServerInitializationException(String msg) {
		super(msg);
	}

	public ServerInitializationException(String msg, Throwable t) {
		super(msg, t);
	}
}