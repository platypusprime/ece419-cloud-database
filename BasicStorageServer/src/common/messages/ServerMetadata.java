package common.messages;

import java.net.InetSocketAddress;

/**
 * An object containing server metadata, including socket information and the
 * range of hash values which the associated server is responsible for.
 * <p>
 * Hash ranges are a range of hash values on a hash ring going from
 * <code>0x0</code> to <code>0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF</code>. Ranges
 * can wrap around these limits.
 */
public class ServerMetadata {

	private String host;
	private int clientPort;
	private int serverPort;
	private String start;
	private String end;

	/**
	 * Creates a server metadata object for client use. Contains information about
	 * the client listening socket and the associated hash range. The hash range
	 * descends from <code>start</code> to <code>end</code>.
	 * 
	 * @param host The hostname for the associated server
	 * @param clientPort The port number for client connections
	 * @param start The hash value of the start index (inclusive)
	 * @param end The hash value of the end index (exclusive)
	 */
	public ServerMetadata(String host, int clientPort, String start, String end) {
		this(host, clientPort, -1, start, end);
	}

	/**
	 * Creates a server metadata object for ECS and server use. Contains information
	 * about both the client and server listening sockets, and the associated hash
	 * range. The hash range descends from <code>start</code> to <code>end</code>.
	 * 
	 * @param host The hostname for the associated server
	 * @param clientPort The port number for client connections
	 * @param serverPort The port number for ECS and server connections
	 * @param start The hash value of the start index (inclusive)
	 * @param end The hash value of the end index (exclusive)
	 */
	public ServerMetadata(String host, int clientPort, int serverPort, String start, String end) {
		this.host = host;
		this.clientPort = clientPort;
		this.serverPort = serverPort;
		this.setStart(start);
		this.setEnd(end);
	}

	/**
	 * Returns the hostname of the associated server.
	 * 
	 * @return THe hostname
	 */
	public String getHost() {
		return host;
	}

	/**
	 * Returns the port number for client connections to the associated server.
	 * 
	 * @return The client port
	 */
	public int getClientPort() {
		return clientPort;
	}

	/**
	 * Returns the port number for ECS and server-server connections to the
	 * associated server.
	 * 
	 * @return The server port
	 */
	public int getServerPort() {
		return serverPort;
	}

	/**
	 * Returns the socket address for client connections to the associated server.
	 * 
	 * @return The client socket address
	 */
	public InetSocketAddress getClientSocketAddress() {
		return new InetSocketAddress(host, clientPort);
	}

	/**
	 * Returns the socket address for ECS and server-server connections to the
	 * associated server.
	 * 
	 * @return The server socket address
	 */
	public InetSocketAddress getServerSocketAddress() {
		return new InetSocketAddress(host, serverPort);
	}

	/**
	 * Returns the start index of the associated hash range.
	 * 
	 * @return The start index (inclusive)
	 */
	public String getStart() {
		return start;
	}

	/**
	 * Sets the start index of the associated hash range.
	 * 
	 * @param start The start index to set
	 * @throws IllegalArgumentException If the given index is not a valid MD5 hash
	 */
	public void setStart(String start) throws IllegalArgumentException {
		if (!validateHash(start))
			throw new IllegalArgumentException("Start value \"" + start + "\" is not a valid MD5 hash");
		this.start = start;
	}

	/**
	 * Returns the end index of the associated hash range.
	 * 
	 * @return The end index (exclusive)
	 */
	public String getEnd() {
		return end;
	}

	/**
	 * Sets the end index of the associated hash range.
	 * 
	 * @param end The end index to set
	 * @throws IllegalArgumentException If the given index is not a valid MD5 hash
	 */
	public void setEnd(String end) throws IllegalArgumentException {
		if (!validateHash(end))
			throw new IllegalArgumentException("End value \"" + end + "\" is not a valid MD5 hash");
		this.end = end;
	}

	/**
	 * Checks whether the given MD5 hash belongs within the associated hash range.
	 * 
	 * @param hash The hash string to check
	 * @return <code>true</code> if <code>hash</code> lies within this range's start
	 *         index (inclusive) and end index (exclusive) with consideration for
	 *         wrap-around, <code>false</code> otherwise
	 */
	public boolean containsHash(String hash) {
		ServerMetadata.validateHash(hash);

		if (start.compareTo(end) > 0) {
			// no wrap-around
			return hash.compareTo(start) <= 0 && hash.compareTo(end) > 0;
		} else {
			// wrap-around
			return hash.compareTo(start) <= 0 || hash.compareTo(end) > 0;
		}

	}

	/**
	 * Checks whether the given string is a valid MD5 hash. Specifically checks
	 * whether the string contains a 32-digit hexadecimal number.
	 * 
	 * @param hash The string to check
	 * @return <code>true</code> if the given string is a 32-digit hexadecimal
	 *         number, <code>false</code> otherwise
	 */
	public static boolean validateHash(String hash) {
		return hash != null && hash.matches("[0-9a-f]{32}");
	}

}