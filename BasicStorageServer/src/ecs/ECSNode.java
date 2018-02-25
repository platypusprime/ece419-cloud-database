package ecs;

import java.net.InetSocketAddress;

import common.HashUtil;

/**
 * An object containing server metadata, including socket information and the
 * range of hash values which the associated server is responsible for.
 * <p>
 * Hash ranges are a range of hash values on a hash ring going from
 * <code>0x0</code> to <code>0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF</code>. Ranges
 * can wrap around these limits.
 */
public class ECSNode implements IECSNode {

	private String name;
	private String host;
	private int port;
	private String start;
	private String end;

	/**
	 * Creates a server metadata object with default range. Contains information
	 * about both the
	 * server's listening socket and hash range. The hash range descends from
	 * <code>start</code> to <code>end</code>.
	 * 
	 * @param host The hostname for the associated server
	 * @param port The listening port number for the associated server
	 */
	public ECSNode(String host, int port) {
		this(host, port, HashUtil.MAX_MD5, HashUtil.MIN_MD5);
	}

	/**
	 * Creates a server metadata object. Contains information about both the
	 * server's listening socket and hash range. The hash range descends from
	 * <code>start</code> to <code>end</code>.
	 * 
	 * @param host The hostname for the associated server
	 * @param port The listening port number for the associated server
	 * @param start The hash value of the start index (inclusive)
	 * @param end The hash value of the end index (exclusive)
	 */
	public ECSNode(String host, int port, String start, String end) {
		this.host = host;
		this.port = port;
		this.name = "Server " + host + ":" + port;
		this.setStart(start);
		this.setEnd(end);
	}

	@Override
	public String getNodeName() {
		return name;
	}

	@Override
	public String getNodeHost() {
		return host;
	}

	@Override
	public int getNodePort() {
		return port;
	}

	@Override
	public InetSocketAddress getNodeSocketAddress() {
		return new InetSocketAddress(getNodeHost(), getNodePort());
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
		if (!HashUtil.validateHash(start))
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
		if (!HashUtil.validateHash(end))
			throw new IllegalArgumentException("End value \"" + end + "\" is not a valid MD5 hash");
		this.end = end;
	}

	@Override
	public String[] getNodeHashRange() {
		return new String[] { getStart(), getEnd() };
	}

	@Override
	public boolean containsHash(String hash) {
		HashUtil.validateHash(hash);

		if (start.compareTo(end) > 0) {
			// no wrap-around
			return hash.compareTo(start) <= 0 && hash.compareTo(end) > 0;
		} else {
			// wrap-around
			return hash.compareTo(start) <= 0 || hash.compareTo(end) > 0;
		}

	}

}