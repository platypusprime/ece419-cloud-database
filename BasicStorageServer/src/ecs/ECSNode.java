package ecs;

import java.net.InetSocketAddress;
import java.util.Objects;

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

	private final String name;
	private final String host;
	private final int port;
	private final String start;
	private String end;
	private final String cacheStrategy;
	private final int cacheSize;

	/**
	 * Creates a sparse server metadata object. Used on the client-side application
	 * as an initial server.
	 * 
	 * @param host The hostname for the associated server
	 * @param port The listening port number for the associated server
	 */
	public ECSNode(String host, int port) {
		this(null, host, port, null, -1);
	}

	/**
	 * Creates a server metadata object. Contains information about the server's
	 * listening socket, hash range, and cache configuration. The hash range
	 * descends from <code>start</code> (inclusive) to <code>end</code> (exclusive).
	 * 
	 * @param name A string naming the associated server
	 * @param host The hostname for the associated server
	 * @param port The listening port number for the associated server
	 * @param cacheStrategy The cache strategy for the associated server
	 * @param cacheSize The cache size for the associated server
	 */
	public ECSNode(String name, String host, int port, String cacheStrategy, int cacheSize) {
		this.name = name;
		this.host = host;
		this.port = port;
		this.start = HashUtil.toMD5(host, port);
		this.end = null;
		this.cacheStrategy = cacheStrategy;
		this.cacheSize = cacheSize;
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

	@Override
	public String getNodeHashRangeStart() {
		return start;
	}

	@Override
	public String getNodeHashRangeEnd() {
		return end;
	}

	@Override
	public void setNodeHashRangeEnd(String end) throws IllegalArgumentException {
		if (!HashUtil.validateHash(end))
			throw new IllegalArgumentException("End value \"" + end + "\" is not a valid MD5 hash");
		this.end = end;
	}

	@Override
	public boolean containsHash(String hash) {
		if (!HashUtil.validateHash(hash)) {
			return false;
		}

		// no end value corresponds to full hash circle
		if (end == null) {
			return true;
		}

		if (start.compareTo(end) > 0) {
			// no wrap-around
			return hash.compareTo(start) <= 0 && hash.compareTo(end) > 0;
		} else {
			// wrap-around
			return hash.compareTo(start) <= 0 || hash.compareTo(end) > 0;
		}
	}

	@Override
	public String toString() {
		return new StringBuilder("ECSNode{")
				.append(" name:").append(String.valueOf(name))
				.append(" address:").append(host).append(":").append(port)
				.append(" range:[").append(start).append(",").append(end == null ? "" : end).append(")")
				.append(" }").toString();
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof IECSNode)) return false;

		IECSNode node = (IECSNode) o;
		return Objects.equals(name, node.getNodeName())
				&& Objects.equals(host, node.getNodeHost())
				&& Objects.equals(port, node.getNodePort());
	}

	@Override
	public int hashCode() {
		return Objects.hash(name, host, port);
	}

	@Override
	public String getCacheStrategy() {
		return cacheStrategy;
	}

	@Override
	public int getCacheSize() {
		return cacheSize;
	}

}