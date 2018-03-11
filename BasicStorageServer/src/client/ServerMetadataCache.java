package client;

import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

import ecs.IECSNode;

/**
 * A client-side cache for server metadata. Data contained in this object is not
 * necessarily the most up-to-date. Instead, it represents a client's best guess
 * as to the full set of current server metadata based on prior server
 * responses.
 */
public class ServerMetadataCache {

	private final NavigableMap<String, IECSNode> metadataCache;

	/**
	 * Creates an empty server metadata cache.
	 */
	public ServerMetadataCache() {
		metadataCache = new TreeMap<>();
	}

	/**
	 * Identifies the server which is responsible for the given hash, given the
	 * information available to this cache.
	 * 
	 * @param keyHash The MD5 hash for the key to find a server for
	 * @return The server matching the key,
	 *         or <code>null</code> if the cache is empty
	 */
	public IECSNode findServer(String keyHash) {

		if (metadataCache.isEmpty())
			return null;

		return Optional
				// find the first server past the key in the hash ring
				.ofNullable(metadataCache.ceilingEntry(keyHash))
				.map(Map.Entry::getValue)

				// otherwise use the first server (guaranteed to exist because of above check)
				.orElse(metadataCache.firstEntry().getValue());
	}

	/**
	 * Updates a server's metadata in this cache. Should be called whenever a client
	 * receives information about a responsible server in a
	 * <code>SERVER_NOT_RESPONSIBLE</code> response.
	 * 
	 * @param metadata The metadata information to update in the cache
	 */
	public void updateServer(IECSNode metadata) {
		metadataCache.put(metadata.getNodeHashRangeStart(), metadata);
	}

	/**
	 * Removes a server's metadata from this cache. Should be called when a server
	 * that is believed to be responsible for a particular key turns out to no
	 * longer be responsible. This may happen when:
	 * <ol>
	 * <li>The server has been brought down and is no longer servicing requests</li>
	 * <li>A new server has been brought up and is now servicing part of the new
	 * server's former key range</li>
	 * <li>A combination of the above</li>
	 * </ol>
	 * 
	 * @param metadata The stale metadata
	 */
	public void invalidateServer(IECSNode metadata) {
		metadataCache.remove(metadata.getNodeHashRangeStart());
	}
}
