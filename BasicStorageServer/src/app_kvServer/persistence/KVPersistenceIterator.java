package app_kvServer.persistence;

import java.io.Closeable;
import java.util.Map;
import java.util.function.Predicate;

/**
 * This interface provides the interface for chunk-wise iteration through
 * key-value pairs contained in a persistence.
 */
public interface KVPersistenceIterator extends Closeable {

	/**
	 * Determines whether there is still data remaining to be iterated over.
	 * 
	 * @return <code>true</code> if there is at least one key-value pair that has
	 *         not yet been iterated over, <code>false</code> otherwise
	 */
	public boolean hasNextChunk();

	/**
	 * Retrieves the next chunk of data from the persistence. A chunk will always
	 * contain complete key-value pairs and will exceed 512 kBytes in combined
	 * key/value size by at most one key-value pair.
	 * 
	 * @return The key-value pairs in the next chunk
	 */
	public Map<String, String> nextChunk();

	/**
	 * Retrieves the next chunk of data satisfying the predicate from the
	 * persistence. A chunk will always contain complete key-value pairs and will
	 * not exceed 512 kBytes in combined key/value size.
	 * 
	 * @param keyPredicate The predicate to apply on keys which decides which
	 *            key-value
	 *            pairs are returned in the chunk
	 * @return The key-value pairs in the next chunk
	 */
	public Map<String, String> nextChunk(Predicate<String> keyPredicate);

}