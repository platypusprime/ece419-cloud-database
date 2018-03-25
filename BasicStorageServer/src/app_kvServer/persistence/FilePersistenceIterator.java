package app_kvServer.persistence;

import static common.zookeeper.ZKSession.UTF_8;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Predicate;

/**
 * This class provides batch access to key-value pairs in a file persistence.
 * Should only be used when the underlying persistence is locked for writing.
 */
public class FilePersistenceIterator implements KVPersistenceIterator {

	/** The maximum chunk size. */
	public static final int MAX_CHUNK_SIZE = 512 * 1024;

	private final Scanner scanner;

	/**
	 * Creates an iterator for the given persistence file.
	 * 
	 * @param filename The path for the persistence file
	 * @throws FileNotFoundException If the persistence file cannot be found
	 */
	public FilePersistenceIterator(String filename) throws FileNotFoundException {
		this.scanner = new Scanner(new File(filename), UTF_8);
	}

	@Override
	public void close() throws IOException {
		scanner.close();
	}

	@Override
	public boolean hasNextChunk() {
		return scanner.hasNextLine();
	}

	@Override
	public Map<String, String> nextChunk() {
		return nextChunk(key -> true);
	}

	@Override
	public Map<String, String> nextChunk(Predicate<String> keyPredicate) {
		Map<String, String> pairs = new HashMap<String, String>();

		for (int size = 0; scanner.hasNextLine() && size < MAX_CHUNK_SIZE; scanner.nextLine()) {

			String key = scanner.findInLine("[^ ]+");
			String value = scanner.findInLine("(?<= )[^\\n]+");

			if (keyPredicate.test(key)) {
				size += (key.length() + value.length());
				pairs.put(key, value);
			}
		}

		return pairs;
	}

}