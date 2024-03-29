package app_kvServer.persistence;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;

import org.apache.log4j.Logger;

import common.HashUtil;

/**
 * This class manipulates a file-based persistence system for key-value
 * databases. The persistence consists of a single file in which key-value pairs
 * are delimited by line breaks and keys are separated from values by a single
 * space. Note that this is possible since keys are guaranteed to not contain
 * space characters.
 * <p>
 * TODO make this thread-safe
 */
public class FilePersistence implements KVPersistence {

	/** The default filename for persistence files. */
	public static final String DEFAULT_PERSISTENCE_FILENAME = "persistence.csv";

	/** Naming information for the scratch file used in write operations */
	private static final String SCRATCH_FILE_PREFIX = "ece419-put-buffer";
	private static final String SCRATCH_FILE_SUFFIX = ".csv";

	/** The logger for this class. */
	private Logger log = Logger.getLogger(FilePersistence.class);

	/** The path to the file containing the key-value pairs. */
	private final String filename;

	/**
	 * Creates a key-value persistence using the default persistence file
	 * (<code>persistence.csv</code>).
	 * 
	 * @deprecated Use {@link FilePersistence#FilePersistence(String)
	 *             FilePersistence(String)} instead
	 */
	@Deprecated
	public FilePersistence() {
		this(DEFAULT_PERSISTENCE_FILENAME);
	}

	/**
	 * Creates a key-value persistence using the specified persistence file. If the
	 * file does not currently exist, it is created.
	 * 
	 * @param filename The file to load/store KV data
	 */
	public FilePersistence(String filename) {
		log.info("Creating persistence manager for file: " + filename);
		this.filename = filename;

		// check to see that the file exists
		File f = new File(filename);
		if (!f.exists()) {
			log.debug("Creating missing persistence file: " + filename);
			try {
				f.getAbsoluteFile().getParentFile().mkdirs();
				f.createNewFile();

			} catch (IOException e) {
				log.fatal("Could not create missing persistence file", e);
				System.exit(1);
			}
		}
	}

	@Override
	public boolean containsKey(String key) {
		try (Scanner scanner = new Scanner(new File(filename), UTF_8.name())) {
			while (scanner.hasNextLine()) {
				String currKey = scanner.next("[^ ]+");
				if (currKey.equals(key)) return true;
				scanner.nextLine();
			}
			return false;

		} catch (FileNotFoundException e) {
			log.warn("Persistence file could not be found", e);
			return false;
		}
	}

	/**
	 * Creates a scratch file in the default temporary files directory.
	 * 
	 * @return A file object pointing to the newly created scratch file
	 * @throws IOException If the file could not be created
	 */
	private File generateScratchFile() throws IOException {
		File tempFile = File.createTempFile(SCRATCH_FILE_PREFIX, SCRATCH_FILE_SUFFIX);
		tempFile.deleteOnExit();
		return tempFile;
	}

	@Override
	public String get(String key) {
		log.info("Looking up key '" + key + "' in persistence...");

		try (Scanner scanner = new Scanner(new File(filename), UTF_8.name())) {
			while (scanner.hasNextLine()) {
				String currKey = scanner.findInLine("[^ ]+");
				if (currKey.equals(key)) {
					String value = scanner.findInLine("(?<= )[^\\n]+");
					log.info("Value found in persistence for key '" + key + "': '" + value + "'");
					return value;
				}
				scanner.nextLine();
			}

			log.info("Value for key '" + key + "' not found");
			return null;

		} catch (FileNotFoundException e) {
			log.warn("Persistence file could not be found", e);
			return null;
		}
	}

	@Override
	@Deprecated
	public Map<String, String> getAll() {
		Map<String, String> pairs = new HashMap<String, String>();
		log.info("Loading all key values pairs from persistence...");
	
		try (Scanner scanner = new Scanner(new File(filename), UTF_8.name())) {
			while (scanner.hasNextLine()) {
				String key = scanner.findInLine("[^ ]+");
				String value = scanner.findInLine("(?<= )[^\\n]+");
	
				pairs.put(key, value);
				scanner.nextLine();
			}
	
		} catch (FileNotFoundException e) {
			log.warn("Persistence file could not be found", e);
			return null;
		}
	
		return pairs;
	}

	@Override
	public KVPersistenceChunkator chunkator() {
		try {
			return new FilePersistenceChunkator(filename);
		} catch (FileNotFoundException e) {
			log.error("Persistence file could not be found", e);
			return null;
		}
	}

	@Override
	public String put(String key, String value) {
		String prevValue = null;

		if (value == null) {
			return delete(key);
		}

		try (RandomAccessFile r = new RandomAccessFile(filename, "rw");
				RandomAccessFile rtemp = new RandomAccessFile(generateScratchFile(), "rw");
				FileChannel sourceChannel = r.getChannel();
				FileChannel targetChannel = rtemp.getChannel()) {
			long fileSize = r.length();
			long offset = -1L;

			// attempt to find an existing value
			String ln;
			while ((ln = r.readLine()) != null) {
				String currKey = ln.substring(0, ln.indexOf(' '));
				if (currKey.equals(key)) {
					offset = r.getFilePointer() - ln.length() + key.length();
					prevValue = ln.substring(ln.indexOf(' ') + 1, ln.length());
					break;
				}
			}

			if (offset >= 0L) {
				sourceChannel.transferTo(offset, (fileSize - offset), targetChannel);
				sourceChannel.truncate(offset);
				r.seek(offset);
				r.write(value.getBytes());
				long newOffset = r.getFilePointer();
				targetChannel.position(prevValue.length());
				sourceChannel.transferFrom(targetChannel, newOffset, (fileSize - offset));
			} else {
				r.write(String.format("%s %s\n", key, value).getBytes(UTF_8));
			}

		} catch (IOException e) {
			log.error("I/O exception while writing to persistence file", e);
		}

		return prevValue;
	}

	@Override
	public boolean insertAll(Map<String, String> pairs) {
		try {
			RandomAccessFile r = new RandomAccessFile(filename, "rw");
			r.seek(r.length());

			for (Entry<String, String> entry : pairs.entrySet()) {
				String key = entry.getKey();
				String value = entry.getValue();

				r.write(String.format("%s %s\n", key, value).getBytes(UTF_8));
			}
			r.close();

		} catch (IOException e) {
			log.error("I/O exception while writing to persistence file", e);
			return false;
		}

		return true;
	}

	private String delete(String key) {
		String prevValue = null;
		try (RandomAccessFile r = new RandomAccessFile(filename, "rw");
				RandomAccessFile rtemp = new RandomAccessFile(generateScratchFile(), "rw");
				FileChannel sourceChannel = r.getChannel();
				FileChannel targetChannel = rtemp.getChannel()) {
			long fileSize = r.length();
			long offset = -1L;

			// attempt to find an existing value
			String ln;
			while ((ln = r.readLine()) != null) {
				String currKey = ln.substring(0, ln.indexOf(' '));
				if (currKey.equals(key)) {
					offset = r.getFilePointer() - ln.length() - 1;
					prevValue = ln.substring(ln.indexOf(' ') + 1, ln.length());
					break;
				}
			}

			if (offset >= 0L) {
				sourceChannel.transferTo(offset, (fileSize - offset), targetChannel);
				sourceChannel.truncate(offset);
				r.seek(offset);
				targetChannel.position(ln.length() + 1);
				sourceChannel.transferFrom(targetChannel, offset, (fileSize - offset));
			}

		} catch (IOException e) {
			log.error("I/O exception while writing to persistence file", e);
		}

		return prevValue;
	}

	@Override
	public void clear() {
		try (FileWriter writer = new FileWriter(new File(filename), false)) {
			// no writing necessary to clear the file

		} catch (IOException e) {
			log.error("I/O exception while clearing persistence file", e);
		}
	}

	@Override
	public void clearRange(String[] hashRange) {
		try (RandomAccessFile r = new RandomAccessFile(filename, "rw");
				RandomAccessFile rtemp = new RandomAccessFile(generateScratchFile(), "rw");
				FileChannel sourceChannel = r.getChannel();
				FileChannel targetChannel = rtemp.getChannel()) {

			String ln;
			while ((ln = r.readLine()) != null) {
				String key = ln.substring(0, ln.indexOf(' '));
				if (!HashUtil.containsHash(HashUtil.toMD5(key), hashRange)) {
					rtemp.write(ln.concat("\n").getBytes(UTF_8));
				}
			}

			sourceChannel.truncate(0);
			r.seek(0);
			targetChannel.position(0);
			sourceChannel.transferFrom(targetChannel, 0, rtemp.length());

		} catch (IOException e) {
			log.error("I/O exception while writing to persistence file", e);
		}

	}

}
