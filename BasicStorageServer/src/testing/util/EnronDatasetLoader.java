package testing.util;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Pattern;

import app_kvServer.persistence.FilePersistence;

/**
 * A utility which converts the Enron corpus into key-value pairs that are
 * usable for testing by the key-value database service.
 */
public class EnronDatasetLoader {

	private static final Pattern MESSAGE_ID_PATTERN = Pattern.compile("\\d+");
	private static final Pattern LAST_METADATA_LINE_PATTERN = Pattern.compile("X-FileName.*");
	private static final Pattern VALUE_PATTERN = Pattern.compile(".*", Pattern.DOTALL);

	private static final int MAX_VALUE_LENGTH = 120 * 1000;

	/**
	 * Converts a given number of emails in a directory into a single text file,
	 * which can be used by a {@link FilePersistence} instance.
	 * 
	 * @param args Expects <code>args[0]</code> to contain the root directory of the
	 *            Enron corpus and <code>args[1]</code> to be the number of emails
	 *            (i.e. entries) to load
	 */
	public static void main(String[] args) {
		Path dir = Paths.get(args[0]);
		Map<String, String> data = streamEmails(dir, Integer.parseInt(args[1]));

		FilePersistence persistence = new FilePersistence("enron.txt");
		persistence.clear();
		persistence.insertAll(data);
	}

	/**
	 * Iterates recursively through the given directory and converts up to
	 * <code>num</code> emails into key-value pairs.
	 * 
	 * @param dir The directory to stream emails from
	 * @param num The maximum number of key-value pairs to parse
	 * @return A map containing all loaded key-value pairs
	 */
	public static Map<String, String> streamEmails(Path dir, int num) {
		Map<String, String> data = new HashMap<>();
		System.out.println("Streaming emails in " + dir);
		try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir)) {
			for (Path path : stream) {
				if (Files.isDirectory(path)) {
					data.putAll(streamEmails(path, num - data.size()));
				} else {
					Map.Entry<String, String> entry = parseEmail(path);
					if (entry != null) {
						data.put(entry.getKey(), entry.getValue());
					}
				}
				if (data.size() >= num) break;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return data;

	}

	/**
	 * Parses an email file from the Enron corpus. Uses the numeric portion of each
	 * email's Message-ID as a key and the body of the email as a value. Since the
	 * numeric Message-ID is often 21 characters long, two characters are removed
	 * from the middle (i.e. after the period), since the characters there are
	 * usually "10" and are unlikely to significantly weaken the uniqueness of the
	 * IDs. The value is clamped to 120 kBytes.
	 * 
	 * @param emailFilePath The path for the email file
	 * @return A key-value pair loaded from the email
	 */
	public static Map.Entry<String, String> parseEmail(Path emailFilePath) {
		try (Scanner scanner = new Scanner(new FileInputStream(emailFilePath.toFile()))) {
			String messageId1 = scanner.findInLine(MESSAGE_ID_PATTERN);
			String messageId2 = scanner.findInLine(MESSAGE_ID_PATTERN);
			if (messageId1 == null || messageId2 == null) return null;

			String key = messageId1.concat(messageId2.substring(2));

			scanner.findWithinHorizon(LAST_METADATA_LINE_PATTERN, 0);
			String value = scanner.findWithinHorizon(VALUE_PATTERN, 0);
			if (value.length() > MAX_VALUE_LENGTH)
				value = value.substring(0, MAX_VALUE_LENGTH);
			value = value.replaceAll("\\r?\\n", " "); // remove newlines
			return new AbstractMap.SimpleEntry<String, String>(key, value);

		} catch (FileNotFoundException e) {
			return null;
		}
	}

}
