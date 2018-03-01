package testing.app_kvServer.persistence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Level;
import org.junit.Before;
import org.junit.Test;

import app_kvServer.persistence.FilePersistence;
import app_kvServer.persistence.KVPersistence;
import testing.util.LogInstrumentingTest;

/**
 * Tests the functionality of the {@link FilePersistence} class.
 */
public class FilePersistenceTest extends LogInstrumentingTest {

	/** Naming information for the temporary file used by the persistence under test. */
	private static final String TEST_FILE_PREFIX = "test-persistence";
	private static final String TEST_FILE_SUFFIX = ".csv";

	/** The persistence implementation under test. */
	private KVPersistence persistence;
	private String tempFilePath;

	@Override
	public Class<?> getClassUnderTest() {
		return FilePersistence.class;
	}

	/**
	 * Freshly instantiates the persistence under test before each test.
	 * 
	 * @throws IOException If an exception occurs while creating
	 *             the temporary test persistence file
	 */
	@Before
	public void setup() throws IOException {

		File tempFile = File.createTempFile(TEST_FILE_PREFIX, TEST_FILE_SUFFIX);
		tempFilePath = tempFile.getCanonicalPath();
		tempFile.deleteOnExit();
		persistence = new FilePersistence(tempFilePath);
		assertTrue(persistence.getAll().isEmpty());
	}

	/**
	 * Tests instantiation of a file persistence when the specified file
	 * does not exist.
	 * 
	 * @throws IOException If an exception occurs while creating the
	 *             temporary test persistence file
	 */
	@Test
	public void testNoFileInstantiation() throws IOException {
		// delete the temporary file created in setup()
		File tempFile = new File(tempFilePath);
		tempFile.delete();
		assertFalse(tempFile.exists());

		// instantiate a file persistence using the path from the deleted file
		// this should create a new file in its place
		new FilePersistence(tempFile.getCanonicalPath());
		assertTrue(tempFile.exists());
	}

	/**
	 * Verifies the correctness of {@link FilePersistence#containsKey(String)} and
	 * {@link FilePersistence#get(String)} before and after an insertion.
	 */
	@Test
	public void testGet() {
		// verify that the non-existent key is reported as such
		assertFalse(persistence.containsKey("a"));
		assertNull(persistence.get("a"));

		// add the entry
		persistence.put("a", "alpha 1");

		// verify that the proper existence and value are reported
		assertTrue(persistence.containsKey("a"));
		assertEquals("alpha 1", persistence.get("a"));
	}

	/**
	 * Checks whether the persistent reacts properly to a missing persistence file
	 * when performing a get operation (i.e. log a warning).
	 */
	@Test
	public void testGetMissingFile() {
		File tempFile = new File(tempFilePath);
		tempFile.delete();

		assertNull(persistence.get("foo"));
		assertLastLogEquals(Level.WARN, "Persistence file could not be found");
	}
	
	/**
	 * Checks the correctness of {@link FilePersistence#getAll()}.
	 */
	@Test
	public void testGetAll() {
		// set up initial key-value entries
		persistence.put("a", "alpha 1");
		persistence.put("b", "bravo 2");
		persistence.put("c", "charlie 3");

		// verify that entry insertion was successful
		assertTrue(persistence.containsKey("a"));
		assertTrue(persistence.containsKey("b"));
		assertTrue(persistence.containsKey("c"));

		// retrieve the entry set
		Map<String, String> entries = persistence.getAll();

		// verify that all keys have been removed
		assertEquals(3, entries.size());
		// verify that keys were loaded correctly
		assertEquals("alpha 1", entries.get("a"));
		assertEquals("bravo 2", entries.get("b"));
		assertEquals("charlie 3", entries.get("c"));
	}
	
	/**
	 * Checks whether the persistent reacts properly to a missing persistence file
	 * when performing a batch get operation (i.e. log a warning).
	 */
	@Test
	public void testGetAllMissingFile() {
		File tempFile = new File(tempFilePath);
		tempFile.delete();

		assertNull(persistence.getAll());
		assertLastLogEquals(Level.WARN, "Persistence file could not be found");
	}
	
	/**
	 * Checks whether the persistent reacts properly to a missing persistence file
	 * when performing a containsKey operation (i.e. log a warning).
	 */
	@Test
	public void testContainsKeyMissingFile() {
		File tempFile = new File(tempFilePath);
		tempFile.delete();

		assertFalse(persistence.containsKey("foo"));
		assertLastLogEquals(Level.WARN, "Persistence file could not be found");
	}

	/**
	 * Checks the correctness of {@link FilePersistence#put(String, String)}
	 * when used for deletion.
	 */
	@Test
	public void testDelete() {
		// set up initial key-value entries
		persistence.put("a", "alpha 1");
		persistence.put("b", "bravo 2");
		persistence.put("c", "charlie 3");

		// verify that entry insertion was successful
		assertTrue(persistence.containsKey("a"));
		assertTrue(persistence.containsKey("b"));
		assertTrue(persistence.containsKey("c"));

		// request deletion of the middle entry
		persistence.put("b", null);

		// verify that 'b' was deleted successfully and other entries were unaffected
		assertTrue(persistence.containsKey("a"));
		assertFalse(persistence.containsKey("b"));
		assertTrue(persistence.containsKey("c"));
	}

	/**
	 * Checks the correctness of {@link FilePersistence#put(String, String)}
	 * when used to update key-value entries.
	 */
	@Test
	public void testUpdate() {
		// set up initial key-value entries
		persistence.put("a", "alpha");
		persistence.put("b", "bravo 2");
		persistence.put("c", "charlie 3");

		// verify that entry insertion was successful
		assertEquals("alpha", persistence.get("a"));
		assertEquals("bravo 2", persistence.get("b"));
		assertEquals("charlie 3", persistence.get("c"));

		// request modification of the first entry
		persistence.put("a", "alpha 1");

		// verify that 'a' was modified correctly and other entries were unaffected
		assertEquals("alpha 1", persistence.get("a"));
		assertEquals("bravo 2", persistence.get("b"));
		assertEquals("charlie 3", persistence.get("c"));
	}
	
	/**
	 * Checks whether the persistent reacts properly to a missing persistence file
	 * when performing a put operation.
	 */
	@Test
	public void testPutMissingFile() {
		File tempFile = new File(tempFilePath);
		tempFile.delete();

		assertNull(persistence.put("foo", "bar"));
		assertTrue(tempFile.exists());
	}
	
	/**
	 * Checks the correctness of {@link FilePersistence#insertAll(Map)}.
	 */
	@Test
	public void testInsertAll() {
		// verify that keys are initially unset
		assertFalse(persistence.containsKey("a"));
		assertFalse(persistence.containsKey("b"));
		assertFalse(persistence.containsKey("c"));
		
		// create argument map
		Map<String, String> entries = new HashMap<>();
		entries.put("a", "alpha 1");
		entries.put("b", "bravo 2");
		entries.put("c", "charlie 3");
		
		// invoke method under test
		persistence.insertAll(entries);
		
		// verify that keys were loaded correctly
		assertEquals("alpha 1", persistence.get("a"));
		assertEquals("bravo 2", persistence.get("b"));
		assertEquals("charlie 3", persistence.get("c"));
	}
	
	/**
	 * Checks the correctness of {@link FilePersistence#clear()}.
	 */
	@Test
	public void testClear() {
		// set up initial key-value entries
		persistence.put("a", "alpha 1");
		persistence.put("b", "bravo 2");
		persistence.put("c", "charlie 3");

		// verify that entry insertion was successful
		assertTrue(persistence.containsKey("a"));
		assertTrue(persistence.containsKey("b"));
		assertTrue(persistence.containsKey("c"));

		// request deletion of the middle entry
		persistence.clear();

		// verify that all keys have been removed
		assertFalse(persistence.containsKey("a"));
		assertFalse(persistence.containsKey("b"));
		assertFalse(persistence.containsKey("c"));
	}

}
