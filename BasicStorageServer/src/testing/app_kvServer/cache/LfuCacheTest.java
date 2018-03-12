package testing.app_kvServer.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.cache.FifoCache;
import app_kvServer.cache.KVCache;
import app_kvServer.cache.LfuCache;

/**
 * Tests various cache behaviors using the LFU cache strategy implementation.
 */
public class LfuCacheTest {

	private KVCache cache;

	/**
	 * Sets up the LFU cache with a size of 3 before each test.
	 */
	@Before
	public void setup() {
		cache = new LfuCache();
		cache.setCacheSize(3);
	}

	/**
	 * Checks that the correct cache strategy is being returned by
	 * {@link LfuCache#getCacheStrategy() getCacheStrategy()}.
	 */
	@Test
	public void testGetCacheStrategy() {
		assertEquals(CacheStrategy.LFU, cache.getCacheStrategy());
	}

	/**
	 * Checks that the correct cache capacity is being returned by
	 * {@link LfuCache#getCacheSize() getCacheSize()}.
	 */
	@Test
	public void testGetCacheSize() {
		assertEquals(3, cache.getCacheSize());
	}

	/**
	 * Checks for proper LRU cache eviction behavior. Inserts 3 KV pairs into a
	 * FIFO cache of size 3 and performs varying numbers of reads on those pairs.
	 * Adds a fourth pair and expects the least-read pair to be evicted.
	 */
	@Test
	public void testCacheEviction() {
		// store 3 values into the cache (should now be at capacity)
		cache.put("foo1", "bar1");
		cache.put("foo2", "bar2");
		cache.put("foo3", "bar3");

		// check that values were inserted successfully
		assertTrue(cache.containsKey("foo1"));
		assertTrue(cache.containsKey("foo2"));
		assertTrue(cache.containsKey("foo3"));

		// read the values for varying numbers of times
		// @formatter:off
		for (int i = 0; i < 6;  i++) cache.get("foo1");
		for (int i = 0; i < 4;  i++) cache.get("foo2");
		for (int i = 0; i < 10; i++) cache.get("foo3");
		// @formatter:on

		// store new value into cache to trigger eviction
		cache.put("foo4", "bar4");

		// check that the correct key was evicted
		assertTrue(cache.containsKey("foo1"));
		assertFalse(cache.containsKey("foo2"));
		assertTrue(cache.containsKey("foo3"));
		assertTrue(cache.containsKey("foo4"));
	}

	/**
	 * Checks that increasing the size of the cache correctly allows more entries
	 * into the cache.
	 */
	@Test
	public void testCacheExpand() {
		// expand the cache to size 4
		cache.setCacheSize(4);

		// insert 5 pairs into the cache
		cache.put("foo1", "bar1");
		cache.get("foo1");
		cache.put("foo2", "bar2");
		cache.put("foo3", "bar3");
		cache.get("foo3");
		cache.put("foo4", "bar4");
		cache.get("foo4");
		cache.put("foo5", "bar5");

		// check that 4 pairs remain
		assertTrue(cache.containsKey("foo1"));
		assertFalse(cache.containsKey("foo2"));
		assertTrue(cache.containsKey("foo3"));
		assertTrue(cache.containsKey("foo4"));
		assertTrue(cache.containsKey("foo5"));
	}

	/**
	 * Checks that decreasing the size of the cache correctly evicts extra entries
	 * from the cache.
	 */
	@Test
	public void testCacheShrink() {
		// insert 3 pairs into the cache
		cache.put("foo1", "bar1");
		cache.get("foo1");
		cache.put("foo2", "bar2");
		cache.get("foo2");
		cache.get("foo2");
		cache.put("foo3", "bar3");

		// confirm that they exist in the cache
		assertTrue(cache.containsKey("foo1"));
		assertTrue(cache.containsKey("foo2"));
		assertTrue(cache.containsKey("foo3"));

		// shrink cache size from 3 to 1
		cache.setCacheSize(1);

		// confirm that two pairs are evicted
		assertFalse(cache.containsKey("foo1"));
		assertTrue(cache.containsKey("foo2"));
		assertFalse(cache.containsKey("foo3"));

		// confirm that the capacity remains as 1 by inserting another pair
		cache.put("foo4", "bar4");
		assertFalse(cache.containsKey("foo3"));
		assertTrue(cache.containsKey("foo4"));

	}

	/**
	 * Checks that the {@link FifoCache#getCacheSize() setCacheSize()} method
	 * correctly rejects negative values.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void testSetCacheSizeNegative() {
		cache.setCacheSize(-394);
	}

	/**
	 * Checks that providing a null value to put triggers a deletion.
	 */
	@Test
	public void testDeletion() {
		cache.put("foo", "bar");
		assertTrue(cache.containsKey("foo"));
		cache.put("foo", null);
		assertFalse(cache.containsKey("foo"));
	}

	/**
	 * Checks cache clearing.
	 */
	@Test
	public void testClear() {
		// insert 3 pairs into the cache
		cache.put("foo1", "bar1");
		cache.put("foo2", "bar2");
		cache.put("foo3", "bar3");

		// confirm that they exist in the cache
		assertTrue(cache.containsKey("foo1"));
		assertTrue(cache.containsKey("foo2"));
		assertTrue(cache.containsKey("foo3"));

		// clear the cache
		cache.clear();

		// confirm that they have all been removed
		assertFalse(cache.containsKey("foo1"));
		assertFalse(cache.containsKey("foo2"));
		assertFalse(cache.containsKey("foo3"));
	}

}
