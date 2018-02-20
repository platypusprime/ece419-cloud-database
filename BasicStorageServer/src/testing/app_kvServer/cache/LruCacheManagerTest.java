package testing.app_kvServer.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.cache.FifoCacheManager;
import app_kvServer.cache.KVCacheManager;
import app_kvServer.cache.LruCacheManager;

/**
 * Tests various cache behaviors using the LRU cache strategy implementation.
 */
public class LruCacheManagerTest {

	private KVCacheManager cache;

	/**
	 * Sets up the cache manager with a size of 3 before each test.
	 */
	@Before
	public void setup() {
		cache = new LruCacheManager();
		cache.setCacheSize(3);
	}

	/**
	 * Checks that the correct cache strategy is being returned by
	 * {@link LruCacheManager#getCacheStrategy() getCacheStrategy()}.
	 */
	@Test
	public void testGetCacheStrategy() {
		assertEquals(CacheStrategy.LRU, cache.getCacheStrategy());
	}

	/**
	 * Checks that the correct cache capacity is being returned by
	 * {@link LruCacheManager#getCacheSize() getCacheSize()}.
	 */
	@Test
	public void testGetCacheSize() {
		assertEquals(3, cache.getCacheSize());
	}

	/**
	 * Checks for proper LRU cache eviction behavior.
	 */
	@Test
	public void testCacheEviction() {
		cache.put("foo1", "bar1");
		cache.put("foo2", "bar2");
		cache.put("foo3", "bar3");
		cache.put("foo1", "bar4"); // should update usage for foo1
		cache.put("foo4", "bar5");

		assertTrue(cache.containsKey("foo1"));
		assertFalse(cache.containsKey("foo2"));
		assertTrue(cache.containsKey("foo3"));
		assertTrue(cache.containsKey("foo4"));

		String value = cache.get("foo2");
		assertNull(value);
		cache.put("foo2", "bar2"); // simulates update from persistence

		assertFalse(cache.containsKey("foo3"));
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
		cache.put("foo2", "bar2");
		cache.put("foo3", "bar3");
		cache.put("foo4", "bar4");
		cache.put("foo5", "bar5");

		// check that 4 pairs remain
		assertFalse(cache.containsKey("foo1"));
		assertTrue(cache.containsKey("foo2"));
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
		cache.put("foo2", "bar2");
		cache.put("foo3", "bar3");

		// confirm that they exist in the cache
		assertTrue(cache.containsKey("foo1"));
		assertTrue(cache.containsKey("foo2"));
		assertTrue(cache.containsKey("foo3"));

		// shrink cache size from 3 to 1
		cache.setCacheSize(1);

		// confirm that the first two pairs are evicted
		assertFalse(cache.containsKey("foo1"));
		assertFalse(cache.containsKey("foo2"));
		assertTrue(cache.containsKey("foo3"));

		// confirm that the capacity remains as 1 by inserting another pair
		cache.put("foo4", "bar4");
		assertFalse(cache.containsKey("foo3"));
		assertTrue(cache.containsKey("foo4"));

	}

	/**
	 * Checks that the {@link FifoCacheManager#getCacheSize() setCacheSize()} method
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
