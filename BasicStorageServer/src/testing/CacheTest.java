package testing;

import org.junit.Test;

import app_kvServer.IKVServer.CacheStrategy;
import app_kvServer.cache.FifoCacheManager;
import app_kvServer.cache.KVCacheManager;
import app_kvServer.cache.LfuCacheManager;
import app_kvServer.cache.LruCacheManager;
import app_kvServer.persistence.MemoryPersistenceManager;
import junit.framework.TestCase;

public class CacheTest extends TestCase {

	/**
	 * Checks for proper FIFO cache eviction behavior.
	 */
	@Test
	public void testFifoCacheEviction() {
		KVCacheManager cache = new FifoCacheManager();
		assertEquals(CacheStrategy.FIFO, cache.getCacheStrategy());
		cache.setCacheSize(3);
		cache.setPersistenceManager(new MemoryPersistenceManager());

		cache.put("foo1", "bar1");
		cache.put("foo2", "bar2");
		cache.put("foo3", "bar3");
		cache.put("foo1", "bar4");
		cache.put("foo4", "bar5");

		assertTrue(cache.containsKey("foo2"));
		assertTrue(cache.containsKey("foo3"));
		assertTrue(cache.containsKey("foo4"));
		assertFalse(cache.containsKey("foo1"));

		String value = cache.get("foo1");
		assertEquals("bar4", value);

		assertFalse(cache.containsKey("foo2"));
	}

	/**
	 * Checks for proper LRU cache eviction behavior.
	 */
	@Test
	public void testLruCacheEviction() {
		KVCacheManager cache = new LruCacheManager();
		assertEquals(CacheStrategy.LRU, cache.getCacheStrategy());
		cache.setCacheSize(3);
		cache.setPersistenceManager(new MemoryPersistenceManager());

		cache.put("foo1", "bar1");
		cache.put("foo2", "bar2");
		cache.put("foo3", "bar3");
		cache.put("foo1", "bar4");
		cache.put("foo4", "bar5");

		assertFalse(cache.containsKey("foo2"));
		assertTrue(cache.containsKey("foo3"));
		assertTrue(cache.containsKey("foo4"));
		assertTrue(cache.containsKey("foo1"));

		String value = cache.get("foo2");
		assertEquals("bar2", value);

		assertFalse(cache.containsKey("foo3"));
	}

	/**
	 * Checks for proper LFU cache eviction behavior.
	 */
	@Test
	public void testLfuCacheEviction() {
		KVCacheManager cache = new LfuCacheManager();
		assertEquals(CacheStrategy.LFU, cache.getCacheStrategy());
		cache.setCacheSize(3);
		cache.setPersistenceManager(new MemoryPersistenceManager());

		cache.put("foo1", "bar1");
		cache.put("foo2", "bar2");
		cache.put("foo3", "bar3");

		for (int i = 0; i < 6; i++) {
			cache.get("foo1");
		}

		for (int i = 0; i < 4; i++) {
			cache.get("foo2");
		}

		for (int i = 0; i < 10; i++) {
			cache.get("foo3");
		}
		
		cache.put("foo4", "bar4");

		assertTrue(cache.containsKey("foo1"));
		assertFalse(cache.containsKey("foo2"));
		assertTrue(cache.containsKey("foo3"));
		assertTrue(cache.containsKey("foo4"));
	}

}
