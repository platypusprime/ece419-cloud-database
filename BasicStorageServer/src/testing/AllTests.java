package testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import testing.app_kvServer.cache.FifoCacheTest;
import testing.app_kvServer.cache.LfuCacheTest;
import testing.app_kvServer.cache.LruCacheTest;

/**
 * An aggregate test suite that runs all tests for the storage server project.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
		ConnectionTest.class,
		InteractionTest.class,
		IllegalArgumentTest.class,
		KVClientTest.class,
		FifoCacheTest.class,
		LfuCacheTest.class,
		LruCacheTest.class
})
public class AllTests {}
