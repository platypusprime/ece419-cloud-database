package testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import testing.app_kvServer.cache.FifoCacheManagerTest;
import testing.app_kvServer.cache.LfuCacheManagerTest;
import testing.app_kvServer.cache.LruCacheManagerTest;

/**
 * An aggregate test suite that runs all tests for the storage server project.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
		ConnectionTest.class,
		InteractionTest.class,
		IllegalArgumentTest.class,
		FifoCacheManagerTest.class,
		LfuCacheManagerTest.class,
		LruCacheManagerTest.class
})
public class AllTests {}
