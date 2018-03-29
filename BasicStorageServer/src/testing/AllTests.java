package testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import testing.app_kvServer.cache.FifoCacheTest;
import testing.app_kvServer.cache.LfuCacheTest;
import testing.app_kvServer.cache.LruCacheTest;
import testing.app_kvServer.persistence.FilePersistenceTest;
import testing.common.messages.StreamUtilTest;
import testing.common.zookeeper.ZKWrapperTest;
import testing.ecs.ECSNodeTest;
import testing.ecs.ECSClientTest;

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
		LruCacheTest.class,
		FilePersistenceTest.class,
		StreamUtilTest.class,
		ECSNodeTest.class,
		ZKWrapperTest.class,
		ECSClientTest.class,
		ReplicationTest.class
})
public class AllTests {}
