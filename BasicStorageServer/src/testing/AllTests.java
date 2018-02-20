package testing;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * An aggregate test suite that runs all tests for the storage server project.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
		ConnectionTest.class,
		InteractionTest.class,
		CacheTest.class,
		IllegalArgumentTest.class
})
public class AllTests {}
