package testing.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.function.Predicate;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;

/**
 * Provides a means for making assertions based on logged messages during
 * unit testing.
 */
public abstract class LogInstrumentingTest {

	private TrackingAppender logTracker;
	private Level origLevel;

	/**
	 * Returns the class whose logger should be tracked.
	 * 
	 * @return The class under test
	 */
	public abstract Class<?> getClassUnderTest();
	
	/**
	 * Instantiates the client object before each test.
	 */
	@Before
	public void instrumentLogger() {
		origLevel = Logger.getRootLogger().getLevel(); // store original log level
		Logger.getRootLogger().setLevel(Level.ALL); // allow all logs

		logTracker = new TrackingAppender();
		Logger.getLogger(getClassUnderTest()).addAppender(logTracker);
	}

	/**
	 * Resets changes to global state made by these tests.
	 */
	@After
	public void removeInstrumentation() {
		Logger.getLogger(getClassUnderTest()).removeAppender(logTracker);
		Logger.getRootLogger().setLevel(origLevel); // restore original log level
	}

	/**
	 * Checks if the last log matches the expected level and message.
	 * 
	 * @param level The expected log level
	 * @param msg The expected log message
	 */
	public void assertLastLogEquals(Level level, String msg) {
		LoggingEvent lastLog = logTracker.getLastEvent();
		assertEquals(msg, lastLog.getMessage());
		assertEquals(level, lastLog.getLevel());
	}

	/**
	 * Checks whether all logs match the specified matching rule.
	 * 
	 * @param matcher The rule to check
	 */
	public void assertAllLogsMatch(Predicate<? super LoggingEvent> matcher) {
		boolean allMatch = logTracker.getEventLog().stream()
				.allMatch(matcher);
		assertTrue(allMatch);
	}

	/**
	 * Checks whether the specified log has been recorded by this tracker.
	 * 
	 * @param level The log level of the event
	 * @param query The message to check for
	 */
	public void assertLogExists(Level level, String query) {
		boolean messageLogged = logTracker.getEventLog().stream()
				.filter(log -> log.getLevel().equals(level))
				.map(LoggingEvent::getMessage)
				.anyMatch(msg -> msg.equals(query));
		assertTrue(messageLogged);
	}

	/**
	 * Clears recorded log messages from the underlying log tracker.
	 * 
	 * @see TrackingAppender#clear()
	 */
	public void clearTracker() {
		logTracker.clear();
	}

}
