package testing.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;

/**
 * A utility class for tracking log messages in testing.
 */
public class TrackingAppender extends AppenderSkeleton {

	private final List<LoggingEvent> eventLog = new ArrayList<LoggingEvent>();

	@Override
	public boolean requiresLayout() {
		return false;
	}

	@Override
	protected void append(LoggingEvent event) {
		eventLog.add(event);
	}

	/**
	 * Returns a list containing the logging events recorded by this appender.
	 * 
	 * @return The event log
	 */
	public List<LoggingEvent> getEventLog() {
		return new ArrayList<LoggingEvent>(eventLog);
	}

	/**
	 * Returns the most recent event recorded by this appender.
	 * 
	 * @return The last logging event in the event log
	 *         or <code>null</code> if the log is empty
	 */
	public LoggingEvent getLastEvent() {
		if (!eventLog.isEmpty())
			return eventLog.get(eventLog.size() - 1);
		else
			return null;
	}

	/**
	 * Clears the event log.
	 */
	public void clear() {
		eventLog.clear();
	}

	@Override
	public void close() {}

}
