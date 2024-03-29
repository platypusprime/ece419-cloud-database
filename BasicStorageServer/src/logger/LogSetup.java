package logger;

import java.io.IOException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Helper class providing basic logger and appender configuration for Log4J.
 */
public class LogSetup {

	/** The pattern string to set for the file appender. */
	public static final String FILE_PATTERN = "[%d{ISO8601}][%-5p][%t - %c] %m%n";

	/**
	 * Defeats instantiation.
	 */
	private LogSetup() {}

	/**
	 * Configures the root logger with the specified log directory for the file
	 * appender and log level. Provided for compatibility with the starter kit.
	 * 
	 * @param logdir The destination (i.e. directory + filename) for the persistent
	 *            logging information
	 * @param level The logging level to set for the root logger
	 * @throws IOException
	 */
	public static void initialize(String logdir, Level level) throws IOException {
		initialize(logdir, level, FILE_PATTERN);
	}

	/**
	 * Configures the root logger with the specified log directory for the file
	 * appender, log level, and pattern for the console appender.
	 * 
	 * @param logdir The destination (i.e. directory + filename) for the persistent
	 *            logging information
	 * @param level The logging level to set for the root logger
	 * @param consolePattern The pattern string to set for the console appender
	 * @throws IOException if the log destination could not be found.
	 */
	public static void initialize(String logdir, Level level, String consolePattern) throws IOException {
		PatternLayout fileLayout = new PatternLayout(FILE_PATTERN);
		FileAppender fileAppender = new FileAppender(fileLayout, logdir, true);
		fileAppender.setName("file");

		PatternLayout consoleLayout = new PatternLayout(consolePattern);
		ConsoleAppender consoleAppender = new ConsoleAppender(consoleLayout);
//		consoleAppender.setThreshold(Level.INFO);
		consoleAppender.setName("stdout");

		Logger rootLogger = Logger.getRootLogger();
		rootLogger.removeAppender("stdout");
		rootLogger.addAppender(consoleAppender);
		rootLogger.removeAppender("file");
		rootLogger.addAppender(fileAppender);
		rootLogger.setLevel(level);
		
		// suppress logs from ZooKeeper library
		Logger zkLogger = Logger.getLogger("org.apache.zookeeper");
		zkLogger.setLevel(Level.WARN);
	}

	/**
	 * Removes all appenders from the root logger and sets its log level to
	 * <code>ERROR</code>.
	 */
	public static void teardown() {
		Logger rootLogger = Logger.getRootLogger();
		rootLogger.removeAllAppenders();
		rootLogger.setLevel(Level.ERROR);
	}

}
