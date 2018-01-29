package logger;

import java.io.IOException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Helper class providing basic appender configuration for logging with Log4J.
 */
public class LogSetup {

	public static final String FILE_PATTERN = "[%d{ISO8601}][%-5p][%t][%c] %m%n";

	/**
	 * Defeats instantiation.
	 */
	private LogSetup() {}

	public static void initialize(String logdir, Level level) throws IOException {
		initialize(logdir, level, FILE_PATTERN);
	}

	/**
	 * Configures the root logger, adding a file and console appender.
	 * 
	 * @param logdir the destination (i.e. directory + filename) for the persistent
	 *            logging information.
	 * @throws IOException if the log destination could not be found.
	 */
	public static void initialize(String logdir, Level level, String consolePattern) throws IOException {
		PatternLayout fileLayout = new PatternLayout(FILE_PATTERN);
		FileAppender fileAppender = new FileAppender(fileLayout, logdir, true);

		PatternLayout consoleLayout = new PatternLayout(consolePattern);
		ConsoleAppender consoleAppender = new ConsoleAppender(consoleLayout);

		Logger rootLogger = Logger.getRootLogger();
		rootLogger.addAppender(consoleAppender);
		rootLogger.addAppender(fileAppender);
		rootLogger.setLevel(level);
	}

}
