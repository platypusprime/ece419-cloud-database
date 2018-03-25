package app_kvECS;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;

import org.apache.log4j.Logger;

/**
 * Provides a command-line interface for the K/V service's external
 * configuration service component.
 */
public class ECSAdminConsole implements Runnable {

	private static final Logger log = Logger.getLogger(ECSAdminConsole.class);

	/** The command-line prompt, prepended to all lines. */
	public static final String PROMPT = "kvECS> ";

	/** The Log4J appender pattern for the console appender. */
	public static final String CONSOLE_PATTERN = PROMPT + "%m%n";

	/** The ECS client which performs all ECS functionality. */
	private final ECSClient ecsClient;

	/**
	 * Creates a command-line wrapper for the given ECS client.
	 * 
	 * @param ecsClient The client to wrap
	 */
	public ECSAdminConsole(ECSClient ecsClient) {
		this.ecsClient = ecsClient;
	}

	@Override
	public void run() {
		try (BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in))) {
			log.info("Welcome to kvECS application");
			boolean stop = false;
			while (!stop) {
				System.out.print(PROMPT);

				try {
					String ln = stdin.readLine();
					stop = handleCommand(ln);

				} catch (IOException e) {
					stop = true;
					log.warn("IO exception while reading from standard input", e);
				}
			}

		} catch (IOException e) {
			log.fatal("IO exception while closing standard input reader", e);
		}

		log.info("KVECS shutting down");
		ecsClient.shutdown();

	}

	/**
	 * Interprets a single command from the admin CLI.
	 * 
	 * @param ln The command to process
	 * @return <code>true</code> if execution should stop after the current command,
	 *         <code>false</code> otherwise
	 */
	public boolean handleCommand(String ln) {
		if (ln == null || ln.isEmpty()) {
			log.warn("Please input a command;");
			return false;
		}

		String[] tokens = ln.trim().split("\\s+");

		if (tokens.length < 1) {
			log.warn("Empty command");
			printHelp();

		} else if (tokens[0].equals("start")) {
			ecsClient.start();

		} else if (tokens[0].equals("stop")) {
			ecsClient.stop();

		} else if (tokens[0].equals("shutdown")) {
			return true;

		} else if (tokens[0].equals("add")) {
			if (tokens.length == 3) {
				ecsClient.addNode(tokens[1], Integer.parseInt(tokens[2]));
			} else if (tokens.length == 4) {
				ecsClient.addNodes(Integer.parseInt(tokens[1]), tokens[2], Integer.parseInt(tokens[3]));
			} else {
				log.error("Invalid number of arguments (usage: add <count>(optional) <cacheStrategy> <cacheSize>)");
			}

		} else if (tokens[0].equals("remove")) {
			if (tokens.length > 1) {
				String[] names = Arrays.copyOfRange(tokens, 1, tokens.length);
				ecsClient.removeNodes(Arrays.asList(names));
			} else {
				log.error("Invalid number of arguments (usage: remove <serverName1> <serverName2> ...)");
			}

		} else if (tokens[0].equals("help")) {
			printHelp();

		} else {
			log.warn("Unknown command");
			printHelp();
		}

		return false;
	}

	/**
	 * Prints the help message for the CLI.
	 */
	private void printHelp() {
		log.info("");
		log.info("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
		log.info("KV ECS HELP (Usage):");
		log.info("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
		log.info("");
		log.info("Procedure: add -> start -> add/remove/stop/shutdown");
		log.info("");
		log.info("add <count>(optional) <cacheStrategy> <cacheSize>");
		log.info("\t\tStarts up <count> servers (or 1, if <count> is not specified)");
		log.info("");
		log.info("remove <serverName1> <serverName2> ...");
		log.info("\t\tRemoves nodes with given names");
		log.info("");
		log.info("start");
		log.info("\t\tStarts all storage servers, opening them for client requests");
		log.info("");
		log.info("stop");
		log.info("\t\tStops all storage servers, but does not end their processes");
		log.info("");
		log.info("shutdown");
		log.info("\t\tShuts down the storage service, ending all processes and quitting the application");
		log.info("");
		log.info("help");
		log.info("\t\tShows this message");
		log.info("");
	}

}
