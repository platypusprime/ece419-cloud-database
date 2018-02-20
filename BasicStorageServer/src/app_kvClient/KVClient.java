package app_kvClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import client.KVCommInterface;
import client.KVStore;
import common.messages.KVMessage;
import logger.LogSetup;

/**
 * Implements the command-line interface for the KV client application. Parses
 * user input from standard input to form commands, which are delegated to an
 * underlying {@link KVCommInterface}, which provides a level of abstraction
 * between this class and the KV server. Prompts and messages are printed to the
 * console using a Log4J console appender. The following commands are supported:
 * 
 * <ul>
 * <li><b><code>connect &lt;address&gt; &lt;port&gt;</code></b> : Tries to
 * establish a TCP connection to the storage server based on the given hostname
 * and port number</li>
 * <li><b><code>disconnect</code></b> : Tries to disconnect from the connected
 * server</li>
 * <li><b><code>put &lt;key&gt; &lt;value&gt;</code></b> : <i>Inserts</i> a
 * key-value pair into the storage server data structures. <i>Updates</i>
 * (overwrites) the current value with the given value if the server already
 * contains the specified key. <i>Deletes</i> the entry for the given key if
 * <code>&lt;value&gt;</code> equals null</li>
 * <li><b><code>get &lt;key&gt;</code></b> : Retrieves the value for the given
 * key from the storage server</li>
 * <li><b><code>logLevel &lt;level&gt;</code></b> : Sets the logger to the
 * specified log level</li>
 * <li><b><code>help</code></b> : Displays the help message</li>
 * </ul>
 */
public class KVClient implements IKVClient {

	private static final Logger log = Logger.getLogger(KVClient.class);

	private static final String PROMPT = "KVClient> ";
	private static final String CONSOLE_PATTERN = PROMPT + "%m%n";

	private KVCommInterface commModule = null;

	/**
	 * Entry point for the KV client application.
	 * 
	 * @param args Does not expect any command-line args
	 */
	public static void main(String[] args) {

		// set up logging
		try {
			LogSetup.initialize("logs/client.log", Level.INFO, CONSOLE_PATTERN);
		} catch (IOException e) {
			System.out.println("ERROR: unable to initialize logger");
			e.printStackTrace();
			System.exit(1);
		}

		// initialize client
		KVClient clientApplication = new KVClient();
		clientApplication.runInterface();
	}

	@Override
	public void newConnection(String hostname, int port) throws Exception {
		log.info("Establishing connection to server at " + hostname + ":" + port);
		this.commModule = new KVStore(hostname, port);
		this.commModule.connect();
	}

	@Override
	public KVCommInterface getStore() {
		return commModule;
	}

	/**
	 * Explicitly sets this object's client-side communications module for testing
	 * purposes.
	 * 
	 * @param commModule The KV store to use
	 */
	public void setStore(KVCommInterface commModule) {
		this.commModule = commModule;
	}

	/**
	 * Starts the command line interface for the KV client application.
	 */
	public void runInterface() {
		try (BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in))) {
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

		log.info("KVClient shutting down");
	}

	/**
	 * Tokenizes and interprets user input, delegating the server communication
	 * logic to the communications module.
	 * 
	 * @param ln The command to process
	 * @return <code>true</code> if no further commands should be processed,
	 *         <code>false</code> otherwise
	 */
	public boolean handleCommand(String ln) {
		if (ln == null || ln.isEmpty()) {
			handleInvalidCommand();
			return false;
		}

		String[] tokens = ln.trim().split("\\s+", 3);

		if (tokens.length < 1) {
			handleInvalidCommand();

		} else if (tokens[0].equals("connect")) {
			handleConnect(tokens);

		} else if (tokens[0].equals("disconnect")) {
			disconnect();

		} else if (tokens[0].equals("put")) {
			handlePut(tokens);

		} else if (tokens[0].equals("get")) {
			handleGet(tokens);

		} else if (tokens[0].equals("logLevel")) {
			handleLogLevel(tokens);

		} else if (tokens[0].equals("help")) {
			printHelp();

		} else if (tokens[0].equals("quit")) {
			disconnect();
			return true;

		} else {
			handleInvalidCommand();
		}

		return false;
	}

	/**
	 * Processes a <code>connect</code> command from the CLI.
	 * 
	 * @param tokens The tokenized command
	 */
	private void handleConnect(String[] tokens) {
		if (commModule != null) {
			// TODO handle unexpected disconnects
			log.error("Already connected to server; disconnect first");
			return;
		}

		try {
			String hostname = tokens[1];
			int port = Integer.parseInt(tokens[2]);
			newConnection(hostname, port);

		} catch (ArrayIndexOutOfBoundsException e) {
			log.error("Insufficient arguments for connect command");
		} catch (NumberFormatException e) {
			log.error("Could not parse port number as an integer: " + tokens[2]);
		} catch (Exception e) {
			log.error("Could not establish connection to server", e);
		}		
	}

	/**
	 * Processes a <code>put</code> command from the CLI.
	 * 
	 * @param tokens The tokenized command
	 */
	private void handlePut(String[] tokens) {
		if (tokens.length == 2) {
			sendPut(tokens[1], null);
		} else if (tokens.length >= 3) {
			sendPut(tokens[1], tokens[2]);
		} else if (tokens.length < 2) {
			log.error("Insufficient arguments for put command");
		}
	}

	/**
	 * Processes a <code>get</code> command from the CLI.
	 * 
	 * @param tokens The tokenized command
	 */
	private void handleGet(String[] tokens) {
		if (tokens.length >= 2) {
			sendGet(tokens[1]);
		} else {
			log.error("Insufficient arguments for get command");
		}
	}

	/**
	 * Processes a <code>logLevel</code> command from the CLI.
	 * 
	 * @param tokens The tokenized command
	 */
	private void handleLogLevel(String[] tokens) {
		if (tokens.length >= 2) {
			setLogLevel(tokens[1]);
		} else {
			log.error("Insufficient arguments for logLevel command");
		}
	}

	/**
	 * Processes an unknown or empty command from the CLI.
	 */
	private void handleInvalidCommand() {
		log.warn("Unknown command;");
		printHelp();
	}

	/**
	 * Disconnects this application from the KV server. If no such connection
	 * exists, does nothing.
	 */
	private void disconnect() {
		if (commModule != null) {
			commModule.disconnect();
			commModule = null;
			
			log.info("Disconnect successful");

		} else {
			log.warn("No current server connection; skipping disconnect");
		}
	}

	/**
	 * Sends a PUT request to the KV server with the specified key and value. Prints
	 * the result of the operation to the CLI.
	 * 
	 * @param key The key to set
	 * @param value The value to assign
	 */
	private void sendPut(String key, String value) {
		if (commModule == null) {
			log.error("FAILURE - not connected to server; cannot execute put");
			return;
		}

		try {
			KVMessage response = commModule.put(key, value);

			switch (response.getStatus()) {
			case PUT_SUCCESS:
			case PUT_UPDATE:
			case DELETE_SUCCESS:
				log.info("SUCCESS - put successful");
				break;
			case PUT_ERROR:
			case DELETE_ERROR:
				log.info("FAILURE - put unsuccessful");
				break;
			default:
				log.warn("FAILURE - unexpected response: " + response.getStatus());
				break;
			}

		} catch (Exception e) {
			log.error("Exception encountered while executing put", e);
		}
	}

	/**
	 * Sends a GET request to the KV server with the specified key. Prints the
	 * result of the operation to the CLI.
	 * 
	 * @param key The key to look up
	 */
	private void sendGet(String key) {
		if (commModule == null) {
			log.error("FAILURE - not connected to server; cannot execute get");
			return;
		}

		try {
			KVMessage response = commModule.get(key);

			switch (response.getStatus()) {
			case GET_SUCCESS:
				log.info("SUCCESS - get result is " + response.getKey() + ":" + response.getValue());
				break;
			case GET_ERROR:
				log.info("FAILURE - could not get value for " + response.getKey());
				break;
			default:
				log.warn("FAILURE - unexpected response: " + response.getStatus());
				break;
			}

		} catch (Exception e) {
			log.error("Exception encountered while executing get", e);
		}
	}

	/**
	 * Sets the log level of the client application.
	 * 
	 * @param levelStr A Log4J log level such as ALL, DEBUG, INFO, WARN, ERROR,
	 *            FATAL, or OFF
	 */
	private void setLogLevel(String levelStr) {
		Level level = Level.toLevel(levelStr);
		if (level == Level.DEBUG && !levelStr.equals(Level.DEBUG.toString())) {
			log.error("Invalid log level: " + levelStr);
		} else {
			Logger.getRootLogger().setLevel(level);
			log.info("Set log level: " + level.toString());
		}
	}

	/**
	 * Prints the help message for the CLI.
	 */
	private void printHelp() {
		log.info("");
		log.info("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
		log.info("KV CLIENT HELP (Usage):");
		log.info("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::::");
		log.info("connect <address> <port>");
		log.info("\tTries to establish a TCP-connection to the storage server based");
		log.info("\ton the given server address and the port number of the storage service");
		log.info("disconnect");
		log.info("\tTries to disconnect from the connected server");
		log.info("put <key> <value>");
		log.info("\tInserts a key-value pair into the storage server data structures");
		log.info("\tUpdates (overwrites) the current value with the given value if "
				+ "the server already contains the specified key");
		log.info("\tDeletes the entry for the given key if <value> is empty");
		log.info("get <key>");
		log.info("\tRetrieves the value for the given key from the storage server");
		log.info("logLevel");
		log.info("\tSets the logger to the specified log level");
		log.info("help");
		log.info("\tShows this message");
		log.info("quit");
		log.info("\tTears down the active connections to the server and exits the program");
		log.info("");
	}

}
