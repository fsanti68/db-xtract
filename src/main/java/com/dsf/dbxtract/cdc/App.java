/**
 * Copyright 2016 Fabio De Santi
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dsf.dbxtract.cdc;

import java.security.InvalidParameterException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.dsf.dbxtract.cdc.journal.JournalExecutor;
import com.dsf.dbxtract.cdc.journal.JournalHandler;
import com.dsf.dbxtract.cdc.mon.Monitor;

/**
 * Main application
 * 
 * @author fabio de santi
 * @version 0.2
 */
public class App {

	private static Logger logger = LogManager.getLogger(App.class.getName());

	public static final String BASEPREFIX = "/dbxtract/cdc";

	private Config config = null;
	private ScheduledExecutorService scheduledService = null;

	/**
	 * Constructor
	 * 
	 * @param config
	 *            agent's configuration
	 */
	public App(Config config) {
		this.config = config;
	}

	/**
	 * Gets configuration data
	 * 
	 * @return {@link Config}
	 */
	public Config getConfig() {
		return config;
	}

	/**
	 * Starts scanning services.
	 * 
	 * @throws Exception
	 */
	public void start() throws Exception {

		// Get ZooKeeper's connection string
		String zkConnection = config.getZooKeeper();

		// Get interval (in milliseconds) between executions
		long interval = config.getDataSources().getInterval();

		scheduledService = Executors.newScheduledThreadPool(config.getThreadPoolSize());

		// Prepare the task's list. Each handler becomes a task.
		for (JournalHandler handler : config.getHandlers()) {

			Runnable executor = new JournalExecutor(config.getAgentName(), zkConnection, handler,
					config.getSourceByHandler(handler));
			scheduledService.scheduleAtFixedRate(executor, 0L, interval, TimeUnit.MILLISECONDS);
		}
	}

	/**
	 * Stops all scanning services.
	 */
	public void stop() {
		if (scheduledService != null)
			scheduledService.shutdown();
	}

	/**
	 * Prepare command line options for CLI.
	 * 
	 * @return command line options object
	 */
	private static Options prepareCmdLineOptions() {

		Options options = new Options();
		// required: --config <file>
		options.addOption(Option.builder().longOpt("config").hasArg().numberOfArgs(1).argName("file")
				.desc("configuration file pathname").required().build());
		// optional: --monitor <port>
		options.addOption(Option.builder().longOpt("monitor").hasArg().numberOfArgs(1).argName("port")
				.desc("monitoring port number (default: 9000)").required(false).build());

		// commands:
		OptionGroup commands = new OptionGroup();
		// --source-add <name> <conn> <driver> <user> <password>
		commands.addOption(Option.builder().longOpt("source-add").numberOfArgs(5)
				.argName("name conn driver user passwd").desc("add new datasource").required(false).build());
		// --source-delete <name>
		commands.addOption(Option.builder().longOpt("source-delete").numberOfArgs(1).argName("name")
				.desc("remove datasource").required(false).build());
		// --source-interval <interval ms>
		commands.addOption(Option.builder().longOpt("source-interval").numberOfArgs(1).argName("millisecs")
				.desc("set datasource scan interval").required(false).build());
		// --handler-add <source> <handler>
		commands.addOption(Option.builder().longOpt("handler-add").numberOfArgs(2).argName("source handlerClass")
				.desc("add new handler to an existing datasource (i.e. handler-add myds com.cdc.MyHandler)")
				.required(false).build());
		// --handler-delete <source> <handler>
		commands.addOption(Option.builder().longOpt("handler-delete").numberOfArgs(2).argName("source handlerClass")
				.desc("remove handler from datasource (i.e. handler-delete myds com.cdc.MyHandler)").required(false)
				.build());
		// --list
		commands.addOption(Option.builder().longOpt("list").hasArg(false)
				.desc("list configuration parameters and values").required(false).build());
		// --start
		commands.addOption(
				Option.builder().longOpt("start").hasArg(false).desc("start dbxtract agent").required(false).build());
		options.addOptionGroup(commands);

		return options;
	}

	/**
	 * <p>
	 * Starts the dbxtract app.
	 * </p>
	 * <p>
	 * usage: java -jar dbxtract.jar --config <file><br>
	 * [--handler-add <source handlerClass> |<br>
	 * --handler-delete <source handlerClass> |<br>
	 * --list | --source-add <name conn driver user passwd> |<br>
	 * --source-delete <name> | --source-interval <millisecs> |<br>
	 * --start] [--monitor <port>]
	 * 
	 * <pre>
	 * --config <file>                        configuration file pathname
	 * --handler-add <source handlerClass>    add new handler to an existing
	 *                                        datasource (i.e. handler-add
	 *                                        myds com.cdc.MyHandler)
	 * --handler-delete <source handlerClass> remove handler from datasource
	 *                                        (i.e. handler-delete myds com.cdc.MyHandler)
	 * --list                                 list configuration parameters and values
	 * --monitor <port>                       monitoring port number (default: 9000)
	 * --source-add <name conn driver user passwd> add new datasource --source-delete <name>
	 * remove datasource --source-interval <millisecs> set datasource scan
	 * interval --start start dbxtract agent
	 * </pre>
	 * </p>
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		int monitorPort = 9001;
		String configFilename = null;
		Config config = null;
		try {

			Options options = prepareCmdLineOptions();
			CommandLineParser parser = new DefaultParser();
			try {
				CommandLine cmd = parser.parse(options, args);
				if (cmd.hasOption("config")) {
					configFilename = cmd.getOptionValue("config");
					// configure log4j
					PropertyConfigurator.configure(configFilename);
					// obtain configuration
					config = new Config(configFilename);
				} else {
					throw new Exception("Parameter required: --config");
				}
				if (cmd.hasOption("monitor")) {
					monitorPort = Integer.parseInt(cmd.getOptionValue("monitor"));
				}
				if (cmd.hasOption("source-add")) {
					String[] params = cmd.getOptionValues("source-add");
					if (params == null || params.length < 5)
						throw new InvalidParameterException(
								"Parameters required: <source name> <connection string> <driver class> <username> <password>");
					config.datasourceAdd(params[0], params[1], params[2], params[3], params[4]);

				} else if (cmd.hasOption("source-delete")) {
					String param = cmd.getOptionValue("source-delete");
					config.datasourceDelete(param);

				} else if (cmd.hasOption("source-interval")) {
					String param = cmd.getOptionValue("source-interval");
					config.datasourceInterval(param);

				} else if (cmd.hasOption("handler-add")) {
					String[] params = cmd.getOptionValues("handler-add");
					if (params == null || params.length < 2)
						throw new InvalidParameterException("Parameters required: <sourceName> <handlerClass>");
					config.handlerAdd(params[0], params[1]);

				} else if (cmd.hasOption("handler-delete")) {
					String[] params = cmd.getOptionValues("handler-delete");
					if (params == null || params.length < 2)
						throw new InvalidParameterException("Parameters required: <sourceName> <handlerClass>");
					config.handlerDelete(params[0], params[1]);

				} else if (cmd.hasOption("list")) {
					config.listAll();

				} else if (cmd.hasOption("start")) {
					System.out.println("Welcome to db-xtract");

					// get db-xtract configuration
					config.report();

					// Starts monitor server
					new Monitor(monitorPort, config);

					// Starts service
					App app = new App(config);
					app.start();

				} else {
					throw new ParseException(
							"A command is required: --source-add, --source-delete, --source-interval, --handler-add, --handler-delete, --list or --start");
				}

			} catch (ParseException e1) {
				HelpFormatter formatter = new HelpFormatter();
				System.out.println("\n\n");
				formatter.printHelp("java -jar dbxtract.jar", options, true);

				System.err.println(e1.getMessage());
			}

		} catch (Exception e) {
			logger.fatal("Unable to start dbxtract", e);
		}
	}
}
