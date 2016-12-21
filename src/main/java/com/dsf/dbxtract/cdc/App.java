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

import java.io.IOException;
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

	private static final String PARAM_CONFIG = "config";
	private static final String PARAM_MONITOR = "monitor";

	private static final String COMMAND_LIST = "list";
	private static final String COMMAND_START = "start";

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
	 * @throws ConfigurationException
	 *             any configuration error
	 */
	public void start() throws ConfigurationException {

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
		options.addOption(Option.builder().longOpt(PARAM_CONFIG).hasArg().numberOfArgs(1).argName("file")
				.desc("configuration file pathname").required().build());
		// optional: --monitor <port>
		options.addOption(Option.builder().longOpt(PARAM_MONITOR).hasArg().numberOfArgs(1).argName("port")
				.desc("monitoring port number (default: 9000)").required(false).build());

		// commands:
		OptionGroup commands = new OptionGroup();
		// --list
		commands.addOption(Option.builder().longOpt("list").hasArg(false)
				.desc("list configuration parameters and values").required(false).build());
		// --start
		commands.addOption(
				Option.builder().longOpt("start").hasArg(false).desc("start dbxtract agent").required(false).build());
		options.addOptionGroup(commands);

		return options;
	}

	private static void parseCommand(CommandLine cmd) throws ConfigurationException, IOException, ParseException {

		int monitorPort = 9000;
		String configFilename;
		Config config;

		if (cmd.hasOption(PARAM_CONFIG)) {
			configFilename = cmd.getOptionValue(PARAM_CONFIG);
			// configure log4j
			PropertyConfigurator.configure(configFilename);
			// obtain configuration
			config = new Config(configFilename);
		} else {
			throw new InvalidParameterException("Parameter required: --config");
		}
		if (cmd.hasOption(PARAM_MONITOR)) {
			monitorPort = Integer.parseInt(cmd.getOptionValue(PARAM_MONITOR));
		}

		if (cmd.hasOption(COMMAND_LIST)) {
			config.listAll();

		} else if (cmd.hasOption(COMMAND_START)) {
			logger.info("Welcome to db-xtract");

			// get db-xtract configuration
			config.report();

			// Starts monitor server
			new Monitor(monitorPort, config).start();

			// Starts service
			App app = new App(config);
			app.start();

		} else {
			throw new ParseException("A command is required: --list or --start");
		}
	}

	/**
	 * <p>
	 * Starts the dbxtract app.
	 * </p>
	 * <p>
	 * usage: java -jar dbxtract.jar --config &lt;file&gt; [--list | --start]
	 * [--monitor &lt;port&gt;]
	 * </p>
	 * 
	 * <pre>
	 * --config &lt;file&gt;    configuration file pathname
	 * --list             list configuration parameters and values
	 * --monitor &lt;port&gt;   monitoring port number (default: 9000)
	 * --start            start dbxtract agent
	 * </pre>
	 * 
	 * @param args
	 *            execution call arguments
	 */
	public static void main(String[] args) {

		Options options = prepareCmdLineOptions();
		try {
			CommandLineParser parser = new DefaultParser();

			CommandLine cmd = parser.parse(options, args);
			parseCommand(cmd);

		} catch (ParseException e1) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("java -jar dbxtract.jar", options, true);

			logger.error(e1);

		} catch (Exception e) {
			logger.fatal("Unable to start dbxtract", e);
		}
	}
}
