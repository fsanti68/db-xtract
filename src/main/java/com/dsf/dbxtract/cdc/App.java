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
import org.codehaus.jackson.JsonParseException;

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
	private static final String PARAM_SOURCEADD = "source-add";
	private static final String PARAM_SOURCEDEL = "source-delete";
	private static final String PARAM_SOURCEINTVL = "source-interval";
	private static final String PARAM_HANDLERADD = "handler-add";
	private static final String PARAM_HANDLERDEL = "handler-delete";

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
	 * @throws IOException
	 * @throws JsonParseException
	 * 
	 * @throws Exception
	 */
	public void start() throws ConfigurationException, JsonParseException, IOException {

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
		// --source-add <name> <conn> <driver> <user> <password>
		commands.addOption(Option.builder().longOpt(PARAM_SOURCEADD).numberOfArgs(5)
				.argName("name conn driver user passwd").desc("add new datasource").required(false).build());
		// --source-delete <name>
		commands.addOption(Option.builder().longOpt(PARAM_SOURCEDEL).numberOfArgs(1).argName("name")
				.desc("remove datasource").required(false).build());
		// --source-interval <interval ms>
		commands.addOption(Option.builder().longOpt(PARAM_SOURCEINTVL).numberOfArgs(1).argName("millisecs")
				.desc("set datasource scan interval").required(false).build());
		// --handler-add <source> <handler>
		commands.addOption(Option.builder().longOpt(PARAM_HANDLERADD).numberOfArgs(2).argName("source handlerClass")
				.desc("add new handler to an existing datasource (i.e. handler-add myds com.cdc.MyHandler)")
				.required(false).build());
		// --handler-delete <source> <handler>
		commands.addOption(Option.builder().longOpt(PARAM_HANDLERDEL).numberOfArgs(2).argName("source handlerClass")
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

	private static void parseCommand(CommandLine cmd) throws Exception {

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
		if (cmd.hasOption(PARAM_SOURCEADD)) {
			String[] params = cmd.getOptionValues(PARAM_SOURCEADD);
			if (params == null || params.length < 5)
				throw new InvalidParameterException(
						"Parameters required: <source name> <connection string> <driver class> <username> <password>");
			config.datasourceAdd(params[0], params[1], params[2], params[3], params[4]);

		} else if (cmd.hasOption(PARAM_SOURCEDEL)) {
			String param = cmd.getOptionValue(PARAM_SOURCEDEL);
			config.datasourceDelete(param);

		} else if (cmd.hasOption(PARAM_SOURCEINTVL)) {
			String param = cmd.getOptionValue(PARAM_SOURCEINTVL);
			config.datasourceInterval(param);

		} else if (cmd.hasOption(PARAM_HANDLERADD)) {
			String[] params = cmd.getOptionValues(PARAM_HANDLERADD);
			if (params == null || params.length < 2)
				throw new InvalidParameterException("Parameters required: <sourceName> <handlerClass>");
			config.handlerAdd(params[0], params[1]);

		} else if (cmd.hasOption(PARAM_HANDLERDEL)) {
			String[] params = cmd.getOptionValues(PARAM_HANDLERDEL);
			if (params == null || params.length < 2)
				throw new InvalidParameterException("Parameters required: <sourceName> <handlerClass>");
			config.handlerDelete(params[0], params[1]);

		} else if (cmd.hasOption("list")) {
			config.listAll();

		} else if (cmd.hasOption("start")) {
			logger.info("Welcome to db-xtract");

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
