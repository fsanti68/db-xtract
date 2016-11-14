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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

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
	
	public static final String BASEPREFIX = "/dbxtract/cdc/";

	private Config config = null;
	private ScheduledExecutorService scheduledService = null;

	public void setConfig(Config config) {
		this.config = config;
	}

	public Config getConfig() {
		return config;
	}

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
	
	public void stop() {
		if (scheduledService != null)
			scheduledService.shutdown();
	}

	/**
	 * <p>
	 * Starts the dbxtract app.
	 * </p>
	 * <p>
	 * Usage: java -jar dbxtract.jar --config </path/to/config.properties>
	 * </p>
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		System.out.println("Welcome to db-xtract");

		int monitorPort = 9000;
		try {
			App app = new App();
			for (int i = 0; i < args.length - 1; i++) {
				if (args[i].equals("--config")) {
					String configFilename = args[++i];
					// configure log4j
					PropertyConfigurator.configure(configFilename);
					// an get db-xtract configuration
					app.setConfig(new Config(configFilename));
				} else if (args[i].equals("--monitor")) {
					monitorPort = Integer.parseInt(args[++i]);
				}
			}

			if (app.getConfig() == null) {
				System.out.println("Parameter --config missing.");
				System.out.println("Usage: java -jar dbxtract.jar --config </path/to/config.properties> --monitor <port>");
				System.exit(1);
			}

			// Start CDC service
			app.start();

			// Starts monitor server
			new Monitor(monitorPort, app.getConfig());

		} catch (Exception e) {
			logger.fatal("Something really wrong happened", e);
		}
	}
}
