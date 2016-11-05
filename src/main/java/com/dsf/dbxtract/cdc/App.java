package com.dsf.dbxtract.cdc;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Main application
 * 
 * @author fabio de santi
 * @version 0.2
 */
public class App {

	private static Logger logger = LogManager.getLogger(App.class.getName());

	private Config config = null;

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
		long interval = config.getInterval();

		ScheduledExecutorService scheduledService = Executors.newScheduledThreadPool(config.getThreadPoolSize());

		// Prepare the task's list. Each handler becomes a task.
		for (Handler handler : config.getHandlers()) {

			Runnable executor = new JournalExecutor(config.getAgentName(), zkConnection, handler,
					config.getSourceByHandler(handler));
			scheduledService.scheduleAtFixedRate(executor, 0L, interval, TimeUnit.MILLISECONDS);
		}
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

		try {
			App app = new App();
			for (int i = 0; i < args.length - 1; i++) {
				if (args[i].equals("--config")) {
					String configFilename = args[++i];
					app.setConfig(new Config(configFilename));
					PropertyConfigurator.configure(configFilename);
				}
			}

			if (app.getConfig() == null) {
				System.err.println("Parameter --config missing.");
				System.out.println("Usage: java -jar dbxtract.jar --config </path/to/config.properties>");
				System.exit(1);
			}

			app.start();

		} catch (Exception e) {
			logger.fatal("Something really wrong happened", e);
		}
	}
}
