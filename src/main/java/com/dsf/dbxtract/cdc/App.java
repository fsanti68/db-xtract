package com.dsf.dbxtract.cdc;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Main application
 * 
 * @author fabio de santi
 * @version 0.1
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

		// Prepare the task's list. Each handler becomes a task.
		List<Executor> executors = new ArrayList<Executor>();
		for (Handler handler : config.getHandlers()) {
			executors.add(
					new Executor(config.getAgentName(), zkConnection, handler, config.getSourceByHandler(handler)));
		}

		// Process tasks
		while (true) {
			for (Executor executor : executors) {
				try {
					executor.execute();

				} catch (Exception e) {
					logger.error("Unable to execute task '" + executor.toString() + "'", e);
				}
			}

			try {
				Thread.sleep(interval);
			} catch (Exception e) {
			}
		}
	}

	/**
	 * <p>Starts the dbxtract app.</p>
	 * <p>Usage: java -jar dbxtract.jar --config </path/to/config.properties></p>
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
