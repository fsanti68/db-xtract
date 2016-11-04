package com.dsf.dbxtract.cdc;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * 
 * 
 * @author fabio de santi
 * @version 0.1
 */
public class App {

	private static Logger logger = LogManager.getLogger(App.class.getName());

	private Config config = null;
	private Map<Handler, Source> handlerMap = new HashMap<Handler, Source>();

	public void setConfig(Config config) {
		this.config = config;
	}

	public Config getConfig() {
		return config;
	}

	public void start(String agentName) {

		// Prepare a handler's list and respective data sources
		for (Source source : config.getDataSources()) {
			String[] handlers = source.getHandlers().split(",");
			for (String handlerName : handlers) {
				Handler handler;
				try {
					handler = (Handler) Class.forName(handlerName).newInstance();
					handlerMap.put(handler, source);

				} catch (Exception e) {
					logger.fatal("Unable to instantiate a handler: " + handlerName, e);
					System.err.println("Unable to instantiate a handler: " + handlerName);
					return;
				}
			}
		}

		// Get ZooKeeper's connection string
		String zkConnection = config.getZooKeeper();

		// Get interval (in milliseconds) between executions
		long interval = 5000L;
		String _interval = config.getInterval();
		if (_interval != null && !_interval.isEmpty()) {
			try {
				interval = Long.parseLong(_interval);
			} catch (NumberFormatException nfe) {
			}

		}

		// Prepare the task's list. Each handler becomes a task.
		List<Executor> executors = new ArrayList<Executor>();
		for (Handler handler : handlerMap.keySet()) {
			executors.add(new Executor(agentName, zkConnection, handler, handlerMap.get(handler)));
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

	private static String getComputerName() {
		Map<String, String> env = System.getenv();
		if (env.containsKey("COMPUTERNAME"))
			return env.get("COMPUTERNAME");
		else if (env.containsKey("HOSTNAME"))
			return env.get("HOSTNAME");
		else
			try {
				return "Agent-" + InetAddress.getLocalHost().getHostName();

			} catch (UnknownHostException e) {
				return "Agent-" + (System.currentTimeMillis() % 10000);
			}
	}

	public static void main(String[] args) throws Exception {

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

		String agentName = getComputerName();

		app.start(agentName);
	}
}
