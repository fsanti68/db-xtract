package com.dsf.dbxtract.cdc;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * 
 * @author fabio de santi
 * @version 0.1
 */
public class Config {

	private static final Logger logger = LogManager.getLogger(Config.class.getName());

	private Properties props;
	private String agentName = null;
	private List<Source> sources = null;
	private Map<Handler, Source> handlerMap = new HashMap<Handler, Source>();

	/**
	 * 
	 * @param source
	 * @throws Exception
	 */
	public Config(String source) throws Exception {

		this(new FileInputStream(new File(source)));
	}

	public Config(InputStream source) throws Exception {
		Properties props = new Properties();
		props.load(source);
		this.props = props;
		init();
	}

	private void init() throws Exception {

		// Prepare a handler's list and respective data sources
		for (Source source : getDataSources()) {
			String[] handlers = source.getHandlers().split(",");
			for (String handlerName : handlers) {
				Handler handler;
				try {
					handler = (Handler) Class.forName(handlerName).newInstance();
					handlerMap.put(handler, source);

				} catch (Exception e) {
					logger.fatal("Unable to instantiate a handler: " + handlerName, e);
					throw e;
				}
			}
		}
	}

	/**
	 * The sources list is kept un the configuration file, under the entry
	 * 'sources'. Many data sources can be declared, separated by comma. 
	 * 
	 * @return data sources list
	 */
	public List<Source> getDataSources() {

		if (sources == null) {
			sources = new ArrayList<Source>();
			String[] a = props.getProperty("sources").split(",");
			for (String s : a) {
				String cat = s.trim();
				sources.add(new Source(cat, props.getProperty("source." + cat + ".connection"),
						props.getProperty("source." + cat + ".driver"), props.getProperty("source." + cat + ".user"),
						props.getProperty("source." + cat + ".password"),
						props.getProperty("source." + cat + ".handlers")));
			}
		}
		return sources;
	}

	/**
	 * 
	 * @return
	 */
	public Collection<Handler> getHandlers() {
		return handlerMap.keySet();
	}

	/**
	 * 
	 * @param handler
	 * @return
	 */
	public Source getSourceByHandler(Handler handler) {
		return handlerMap.get(handler);
	}

	/**
	 * 
	 * @return ZooKeeper connection string (i.e. "localhost:2181")
	 */
	public String getZooKeeper() throws Exception {
		String s = props.getProperty("zookeeper");
		if (s == null || s.isEmpty())
			throw new Exception("zookeeper is a required configuration parameter!");
		return s;
	}

	/**
	 * 
	 * @return milliseconds between capture cycles
	 */
	public long getInterval() {
		long interval = 5000L;
		String _interval = props.getProperty("interval");
		if (_interval != null && !_interval.isEmpty()) {
			try {
				interval = Long.parseLong(_interval);
			} catch (NumberFormatException nfe) {
				logger.warn("Invalid config 'interval' = " + _interval + " -> assuming " + interval);
			}
		}
		return interval;
	}

	/**
	 * 
	 * @return maximum concurrent threads
	 */
	public int getThreadPoolSize() {
		int pool = 5;
		String _pool = props.getProperty("thread.pool.size");
		if (_pool != null && !_pool.isEmpty()) {
			try {
				pool = Integer.parseInt(_pool);
			} catch (NumberFormatException nfe) {
				logger.warn("Invalid config 'thread.pool.size' = " + _pool + " -> assuming " + pool);
			}
		}
		return pool;
	}

	/**
	 * 
	 * @return a convenience name for dbxtract agent instance
	 */
	public String getAgentName() {

		if (agentName == null) {
			Map<String, String> env = System.getenv();
			if (env.containsKey("COMPUTERNAME")) {
				agentName = env.get("COMPUTERNAME");
			} else if (env.containsKey("HOSTNAME")) {
				agentName = env.get("HOSTNAME");
			} else {
				try {
					agentName = "Agent-" + InetAddress.getLocalHost().getHostName();

				} catch (UnknownHostException e) {
					agentName = "Agent-" + (System.currentTimeMillis() % 10000);
				}
			}
		}
		return agentName;
	}
}
