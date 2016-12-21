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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
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
import org.codehaus.jackson.map.ObjectMapper;

import com.dsf.dbxtract.cdc.journal.JournalHandler;

/**
 * <p>
 * The Config class represents a configuration properties file.
 * </p>
 * Expected configuration items are:
 * <table summary="configuration parameters">
 * <thead>
 * <tr>
 * <th>Item</th>
 * <th>Description</th>
 * </tr>
 * </thead> <tbody>
 * <tr>
 * <td>zookeeper</td>
 * <td>Zookeeper's connection address</td>
 * </tr>
 * <tr>
 * <td>thread.pool.size</td>
 * <td>Maximum concurrent data capture executors</td>
 * </tr>
 * <tr>
 * <td>affinity</td>
 * <td>Comma-delimited list of data sources enabled for this node</td>
 * </tr>
 * <tr>
 * <td>interval</td>
 * <td>Interval between data capture cycles (any integer equal to or greater
 * than zero)</td>
 * </tr>
 * <tr>
 * <td>sources</td>
 * <td>Comma-delimited list of data sources</td>
 * </tr>
 * <tr>
 * <td>source.&lt;<i>source</i>&gt;.connection</td>
 * <td>jdbc connection string</td>
 * </tr>
 * <tr>
 * <td>source.&lt;<i>source</i>&gt;.driver</td>
 * <td>jdbc driver's full classname</td>
 * </tr>
 * <tr>
 * <td>source.&lt;<i>source</i>&gt;.user</td>
 * <td>connection's user name</td>
 * </tr>
 * <tr>
 * <td>source.&lt;<i>source</i>&gt;.password</td>
 * <td>user's password</td>
 * </tr>
 * <tr>
 * <td>source.&lt;<i>source</i>&gt;.handlers</td>
 * <td>comma-delimited list of handler's full classnames</td>
 * </tr>
 * </tbody>
 * </table>
 * 
 * @author fabio de santi
 * @version 0.5
 */
public class Config {

	private static final Logger logger = LogManager.getLogger(Config.class.getName());

	private static final long MINUTE = 60000L;
	private long lastLoaded = 0L;
	private String configFilename = null;
	private Properties props;
	private String agentName = null;
	private Map<JournalHandler, Source> handlerMap = null;
	private Sources sources = null;

	/**
	 * Constructs the object loading configuration properties from a given file.
	 * 
	 * @param path
	 *            file/path name
	 * 
	 * @throws ConfigurationException
	 *             any configuration retrieval error
	 */
	public Config(String path) throws ConfigurationException {

		configFilename = path;
		init(path);
	}

	/**
	 * Read configuration file data.
	 * 
	 * @param filename
	 *            properties file pathname
	 * 
	 * @throws ConfigurationException
	 */
	private void init(String filename) throws ConfigurationException {

		try {
			InputStream stream = new FileInputStream(new File(filename));
			init(stream);

		} catch (FileNotFoundException e) {
			throw new ConfigurationException("configuration file not found: " + filename, e);
		}
	}

	/**
	 * Get configuration data from a stream and initializes all class members.
	 * 
	 * @param stream
	 *            configuration data stream
	 * 
	 * @throws ConfigurationException
	 */
	private void init(InputStream stream) throws ConfigurationException {

		Properties p = new Properties();
		try {
			p.load(stream);

		} catch (IOException e) {
			throw new ConfigurationException("failed to load configuration", e);
		}
		this.props = p;

		// Prepare a handler's list and respective data sources
		handlerMap = new HashMap<>();
		Sources srcs = getDataSources();
		if (srcs != null) {
			List<String> affinity = getAffinity();
			for (Source source : srcs.getSources()) {
				if (affinity.isEmpty() || affinity.contains(source.getName())) {
					addHandlerToMap(source);

				} else
					logger.info("Source named '" + source.getName() + "' ignored: no match with affinity paramater");
			}

		} else
			logger.warn("No datasources defined");
	}

	/**
	 * Updates the class attributes at least every one minute.
	 * 
	 * @throws ConfigurationException
	 */
	private void checkUpdated() throws ConfigurationException {

		if (System.currentTimeMillis() - lastLoaded > MINUTE) {
			lastLoaded = System.currentTimeMillis();
			init(configFilename);
		}
	}

	/**
	 * Add a handler to the handler's map (handler x source).
	 * 
	 * @param source
	 *            {@link Source} object
	 * @throws ConfigurationException
	 */
	private void addHandlerToMap(Source source) throws ConfigurationException {

		for (String handlerName : source.getHandlers()) {
			JournalHandler handler;
			try {
				handler = (JournalHandler) Class.forName(handlerName).newInstance();
				handlerMap.put(handler, source);

			} catch (Exception e) {
				throw new ConfigurationException("Unable to instantiate a handler: " + handlerName, e);
			}
		}
	}

	/**
	 * Data sources are a list of database connections and its associated
	 * handler's class names.
	 * 
	 * @return a {@link Sources} object with a list of data sources.
	 * 
	 * @throws ConfigurationException
	 *             when a required entry is missed
	 */
	public Sources getDataSources() throws ConfigurationException {

		checkUpdated();
		if (sources == null) {
			String srcs = props.getProperty("sources");
			if (srcs == null)
				throw new ConfigurationException("Required configuration entry missed: 'sources'");

			sources = new Sources();
			for (String srcname : srcs.split(",")) {
				sources.getSources().add(getDataSource(srcname));
			}

			String intrvl = props.getProperty("interval");
			if (intrvl == null || intrvl.isEmpty())
				throw new ConfigurationException("Required configuration entry missed: 'interval'");

			sources.setInterval(Long.parseLong(intrvl));
		}
		return sources;
	}

	/**
	 * Retrieve a datasource by its name.
	 * 
	 * @param srcname
	 *            data source name
	 * @return a {@link Source} object that matches with the given name
	 * 
	 * @throws ConfigurationException
	 */
	private Source getDataSource(String srcname) throws ConfigurationException {

		if (srcname == null || srcname.isEmpty())
			throw new ConfigurationException("source name cannot be empty or null");

		String key = "source." + srcname.trim() + ".";
		Source source = new Source(srcname, props.getProperty(key + "connection"), props.getProperty(key + "driver"),
				props.getProperty(key + "user"), props.getProperty(key + "password"), null);
		String handlers = props.getProperty(key + "handlers");
		if (handlers == null || handlers.isEmpty())
			throw new ConfigurationException("Required configuration entry missed: '" + key + handlers + "'");

		for (String handler : handlers.split(",")) {
			source.getHandlers().add(handler.trim());
		}

		return source;
	}

	/**
	 * Retrieves all handlers defined in the configuration properties.
	 * 
	 * @return a collection of handlers
	 * 
	 * @throws ConfigurationException
	 *             any configuration retrieval error
	 */
	public Collection<JournalHandler> getHandlers() throws ConfigurationException {
		checkUpdated();
		return handlerMap.keySet();
	}

	/**
	 * Retrieves the data source associated to a given handler.
	 * 
	 * @param handler
	 *            a {@link JournalHandler} object
	 * @return a {@link Source} object associated to the handler
	 * 
	 * @throws ConfigurationException
	 *             any configuration retrieval error
	 */
	public Source getSourceByHandler(JournalHandler handler) throws ConfigurationException {
		checkUpdated();
		return handlerMap.get(handler);
	}

	/**
	 * Retrieves the zookeeper connection string.
	 * 
	 * @return ZooKeeper connection string (i.e. "localhost:2181")
	 * 
	 * @throws ConfigurationException
	 *             any configuration retrieval error
	 */
	public String getZooKeeper() throws ConfigurationException {
		checkUpdated();
		String s = props.getProperty("zookeeper");
		if (s == null || s.isEmpty())
			throw new ConfigurationException("zookeeper is a required configuration parameter!");
		return s;
	}

	/**
	 * Retrieves the <code>thread.pool.size</code> parameter.
	 * 
	 * @return maximum concurrent threads
	 * @throws ConfigurationException
	 *             any configuration retrieval error
	 */
	public int getThreadPoolSize() throws ConfigurationException {
		checkUpdated();
		int pool = 5;
		String p = props.getProperty("thread.pool.size");
		if (p != null && !p.isEmpty()) {
			try {
				pool = Integer.parseInt(p);
			} catch (NumberFormatException nfe) {
				logger.warn("Invalid config 'thread.pool.size' = " + p + " -> assuming " + pool);
			}
		}
		return pool;
	}

	/**
	 * Get a list of datasources enabled for this node (parameter
	 * <code>affinity</code>). An empty list means that all datasources must be
	 * considered.
	 * 
	 * @return empty list or a list of datasources for this node
	 * 
	 * @throws ConfigurationException
	 *             any configuration retrieval error
	 */
	public List<String> getAffinity() throws ConfigurationException {

		checkUpdated();
		List<String> affinity = new ArrayList<>();
		String aff = props.getProperty("affinity");
		if (aff != null && !aff.isEmpty()) {
			String[] chunks = aff.split(",");
			for (String s : chunks) {
				String item = s.trim();
				if (!item.isEmpty())
					affinity.add(item);
			}
		}
		return affinity;
	}

	/**
	 * Retrieves a 'unofficial' agent name based. This agent name is generated
	 * as:
	 * <ul>
	 * <li>environment variable 'COMPUTERNAME' or</li>
	 * <li>environment variable 'HOSTNAME' or</li>
	 * <li>network's local hostname (<code>Agent-xxxxx</code>) or</li>
	 * <li>a timestamp generated when the agent is started
	 * (<code>Agent-999999</code>)
	 * <li>
	 * </ul>
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
					logger.warn(e);
				}
			}
		}
		return agentName;
	}

	/**
	 * Nice logging for objects
	 */
	public void report() {
		logger.info("Loaded configuration: ");
		try {
			logger.info("[Data Sources      ] " + getDataSources().getSources().size() + " loaded");
		} catch (Exception e) {
			logger.warn("[Data Sources      ]", e);
		}
		try {
			logger.info("[Zookeeper address ] " + getZooKeeper());
		} catch (Exception e) {
			logger.warn("[Zookeeper address ]", e);
		}
		try {
			logger.info("[Thread pool size  ] " + getThreadPoolSize());
		} catch (ConfigurationException ce) {
			logger.warn("[Thread pool size  ] ", ce);
		}
	}

	/**
	 * Print out configuration data.
	 * 
	 * @throws ConfigurationException
	 *             any configuration retrieval error
	 */
	public void listAll() throws ConfigurationException {
		Sources srcs = getDataSources();
		if (srcs == null)
			throw new ConfigurationException("No datasources found!");

		ObjectMapper mapper = new ObjectMapper();
		try {
			logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(srcs));

		} catch (Exception e) {
			throw new ConfigurationException("failed to get datasource from zk", e);
		}
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Config [dataSources=");
		try {
			sb.append(getDataSources().toString());
		} catch (Exception e) {
			logger.warn("datasource retrieval", e);
		}
		sb.append(", zookeeper=");
		try {
			sb.append(getZooKeeper());
		} catch (Exception e) {
			logger.warn("zookeeper address retrieval", e);
		}
		try {
			sb.append(", threadPoolSize=" + getThreadPoolSize());
		} catch (ConfigurationException ce) {
			logger.warn("thread pool size retrieval", ce);
		}
		sb.append(']');
		return sb.toString();
	}
}