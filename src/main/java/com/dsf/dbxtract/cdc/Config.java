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

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.dsf.dbxtract.cdc.journal.JournalHandler;

/**
 * 
 * @author fabio de santi
 * @version 0.3
 */
public class Config {

	private static final Logger logger = LogManager.getLogger(Config.class.getName());

	private static final String CONFIGPATH = "/config";

	private RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

	private Properties props;
	private String agentName = null;
	private Map<JournalHandler, Source> handlerMap = null;

	/**
	 * Loads configuration file.
	 * 
	 * @param path
	 *            file/path name
	 * @throws ConfigurationException
	 * @throws Exception
	 */
	public Config(String path) throws ConfigurationException {

		InputStream stream = null;
		try {
			stream = new FileInputStream(new File(path));
			init(stream);

		} catch (FileNotFoundException e) {
			throw new ConfigurationException("configuration file not found: " + path, e);

		} finally {
			if (stream != null)
				try {
					stream.close();
				} catch (IOException e) {
					logger.warn("failed to close file " + path, e);
				}
		}
	}

	/**
	 * Loads configuration from a stream.
	 * 
	 * @param source
	 *            configuration source stream
	 * @throws IOException
	 * @throws ConfigurationException
	 * @throws Exception
	 */
	public Config(InputStream source) throws ConfigurationException {
		init(source);
	}

	/**
	 * Check zookeeper connection & config availability
	 * 
	 * @throws ConfigurationException
	 */
	private void checkZooKeeper() throws ConfigurationException {

		CuratorFramework client = CuratorFrameworkFactory.newClient(getZooKeeper(), retryPolicy);
		client.start();

		String path = App.BASEPREFIX + CONFIGPATH;
		try {
			if (client.checkExists().forPath(path) == null)
				client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);

		} catch (Exception e) {
			throw new ConfigurationException("Failed to access zk entry " + path, e);

		} finally {
			client.close();
		}
	}

	/**
	 * Get configuration as a stream
	 * 
	 * @param stream
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

		// check if zk is ok
		checkZooKeeper();

		// Prepare a handler's list and respective data sources
		handlerMap = new HashMap<JournalHandler, Source>();
		Sources sources = getDataSources();
		if (sources != null) {
			List<String> affinity = getAffinity();
			for (Source source : sources.getSources()) {
				if (affinity.isEmpty() || affinity.contains(source.getName())) {
					addHandlerToMap(source);

				} else
					logger.info("Source named '" + source.getName() + "' ignored: no match with affinity paramater");
			}

		} else
			logger.warn("No datasources defined");
	}

	/**
	 * Add a handler to the handler's map (handler x source)
	 * 
	 * @param source
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

	private byte[] getZkData(String path) throws ConfigurationException {

		CuratorFramework zk = CuratorFrameworkFactory.newClient(getZooKeeper(), retryPolicy);
		zk.start();
		try {
			if (zk.checkExists().forPath(path) == null)
				throw new ConfigurationException("No configuration found (zk) at " + path);
			return zk.getData().forPath(path);

		} catch (Exception e) {
			throw new ConfigurationException("failed to retrieve zk entry at " + path, e);

		} finally {
			zk.close();
		}
	}

	/**
	 * Data sources are a list of database connections and its associated
	 * handler's class names. Data sources are kept in ZooKeeper, under the
	 * <i>config</i> entry.
	 * 
	 * @return data sources list
	 * @throws IOException
	 * @throws JsonParseException
	 * @throws Exception
	 */
	public Sources getDataSources() throws ConfigurationException {

		String path = App.BASEPREFIX + CONFIGPATH;

		byte[] json = getZkData(path);
		if (json == null || json.length == 0)
			return null;
		try {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readValue(json, Sources.class);

		} catch (JsonMappingException jme) {
			logger.error("Invalid config data on " + path, jme);
			return null;

		} catch (Exception e) {
			throw new ConfigurationException("Failed to access zk entry", e);
		}
	}

	/**
	 * 
	 * @param sources
	 * @throws ConfigurationException
	 * @throws IOException
	 * @throws JsonMappingException
	 * @throws JsonGenerationException
	 * @throws Exception
	 */
	private void setDataSources(Sources sources) throws ConfigurationException {

		String path = App.BASEPREFIX + CONFIGPATH;
		CuratorFramework zk = CuratorFrameworkFactory.newClient(getZooKeeper(), retryPolicy);
		zk.start();
		try {
			ObjectMapper mapper = new ObjectMapper();
			byte[] data = mapper.writeValueAsBytes(sources);
			zk.setData().forPath(path, data);

		} catch (Exception e) {
			throw new ConfigurationException("Failed to save zk entry " + path, e);

		} finally {
			zk.close();
		}
	}

	/**
	 * 
	 * @return
	 */
	public Collection<JournalHandler> getHandlers() {
		return handlerMap.keySet();
	}

	/**
	 * 
	 * @param handler
	 * @return
	 */
	public Source getSourceByHandler(JournalHandler handler) {
		return handlerMap.get(handler);
	}

	/**
	 * 
	 * @return ZooKeeper connection string (i.e. "localhost:2181")
	 */
	public String getZooKeeper() throws ConfigurationException {
		String s = props.getProperty("zookeeper");
		if (s == null || s.isEmpty())
			throw new ConfigurationException("zookeeper is a required configuration parameter!");
		return s;
	}

	/**
	 * 
	 * @return maximum concurrent threads
	 */
	public int getThreadPoolSize() {
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
	 * Get a list of datasources enabled for this node. An empty list means that
	 * all datasources must be considered.
	 * 
	 * @return empty list or a list of datasources for this node
	 */
	public List<String> getAffinity() {

		List<String> affinity = new ArrayList<String>();
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
	 * nice logging for objects
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
		logger.info("[Thread pool size  ] " + getThreadPoolSize());
	}

	/**
	 * Add a new datasource
	 * 
	 * @param name
	 *            source name
	 * @param conn
	 *            database connection string
	 * @param driverClass
	 *            jdbc driver class name
	 * @param user
	 *            username
	 * @param pwd
	 *            password
	 * @throws IOException
	 * @throws ConfigurationException
	 * @throws JsonParseException
	 * @throws Exception
	 */
	public void datasourceAdd(String name, String conn, String driverClass, String user, String pwd)
			throws ConfigurationException {

		Sources sources = getDataSources();
		if (sources != null) {
			for (Source source : sources.getSources()) {
				if (source.getName().equals(name))
					throw new ConfigurationException("A datasource named '" + name + "' already exists");
			}
			if (name == null || name.isEmpty())
				throw new ConfigurationException("A name must be provided");
			if (conn == null || conn.isEmpty())
				throw new ConfigurationException("A connection string must be provided");

		} else
			sources = new Sources();

		sources.getSources().add(new Source(name, conn, driverClass, user, pwd, new ArrayList<String>()));
		setDataSources(sources);
		logger.info("Datasource '" + name + "' registered");
	}

	/**
	 * Remove an existing datasource
	 * 
	 * @param sourceName
	 *            datasource name
	 * @throws IOException
	 * @throws JsonParseException
	 * @throws Exception
	 */
	public void datasourceDelete(String sourceName) throws ConfigurationException {

		Sources sources = getDataSources();
		if (sources != null) {
			for (Source source : sources.getSources()) {
				if (source.getName().equals(sourceName)) {
					sources.getSources().remove(source);
					setDataSources(sources);
					logger.info("Datasource '" + sourceName + "' removed");
					return;
				}
			}
			throw new ConfigurationException("Datasource named '" + sourceName + "' not found!");

		} else
			throw new ConfigurationException("No datasources defined");
	}

	/**
	 * Sets the datasource scan interval.
	 * 
	 * @param interval
	 *            scan interval in milliseconds.
	 * @throws ConfigurationException
	 * @throws Exception
	 */
	public void datasourceInterval(String interval) throws ConfigurationException {

		Sources sources = getDataSources();
		if (sources == null)
			sources = new Sources();
		long val = 5000L;
		try {
			val = Long.parseLong(interval);
			if (val <= 0)
				throw new NumberFormatException();

		} catch (NumberFormatException nfe) {
			throw new NumberFormatException("Invalid interval: must be an integer positive number");
		}
		sources.setInterval(val);
		setDataSources(sources);
		logger.info("Execution cycle interval set to " + val);
	}

	/**
	 * Add a new handler to a datasource
	 * 
	 * @param sourceName
	 *            datasource name
	 * @param handlerClass
	 *            handler class name
	 * @throws IOException
	 * @throws ConfigurationException
	 * @throws JsonParseException
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	public void handlerAdd(String sourceName, String handlerClass) throws ConfigurationException {

		Sources sources = getDataSources();
		if (sources == null)
			throw new ConfigurationException("No datasource found");

		for (Source source : sources.getSources()) {
			if (source.getName().equals(sourceName)) {
				addHandlerToDatasource(source, handlerClass);
				setDataSources(sources);
				logger.info("Handler '" + handlerClass + "' added to datasource '" + sourceName + "'");
				return;
			}
		}
		throw new ConfigurationException("Datasource '" + sourceName + "' not found");
	}

	private void addHandlerToDatasource(Source source, String handlerClass) throws ConfigurationException {

		for (String handler : source.getHandlers()) {
			if (handler.equals(handlerClass)) {
				throw new ConfigurationException(
						"The handler '" + handlerClass + "' already exists for datasource '" + source.getName() + "'");
			}
		}
		try {
			Class.forName(handlerClass);
			source.getHandlers().add(handlerClass);
			return;

		} catch (ClassNotFoundException cnfe) {
			throw new ConfigurationException("Unable to add handler '" + handlerClass + "': class not found", cnfe);
		}
	}

	/**
	 * Removes a handler from a datasource
	 * 
	 * @param sourceName
	 *            datasource name
	 * @param handlerClass
	 *            handler's class name
	 * @throws IOException
	 * @throws ConfigurationException
	 * @throws JsonParseException
	 * @throws Exception
	 */
	public void handlerDelete(String sourceName, String handlerClass) throws ConfigurationException {

		Sources sources = getDataSources();
		if (sources == null)
			throw new ConfigurationException("No datasources found");

		for (Source source : sources.getSources()) {
			if (source.getName().equals(sourceName)) {
				deleteHandlerFromDatasource(source, handlerClass);
				setDataSources(sources);

			}
		}
		throw new ConfigurationException("Datasource '" + sourceName + "' not found");
	}

	private void deleteHandlerFromDatasource(Source source, String handlerClass) throws ConfigurationException {

		for (String handler : source.getHandlers()) {
			if (handler.equals(handlerClass)) {
				source.getHandlers().remove(handler);
				logger.info("Handler '" + handlerClass + "' removed from datasource '" + source.getName() + "'");
				return;
			}
		}
		throw new ConfigurationException("Datasource '" + source.getName() + "' does not have this handler");
	}

	/**
	 * Print out configuration data.
	 * 
	 * @throws Exception
	 */
	public void listAll() throws ConfigurationException {
		Sources sources = getDataSources();
		if (sources == null)
			throw new ConfigurationException("No datasources found!");

		ObjectMapper mapper = new ObjectMapper();
		try {
			logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(sources));

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
		sb.append(", threadPoolSize=" + getThreadPoolSize()).append(']');
		return sb.toString();
	}
}