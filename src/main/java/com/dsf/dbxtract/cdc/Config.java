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
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
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

	private Properties props;
	private String agentName = null;
	private Map<JournalHandler, Source> handlerMap = null;

	/**
	 * Loads configuration file.
	 * 
	 * @param path
	 *            file/path name
	 * @throws Exception
	 */
	public Config(String path) throws Exception {

		this(new FileInputStream(new File(path)));
	}

	/**
	 * Loads configuration from a stream.
	 * 
	 * @param source
	 *            configuration source stream
	 * @throws Exception
	 */
	public Config(InputStream source) throws Exception {
		Properties props = new Properties();
		props.load(source);
		this.props = props;
		init();
	}

	private void init() throws Exception {

		// Prepare a handler's list and respective data sources
		handlerMap = new HashMap<JournalHandler, Source>();
		Sources sources = getDataSources();
		if (sources != null) {
			for (Source source : sources.getSources()) {
				for (String handlerName : source.getHandlers()) {
					JournalHandler handler;
					try {
						handler = (JournalHandler) Class.forName(handlerName).newInstance();
						handlerMap.put(handler, source);

					} catch (Exception e) {
						logger.fatal("Unable to instantiate a handler: " + handlerName, e);
						throw e;
					}
				}
			}

		} else {
			logger.warn("No datasources defined");
		}
	}

	private CuratorFramework getClientForSources() throws Exception {

		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(getZooKeeper(), retryPolicy);
		client.start();

		String path = App.BASEPREFIX + "/config";
		if (client.checkExists().forPath(path) == null)
			client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath(path);

		return client;
	}

	/**
	 * Data sources are a list of database connections and its associated
	 * handler's class names. Data sources are kept in ZooKeeper, under the
	 * <i>config</i> entry.
	 * 
	 * @return data sources list
	 * @throws Exception
	 */
	public Sources getDataSources() throws Exception {

		String path = App.BASEPREFIX + "/config";
		CuratorFramework client = getClientForSources();
		if (client.checkExists().forPath(path) == null)
			throw new Exception("No configuration found (zk): " + path);
		byte[] json = client.getData().forPath(path);
		client.close();
		if (json == null || json.length == 0)
			return null;

		try {
			ObjectMapper mapper = new ObjectMapper();
			return mapper.readValue(json, Sources.class);

		} catch (JsonMappingException jme) {
			logger.error("Invalid config data: " + new String(json));

			return null;
		}
	}

	/**
	 * 
	 * @param sources
	 * @throws Exception
	 */
	private void setDataSources(Sources sources) throws Exception {

		String path = App.BASEPREFIX + "/config";
		CuratorFramework client = getClientForSources();
		try {
			ObjectMapper mapper = new ObjectMapper();
			byte[] data = mapper.writeValueAsBytes(sources);
			client.setData().forPath(path, data);

		} finally {
			client.close();
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
	public String getZooKeeper() throws Exception {
		String s = props.getProperty("zookeeper");
		if (s == null || s.isEmpty())
			throw new Exception("zookeeper is a required configuration parameter!");
		return s;
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

	/**
	 * nice logging for objects
	 */
	public void report() {
		logger.info("Loaded configuration: ");
		try {
			logger.info("[Data Sources      ] " + getDataSources().getSources().size() + " loaded");
		} catch (Exception e) {
			logger.info("[Data Sources      ] failed - " + e.getMessage());
		}
		try {
			logger.info("[Zookeeper address ] " + getZooKeeper());
		} catch (Exception e) {
			logger.info("[Zookeeper address ] failed - " + e.getMessage());
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
	 * @throws Exception
	 */
	public void datasourceAdd(String name, String conn, String driverClass, String user, String pwd) throws Exception {

		Sources sources = getDataSources();
		if (sources != null) {
			for (Source source : sources.getSources()) {
				if (source.getName().equals(name))
					throw new Exception("A datasource named '" + name + "' already exists");
			}
			if (name == null || name.isEmpty())
				throw new Exception("A name must be provided");
			if (conn == null || conn.isEmpty())
				throw new Exception("A connection string must be provided");

		} else {
			sources = new Sources();
		}

		sources.getSources().add(new Source(name, conn, driverClass, user, pwd, new ArrayList<String>()));
		setDataSources(sources);
		logger.info("Datasource '" + name + "' registered");
	}

	/**
	 * Remove an existing datasource
	 * 
	 * @param sourceName
	 *            datasource name
	 * @throws Exception
	 */
	public void datasourceDelete(String sourceName) throws Exception {

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
			throw new Exception("Datasource named '" + sourceName + "' not found!");

		} else
			throw new Exception("No datasources defined");
	}

	/**
	 * Sets the datasource scan interval.
	 * 
	 * @param interval
	 *            scan interval in milliseconds.
	 * @throws Exception
	 */
	public void datasourceInterval(String interval) throws Exception {

		Sources sources = getDataSources();
		if (sources == null)
			sources = new Sources();
		long val = 5000L;
		try {
			val = Long.parseLong(interval);
			if (val <= 0)
				throw new NumberFormatException();

		} catch (NumberFormatException nfe) {
			throw new Exception("Invalid interval: must be an integer positive number");
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
	 * @throws Exception
	 */
	public void handlerAdd(String sourceName, String handlerClass) throws Exception {

		Sources sources = getDataSources();
		if (sources == null)
			throw new Exception("No datasources found");

		for (Source source : sources.getSources()) {
			if (source.getName().equals(sourceName)) {
				for (String handler : source.getHandlers()) {
					if (handler.equals(handlerClass)) {
						throw new Exception("The handler '" + handlerClass + "' already exists for datasource '"
								+ sourceName + "'");
					}
				}
				try {
					Class.forName(handlerClass);
					source.getHandlers().add(handlerClass);
					setDataSources(sources);
					logger.info("Handler '" + handlerClass + "' added to datasource '" + sourceName + "'");
					return;

				} catch (ClassNotFoundException cnfe) {
					throw new Exception("Unable to add handler '" + handlerClass + "': class not found");
				}
			}
		}
		throw new Exception("Datasource '" + sourceName + "' not found");
	}

	/**
	 * Removes a handler from a datasource
	 * 
	 * @param sourceName
	 *            datasource name
	 * @param handlerClass
	 *            handler's class name
	 * @throws Exception
	 */
	public void handlerDelete(String sourceName, String handlerClass) throws Exception {

		Sources sources = getDataSources();
		if (sources == null)
			throw new Exception("No datasources found");

		for (Source source : sources.getSources()) {
			if (source.getName().equals(sourceName)) {
				for (String handler : source.getHandlers()) {
					if (handler.equals(handlerClass)) {
						source.getHandlers().remove(handler);
						setDataSources(sources);
						logger.info("Handler '" + handlerClass + "' removed from datasource '" + sourceName + "'");
						return;
					}
				}
				throw new Exception("Datasource '" + sourceName + "' does not have this handler");
			}
		}
		throw new Exception("Datasource '" + sourceName + "' not found");
	}

	/**
	 * Print out configuration data.
	 * 
	 * @throws Exception
	 */
	public void listAll() throws Exception {
		Sources sources = getDataSources();
		if (sources == null)
			throw new Exception("No datasources found!");

		ObjectMapper mapper = new ObjectMapper();
		logger.info(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(sources));
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("Config [dataSources=");
		try {
			sb.append(getDataSources().toString());
		} catch (Exception e) {
		}
		sb.append(", zookeeper=");
		try {
			sb.append(getZooKeeper());
		} catch (Exception e) {
		}
		sb.append(", threadPoolSize=" + getThreadPoolSize()).append(']');
		return sb.toString();
	}
}
