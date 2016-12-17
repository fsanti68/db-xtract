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
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.ObjectMapper;

import com.dsf.dbxtract.cdc.journal.JournalHandler;

/**
 * 
 * @author fabio de santi
 * @version 0.4
 */
public class Config {

	private static final Logger logger = LogManager.getLogger(Config.class.getName());

	private Properties props;
	private String agentName = null;
	private Map<JournalHandler, Source> handlerMap = null;
	private Sources sources = null;

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

		// Prepare a handler's list and respective data sources
		handlerMap = new HashMap<JournalHandler, Source>();
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

	/**
	 * Data sources are a list of database connections and its associated
	 * handler's class names.
	 * 
	 * @return data sources list
	 * @throws IOException
	 * @throws JsonParseException
	 * @throws Exception
	 */
	public Sources getDataSources() throws ConfigurationException {

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
	 * Print out configuration data.
	 * 
	 * @throws Exception
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
		sb.append(", threadPoolSize=" + getThreadPoolSize()).append(']');
		return sb.toString();
	}
}