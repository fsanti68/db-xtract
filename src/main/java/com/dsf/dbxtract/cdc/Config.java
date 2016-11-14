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
import org.codehaus.jackson.map.ObjectMapper;

import com.dsf.dbxtract.cdc.journal.JournalHandler;

/**
 * 
 * @author fabio de santi
 * @version 0.2
 */
public class Config {

	private static final Logger logger = LogManager.getLogger(Config.class.getName());

	private Properties props;
	private String agentName = null;
	private Sources sources = null;
	private Map<JournalHandler, Source> handlerMap = new HashMap<JournalHandler, Source>();

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
		report();
	}

	private void init() throws Exception {

		// Prepare a handler's list and respective data sources
		for (Source source : getDataSources().getSources()) {
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

		if (sources == null) {
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
			CuratorFramework client = CuratorFrameworkFactory.newClient(getZooKeeper(), retryPolicy);
			client.start();

			byte[] json = client.getData().forPath(App.BASEPREFIX + "config");
			client.close();

			ObjectMapper mapper = new ObjectMapper();
			sources = mapper.readValue(json, Sources.class);
		}
		return sources;
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

	private void report() {
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
}
