package com.dsf.dbxtract.cdc.mon;

import java.io.StringWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.dsf.dbxtract.cdc.Config;
import com.dsf.dbxtract.cdc.ConfigurationException;
import com.dsf.dbxtract.cdc.mon.Statistics.StatEntry;

import fi.iki.elonen.NanoHTTPD;
import fi.iki.elonen.NanoHTTPD.IHTTPSession;
import fi.iki.elonen.NanoHTTPD.Response;

/**
 * Provides basic statistics from CDC Handlers, like last run time, last capture
 * time and total rows captured during current session.
 * 
 * @author fabio de santi
 * @version 0.2
 */
public class InfoHandler {

	private static Logger logger = LogManager.getLogger(InfoHandler.class.getName());
	private ObjectMapper mapper = null;
	private Config config = null;

	/**
	 * Constructor for /info request's object.
	 * 
	 * @param config
	 *            configuration object
	 */
	public InfoHandler(Config config) {
		this.config = config;
	}

	/**
	 * Serve /info requests.
	 * 
	 * @param session
	 *            {@link IHTTPSession} session data
	 * @return
	 */
	public Response serve(IHTTPSession session) {

		try {
			Map<String, List<String>> parms = session.getParameters();
			if (logger.isDebugEnabled()) {
				for (Map.Entry<String, List<String>> entry : parms.entrySet()) {
					logger.debug(entry.getKey() + "=" + entry.getValue().toString());
				}
			}
			return NanoHTTPD.newFixedLengthResponse(getInfo());

		} catch (Exception e) {
			logger.error("[/info] error", e);
			return NanoHTTPD.newFixedLengthResponse("/info error: " + e.getMessage());
		}
	}

	private String getInfo() throws ConfigurationException {

		if (mapper == null)
			mapper = new ObjectMapper();

		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(config.getZooKeeper(), retryPolicy);
		client.start();
		Stat stat = new Stat();
		try {
			List<String> handlers = client.getChildren().forPath(Statistics.ZOOPATH);
			for (String handler : handlers) {
				byte[] b = client.getData().forPath(Statistics.ZOOPATH + "/" + handler);
				StatEntry entry = mapper.readValue(b, Statistics.StatEntry.class);
				stat.getMap().put(handler, entry);
			}

			StringWriter writer = new StringWriter();
			mapper.writeValue(writer, stat);
			return writer.toString();

		} catch (Exception e) {
			throw new ConfigurationException("failed to retrieve zk statistics at " + Statistics.ZOOPATH, e);

		} finally {
			client.close();
		}
	}

	@XmlRootElement
	@XmlType(propOrder = { "handlers" })
	protected static class Stat {

		private Map<String, Statistics.StatEntry> m;

		@XmlTransient
		protected Map<String, Statistics.StatEntry> getMap() {
			if (m == null)
				m = new HashMap<String, Statistics.StatEntry>();
			return m;
		}

		public Collection<Statistics.StatEntry> getHandlers() {
			return getMap().values();
		}
	}
}
