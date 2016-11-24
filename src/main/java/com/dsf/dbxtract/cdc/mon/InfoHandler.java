package com.dsf.dbxtract.cdc.mon;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
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
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/**
 * Provides basic statistics from CDC Handlers, like last run time, last capture
 * time and total rows captured during current session.
 * 
 * @author fabio de santi
 * @version 0.2
 */
class InfoHandler implements HttpHandler {

	private static Logger logger = LogManager.getLogger(InfoHandler.class.getName());
	private ObjectMapper mapper = null;
	private Config config = null;

	public InfoHandler(Config config) {
		this.config = config;
	}

	@Override
	public void handle(HttpExchange t) {

		try {
			String body = getRequestBody(t);
			if (body != null && !body.isEmpty())
				logger.debug("Request: " + body);
			String response = getInfo();
			t.sendResponseHeaders(200, response.length());
			OutputStream os = t.getResponseBody();
			os.write(response.getBytes());
			os.close();

		} catch (Exception e) {
			logger.error("[/info] error", e);
		}
	}

	private String getRequestBody(HttpExchange t) throws IOException {

		InputStreamReader is = new InputStreamReader(t.getRequestBody());
		StringBuilder sb = new StringBuilder();
		char[] b = new char[1024];
		int len;
		while ((len = is.read(b)) > 0)
			sb.append(b, 0, len);
		is.close();
		return sb.toString();
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
			client.close();

			StringWriter writer = new StringWriter();
			mapper.writeValue(writer, stat);
			return writer.toString();

		} catch (Exception e) {
			throw new ConfigurationException("failed to retrieve zk statistics at " + Statistics.ZOOPATH, e);
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
