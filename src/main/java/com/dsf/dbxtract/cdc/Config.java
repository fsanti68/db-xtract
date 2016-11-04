package com.dsf.dbxtract.cdc;

import java.io.File;
import java.io.FileInputStream;
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

		Properties props = new Properties();
		props.load(new FileInputStream(new File(source)));
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
	 * A lista das fontes de dados é mantida no arquivo de configuração, sob o
	 * item "sources". Várias fontes de dados podem ser cadastradas, separadas
	 * por vírgula.
	 * 
	 * @return lista das fontes de dados
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
	 * @return endereco de conexao do ZooKeeper (por ex. "localhost:2181")
	 */
	public String getZooKeeper() {
		return props.getProperty("zookeeper");
	}

	/**
	 * 
	 * @return intervalo, em milisegundos, entre os ciclos de captura
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
