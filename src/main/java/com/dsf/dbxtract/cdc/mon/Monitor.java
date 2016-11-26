package com.dsf.dbxtract.cdc.mon;

import java.io.IOException;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.dsf.dbxtract.cdc.Config;

import fi.iki.elonen.NanoHTTPD;

/**
 * Set's up a HTTP listener for statistics and administration tasks.
 * 
 * @author fabio de santi
 * @version 0.2
 */
public class Monitor extends NanoHTTPD {

	private static final Logger logger = LogManager.getLogger(Monitor.class.getName());

	private Config config;

	/**
	 * Start's monitor lister
	 * 
	 * @param port
	 *            HTTP port to listen (default: 8080)
	 * @param config
	 *            {@link Config} object
	 * @throws IOException
	 */
	public Monitor(int port, Config config) throws IOException {
		super(port);
		this.config = config;
		logger.info("Started monitor at port " + port);
	}

	/**
	 * Shutdown the monitor listener.
	 */
	public void stop() {
		super.stop();
	}

	@Override
	public Response serve(IHTTPSession session) {
		Method method = session.getMethod();
		String uri = session.getUri();
		logger.info(method + " " + uri);

		return new InfoHandler(config).serve(session);
	}
}
