package com.dsf.dbxtract.cdc.mon;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.dsf.dbxtract.cdc.Config;
import com.sun.net.httpserver.HttpServer;

/**
 * Set's up a HTTP listener for statistics and administration tasks.
 * 
 * @author fabio de santi
 * @version 0.2
 */
public class Monitor {

	protected int serverPort = 8080;
	private HttpServer server = null;

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
		serverPort = port;

		server = HttpServer.create(new InetSocketAddress(port), 0);
		server.createContext("/info", new InfoHandler(config));
		server.setExecutor(null);
		server.start();
	}

	/**
	 * Shutdown the monitor listener.
	 */
	public void stop() {
		if (server != null)
			server.stop(0);
	}
}
