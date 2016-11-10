package com.dsf.dbxtract.cdc.mon;

import java.io.IOException;
import java.net.InetSocketAddress;

import com.sun.net.httpserver.HttpServer;

public class Monitor {

	protected int serverPort = 8080;

	public Monitor(int port) throws IOException {
		this.serverPort = port;

		HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
		server.createContext("/info", new InfoHandler());
		server.setExecutor(null);
		server.start();
	}
}
