package com.dsf.dbxtract.cdc.mon;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Iterator;

import com.dsf.dbxtract.cdc.mon.Statistics.StatEntry;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

class InfoHandler implements HttpHandler {

	@Override
	public void handle(HttpExchange t) throws IOException {

		InputStreamReader is = new InputStreamReader(t.getRequestBody());
		StringBuilder sb = new StringBuilder();
		char[] b = new char[1024];
		int len;
		while ((len = is.read(b)) > 0)
			sb.append(b, 0, len);
		is.close();

		String q = sb.toString();
		String response = getInfo();
		t.sendResponseHeaders(200, response.length());
		OutputStream os = t.getResponseBody();
		os.write(response.getBytes());
		os.close();
	}

	private String getInfo() {
		StringBuilder sb = new StringBuilder();
		sb.append("{ \"handlers\": [");
		Statistics stats = new Statistics();
		Iterator<String> it = stats.keySet().iterator();
		while (it.hasNext()) {
			String handler = it.next();
			StatEntry entry = stats.get(handler);
			sb.append("\n{ \"handler\": \"").append(handler).append("\", {");
			sb.append(" \"lastSeek\": \"").append(entry.getLastSeek()).append("\",");
			sb.append(" \"lastRead\": \"").append(entry.getLastRead()).append("\",");
			sb.append(" \"count\": ").append(entry.getReadCount());
			sb.append("}").append(it.hasNext() ? "," : "");
		}
		sb.append("\n]}\n");
		return sb.toString();
	}
}
