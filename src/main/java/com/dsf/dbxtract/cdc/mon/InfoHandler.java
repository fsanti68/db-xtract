package com.dsf.dbxtract.cdc.mon;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.StringWriter;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

/**
 * Provides basic statistics from CDC Handlers, like last run time, last capture
 * time and total rows captured during current session.
 * 
 * @author fabio de santi
 * @version 0.1
 */
class InfoHandler implements HttpHandler {

	private static Logger logger = LogManager.getLogger(InfoHandler.class.getName());
	private ObjectMapper mapper = null;

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

		} catch (IOException e) {
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

	private String getInfo() throws IOException {

		if (mapper == null)
			mapper = new ObjectMapper();

		StringWriter writer = new StringWriter();
		mapper.writeValue(writer, new Statistics());
		return writer.toString();
	}
}
