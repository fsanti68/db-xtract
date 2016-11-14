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
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.codehaus.jackson.map.ObjectMapper;

import com.dsf.dbxtract.cdc.journal.JournalHandler;

import junit.framework.TestCase;

public class ConfigTest extends TestCase {

	private static final String zookeeper = "localhost:2181";
	private static final long interval = 1000L;
	private static final String driver = "org.gjt.mm.mysql.Driver";
	private static final String connection = "jdbc:mysql://localhost:3306/smartboard";
	private static final String handler = "com.dsf.dbxtract.cdc.sample.TestHandler";

	private Config config;

	@Override
	protected void setUp() throws Exception {
		File f = File.createTempFile("config", "props");
		FileWriter fw = new FileWriter(f);
		fw.append("log4j.rootLogger=DEBUG,A1\nlog4j.appender.A1=org.apache.log4j.ConsoleAppender"
				+ "\nlog4j.appender.A1.layout=org.apache.log4j.PatternLayout"
				+ "\nlog4j.appender.A1.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n");
		fw.append("\nzookeeper=").append(zookeeper).append("\nthread.pool.size=5");
		fw.close();

		Sources sources = new Sources();
		sources.setInterval(interval);
		List<String> handlers = new ArrayList<String>();
		handlers.add(handler);
		sources.getSources().add(new Source("test", connection, driver, "root", "mysql", handlers));
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		CuratorFramework client = CuratorFrameworkFactory.newClient(zookeeper, retryPolicy);
		client.start();
		ObjectMapper mapper = new ObjectMapper();
		byte[] value = mapper.writeValueAsBytes(sources);
		client.setData().forPath(App.BASEPREFIX + "config", value);
		client.close();

		config = new Config(f.getAbsolutePath());
		super.setUp();
	}

	public void testConfig() {
		assertNotNull(config);
	}

	public void testGetDataSources() throws Exception {
		assertTrue(config.getDataSources().getSources().size() == 1);
		Source src = config.getDataSources().getSources().get(0);
		assertEquals(connection, src.getConnection());
		assertEquals(driver, src.getDriver());
	}

	public void testGetHandlers() throws Exception {
		Source src = config.getDataSources().getSources().get(0);
		assertEquals(handler, src.getHandlers().get(0));
	}

	public void testGetSourceByHandler() throws Exception {
		Source src = config.getDataSources().getSources().get(0);
		for (JournalHandler handler : config.getHandlers()) {
			assertEquals(src, config.getSourceByHandler(handler));
		}
	}

	public void testGetZooKeeper() throws Exception {
		assertEquals(zookeeper, config.getZooKeeper());
	}

	public void testGetInterval() throws Exception {
		assertEquals(interval, config.getDataSources().getInterval());
	}

	public void testGetAgentName() {
		assertNotNull(config.getAgentName());
	}
}
