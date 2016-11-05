package com.dsf.dbxtract.cdc;

import java.io.File;
import java.io.FileWriter;

import com.dsf.dbxtract.cdc.Config;
import com.dsf.dbxtract.cdc.Handler;
import com.dsf.dbxtract.cdc.Source;

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
				+ "log4j.appender.A1.layout=org.apache.log4j.PatternLayout"
				+ "log4j.appender.A1.layout.ConversionPattern=%-4r [%t] %-5p %c %x - %m%n");
		fw.append("zookeeper=").append(zookeeper).append("\ninterval=").append(Long.toString(interval))
				.append("thread.pool.size=5").append("\nsources=test\nsource.test.connection=").append(connection)
				.append("\nsource.test.driver=").append(driver).append("\nsource.test.user=root")
				.append("\nsource.test.password=mysql").append("\nsource.test.handlers=").append(handler);
		fw.close();
		config = new Config(f.getAbsolutePath());
		super.setUp();
	}

	public void testConfig() {
		assertNotNull(config);
	}

	public void testGetDataSources() {
		assertTrue(config.getDataSources().size() == 1);
		Source src = config.getDataSources().get(0);
		assertEquals(connection, src.getConnection());
		assertEquals(driver, src.getDriver());
	}

	public void testGetHandlers() {
		Source src = config.getDataSources().get(0);
		assertEquals(handler, src.getHandlers());
	}

	public void testGetSourceByHandler() {
		Source src = config.getDataSources().get(0);
		for (Handler handler : config.getHandlers()) {
			assertEquals(src, config.getSourceByHandler(handler));
		}
	}

	public void testGetZooKeeper() throws Exception {
		assertEquals(zookeeper, config.getZooKeeper());
	}

	public void testGetInterval() {
		assertEquals(interval, config.getInterval());
	}

	public void testGetAgentName() {
		assertNotNull(config.getAgentName());
	}
}
