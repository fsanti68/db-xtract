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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.TabularDataSupport;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import com.dsf.dbxtract.cdc.journal.JournalStrategy;
import com.dsf.dbxtract.cdc.mon.InfoMBean;
import com.dsf.dbxtract.cdc.mon.Monitor;
import com.dsf.dbxtract.cdc.sample.TestWindowHandler;

/**
 * Unit test for simple App.
 */
@Test(singleThreaded = true)
public class AppJournalWindowTest {

	private Logger logger = LogManager.getLogger(AppJournalWindowTest.class.getName());

	private static final String PROPERTY_RESOURCE = "com/dsf/dbxtract/cdc/config-app-journal-window.properties";

	private static final String ZK_STATISTICS_PATH = "/dbxtract/cdc/statistics";

	private int TEST_SIZE = 300;

	private CuratorFramework client;
	private App app;
	private String configFile;

	@Test
	public void setUp() throws Exception {

		configFile = ClassLoader.getSystemResource(PROPERTY_RESOURCE).getFile();

		logger.info("Testing Journal-based CDC with window strategy");

		// Add configuration to zk
		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
		client.start();

		// Clean previous statistics and states
		if (client.checkExists().forPath(ZK_STATISTICS_PATH) != null) {
			List<String> children = client.getChildren().forPath(ZK_STATISTICS_PATH);
			for (String k : children)
				client.delete().forPath(ZK_STATISTICS_PATH + "/" + k);
			client.delete().forPath(ZK_STATISTICS_PATH);
		}
	}

	/**
	 * Rigourous Test :-)
	 * 
	 * @throws Exception
	 *             in case of any error
	 */
	@Test(dependsOnMethods = "setUp", timeOut = 120000)
	public void testAppWithJournalWindow() throws Exception {

		final Config config = new Config(configFile);

		BasicDataSource ds = new BasicDataSource();
		Source source = config.getDataSources().getSources().get(0);
		ds.setDriverClassName(source.getDriver());
		ds.setUsername(source.getUser());
		ds.setPassword(source.getPassword());
		ds.setUrl(source.getConnection());

		// prepara os dados
		Connection conn = ds.getConnection();

		conn.createStatement().execute("truncate table test");
		conn.createStatement().execute("truncate table j$test");

		// Carrega os dados de origem
		PreparedStatement ps = conn.prepareStatement("insert into test (key1,key2,data) values (?,?,?)");
		for (int i = 0; i < TEST_SIZE; i++) {
			if ((i % 100) == 0) {
				ps.executeBatch();
			}
			ps.setInt(1, i);
			ps.setInt(2, i);
			ps.setInt(3, (int) Math.random() * 500);
			ps.addBatch();
		}
		ps.executeBatch();
		ps.close();

		// Popula as tabelas de journal
		ps = conn.prepareStatement("insert into j$test (key1,key2) values (?,?)");
		for (int i = 0; i < TEST_SIZE; i++) {
			if ((i % 500) == 0) {
				ps.executeBatch();
			}
			ps.setInt(1, i);
			ps.setInt(2, i);
			ps.addBatch();
		}
		ps.executeBatch();
		ps.close();

		Long maxWindowId = 0L;
		ResultSet rs = conn.createStatement().executeQuery("select max(window_id) from j$test");
		if (rs.next()) {
			maxWindowId = rs.getLong(1);
			System.out.println("maximum window_id loaded: " + maxWindowId);
		}
		rs.close();
		conn.close();
		ds.close();

		// Clear any previous test
		String zkKey = "/dbxtract/cdc/" + source.getName() + "/J$TEST/lastWindowId";
		if (client.checkExists().forPath(zkKey) != null)
			client.delete().forPath(zkKey);

		// starts monitor
		Monitor.getInstance(config);

		// start app
		app = new App(config);
		System.out.println(config.toString());
		app.start();

		Assert.assertEquals(config.getHandlers().iterator().next().getStrategy(), JournalStrategy.WINDOW);

		while (true) {
			TimeUnit.MILLISECONDS.sleep(500);

			try {
				Long lastWindowId = Long.parseLong(new String(client.getData().forPath(zkKey)));
				System.out.println("lastWindowId = " + lastWindowId);
				if (maxWindowId.longValue() == lastWindowId.longValue()) {
					System.out.println("expected window_id reached");
					break;
				}

			} catch (NoNodeException nne) {
				System.out.println("ZooKeeper - no node exception :: " + zkKey);
			}
		}
	}

	@Test(dependsOnMethods = { "testAppWithJournalWindow" })
	public void testInfoStatistics() throws Exception {

		JMXServiceURL url = new JMXServiceURL("service:jmx:rmi:///jndi/rmi://localhost:5000/jmxrmi");
		JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
		MBeanServerConnection mbsc = jmxc.getMBeanServerConnection();

		ObjectName mbeanName = new ObjectName("com.dsf.dbxtract:type=InfoMBean");

		TabularDataSupport info = (TabularDataSupport) mbsc.getAttribute(mbeanName, InfoMBean.ATTR_INFO);
		Collection<?> list = info.values();
		boolean hasHandlerEntry = false;
		for (Iterator<?> it = list.iterator(); it.hasNext();) {
			CompositeDataSupport entry = (CompositeDataSupport) it.next();

			if (entry.get("handler").equals(TestWindowHandler.class.getName())) {
				assert (((Long) entry.get("readCount")).longValue() == TEST_SIZE);
				hasHandlerEntry = true;
			}
		}
		assert (hasHandlerEntry);
		jmxc.close();
	}

	@AfterTest
	public void tearDown() throws Exception {

		client.close();
		if (app != null)
			app.stop();
	}
}
