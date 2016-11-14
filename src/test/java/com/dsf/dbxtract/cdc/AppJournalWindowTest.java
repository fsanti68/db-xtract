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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.codehaus.jackson.map.ObjectMapper;

import com.dsf.dbxtract.cdc.mon.Monitor;

import junit.framework.TestCase;

/**
 * Unit test for simple App.
 */
public class AppJournalWindowTest extends TestCase {

	private int TEST_SIZE = 1000;

	private Config config;
	private CuratorFramework client;

	@Override
	protected void setUp() throws Exception {

		Sources sources = new Sources();
		sources.setInterval(1000L);
		sources.getSources()
				.add(new Source("test", "jdbc:mysql://localhost:3306/smartboard", "org.gjt.mm.mysql.Driver", "root",
						"mysql", Arrays.asList("com.dsf.dbxtract.cdc.sample.TestWindowHandler",
								"com.dsf.dbxtract.cdc.sample.TestWindowHandler")));

		RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
		client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
		client.start();
		ObjectMapper mapper = new ObjectMapper();
		byte[] value = mapper.writeValueAsBytes(sources);
		client.setData().forPath(App.BASEPREFIX + "config", value);
		
		config = new Config(getClass().getClassLoader()
				.getResourceAsStream("com/dsf/dbxtract/cdc/config-app-journal-window.properties"));
		
		PropertyConfigurator
				.configure(ClassLoader.getSystemResource("com/dsf/dbxtract/cdc/config-app-journal-window.properties"));

		super.setUp();
	}

	/**
	 * Rigourous Test :-)
	 */
	public void testApp() throws Exception {

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
		PreparedStatement ps = conn
				.prepareStatement("insert into test (key1,key2,attr1,attr2,attr3) values (?,?,?,?,?)");
		for (int i = 0; i < TEST_SIZE; i++) {
			if ((i % 100) == 0) {
				ps.executeBatch();
			}
			ps.setInt(1, 5000 + i);
			ps.setInt(2, 6000 + i);
			ps.setInt(3, i);
			ps.setInt(4, (int) Math.random() * 500);
			ps.setInt(5, (int) Math.random() * 500);
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
			ps.setInt(1, 5000 + i);
			ps.setInt(2, 6000 + i);
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

		// Clean previous statistics and states
		if (client.checkExists().forPath("/dbxtract/cdc/statistics") != null) {
			List<String> children = client.getChildren().forPath("/dbxtract/cdc/statistics");
			for (String k : children)
				client.delete().forPath("/dbxtract/cdc/statistics/" + k);
			client.delete().forPath("/dbxtract/cdc/statistics");
		}
		String zkKey = "/dbxtract/cdc/" + source.getName() + "/J$TEST/lastWindowId";
		if (client.checkExists().forPath(zkKey) != null)
			client.delete().forPath(zkKey);

		// start app
		App app = new App();
		app.setConfig(config);
		System.out.println(config.toString());
		app.start();

		// starts monitor
		new Monitor(9123, config);

		while (true) {
			Thread.sleep(1000);

			try {
				Long lastWindowId = Long.parseLong(new String(client.getData().forPath(zkKey)));
				if (maxWindowId.longValue() == lastWindowId.longValue())
					break;

			} catch (NoNodeException nne) {
				System.out.println("ZooKeeper - no node exception :: " + zkKey);
			}
		}
		conn.close();
		ds.close();
	}

	public void testInfoStatistics() throws IOException {

		URL obj = new URL("http://localhost:9123/info");
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();

		// optional default is GET
		con.setRequestMethod("GET");

		// add request header
		con.setRequestProperty("User-Agent", "Mozilla/5.0");

		int responseCode = con.getResponseCode();
		assertEquals(responseCode, 200);

		BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
		String inputLine;
		StringBuffer response = new StringBuffer();
		while ((inputLine = in.readLine()) != null) {
			response.append(inputLine);
		}
		in.close();
		String s = response.toString();
		System.out.println(s);

		assertTrue("unexpected response: " + s, s.startsWith("{\"handlers\":[{\"name\":"));
		assertTrue("unexpected response: " + s, s.contains("\"readCount\":" + TEST_SIZE + "}"));
	}

	@Override
	protected void tearDown() throws Exception {

		client.close();
		super.tearDown();
	}
}
