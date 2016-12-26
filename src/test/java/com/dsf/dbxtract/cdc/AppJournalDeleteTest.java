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
import java.util.concurrent.TimeUnit;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.dsf.dbxtract.cdc.journal.JournalStrategy;

/**
 * Unit test for simple App.
 */
@Test(singleThreaded = true)
public class AppJournalDeleteTest {

	private static final Logger logger = LogManager.getLogger(AppJournalDeleteTest.class.getName());

	private static final String PROPERTY_RESOURCE = "com/dsf/dbxtract/cdc/config-app-journal-delete.properties";

	private int TEST_SIZE = 300;
	private App app;
	private String configFile;

	@BeforeTest
	public void setUp() throws Exception {

		configFile = ClassLoader.getSystemResource(PROPERTY_RESOURCE).getFile();
		PropertyConfigurator.configure(configFile);

		logger.info("Testing Journal-based CDC with delete strategy");
	}

	/**
	 * Rigourous Test :-)
	 * 
	 * @throws Exception
	 *             in case of any error
	 */
	@Test(timeOut = 60000)
	public void testAppWithJournalDelete() throws Exception {

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

		app = new App(config);
		app.start();

		Assert.assertEquals(config.getHandlers().iterator().next().getStrategy(), JournalStrategy.DELETE);

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

		while (true) {
			TimeUnit.MILLISECONDS.sleep(500);

			ResultSet rs = conn.createStatement().executeQuery("select count(*) from j$test");
			if (rs.next()) {
				long count = rs.getLong(1);
				System.out.println("remaining journal rows: " + count);
				rs.close();
				if (count == 0L)
					break;
			}
		}
		conn.close();
		ds.close();
	}

	@AfterTest
	public void tearDown() {
		if (app != null)
			app.stop();
	}
}
