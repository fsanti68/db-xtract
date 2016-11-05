package com.dsf.dbxtract.cdc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.apache.commons.dbcp2.BasicDataSource;

import junit.framework.TestCase;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {

	/**
	 * Rigourous Test :-)
	 */
	public void testApp() throws Exception {

		final Config config = new Config(
				getClass().getClassLoader().getResourceAsStream("com/dsf/dbxtract/cdc/config-apptest.properties"));

		BasicDataSource ds = new BasicDataSource();
		Source source = config.getDataSources().get(0);
		ds.setDriverClassName(source.getDriver());
		ds.setUsername(source.getUser());
		ds.setPassword(source.getPassword());
		ds.setUrl(source.getConnection());

		// prepara os dados
		Connection conn = ds.getConnection();
		// Map<String, Integer> updated = new HashMap<String, Integer>();

		conn.createStatement().execute("truncate table test");
		conn.createStatement().execute("truncate table j$test");

		// Carrega os dados de origem
		PreparedStatement ps = conn
				.prepareStatement("insert into test (key1,key2,attr1,attr2,attr3) values (?,?,?,?,?)");
		for (int i = 0; i < 1000; i++) {
			if ((i % 100) == 0) {
				ps.executeBatch();
			}
			ps.setInt(1, 5000 + i);
			ps.setInt(2, 6000 + i);
			ps.setInt(3, i);
			ps.setInt(4, (int) Math.random() * 500);
			ps.setInt(5, (int) Math.random() * 500);
			ps.addBatch();
			// updated.put((3000 + i) + "-" + (4000 + i), 0);
		}
		ps.executeBatch();
		ps.close();

		App app = new App();
		app.setConfig(config);
		app.start();

		// Popula as tabelas de journal
		ps = conn.prepareStatement("insert into j$test (key1,key2) values (?,?)");
		for (int i = 0; i < 1000; i++) {
			if ((i % 500) == 0) {
				ps.executeBatch();
			}
			ps.setInt(1, 5000 + i);
			ps.setInt(2, 6000 + i);
			ps.addBatch();
		}
		ps.executeBatch();
		ps.close();

		while (true) {
			Thread.sleep(1000);

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
}
