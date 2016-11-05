package com.dsf.dbxtract.cdc;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.commons.dbcp2.BasicDataSource;

import com.dsf.dbxtract.cdc.App;
import com.dsf.dbxtract.cdc.Config;
import com.dsf.dbxtract.cdc.Source;

import junit.framework.TestCase;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {

	/**
	 * Rigourous Test :-)
	 */
	public void testApp() throws Exception {

		final Config config = new Config("/Users/fabio/Public/PromonLogicalis/Monitor Online/config/config.properties");

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

		// Cria duas threads para ter alguma concorrência
		Runnable test = new Runnable() {

			public void run() {
				App app = new App();
				app.setConfig(config);
				try {
					app.start();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};
		Thread t1 = new Thread(test);
		Thread t2 = new Thread(test);
		t1.start();
		t2.start();

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
		conn.close();
		ds.close();

		Thread.sleep(2000);

		t1.interrupt();
		t2.interrupt();
	}
}
