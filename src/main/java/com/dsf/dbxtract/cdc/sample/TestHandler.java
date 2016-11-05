package com.dsf.dbxtract.cdc.sample;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.dsf.dbxtract.cdc.Data;
import com.dsf.dbxtract.cdc.Handler;

/**
 * <p>
 * Sample handler:
 * </p>
 * <ul>
 * <li>journal: j$test</li>
 * <li>batch size: 200 rows per batch</li>
 * <li>target query: select * from test where key1 = :key1 and key2 = :key2</li>
 * </ul>
 * 
 * @author fabio de santi
 *
 */
public class TestHandler implements Handler {

	private static Logger logger = LogManager.getLogger(TestHandler.class.getName());

	public String getJournalTable() {
		return "J$TEST";
	}

	public int getBatchSize() {
		return 200;
	}

	/**
	 * Query parameters must be referenced just like in JPA's NamedQueries.
	 */
	public String getTargetQuery() {
		return "SELECT * FROM TEST WHERE KEY1 = :key1 AND KEY2 = :key2";
	}

	public void publish(Data data) throws Exception {
		logger.info("Data to Publish (columns/rows): " + data.getColumnNames().length + "/" + data.getRows().size()
				+ " rows");

		// TODO: send data somewhere
		System.out.println("[TestHandler.publish] batch size = " + data.getRows().size());
	}
}
