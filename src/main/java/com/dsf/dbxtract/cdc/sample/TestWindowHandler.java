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

package com.dsf.dbxtract.cdc.sample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dsf.dbxtract.cdc.Data;
import com.dsf.dbxtract.cdc.PublishException;
import com.dsf.dbxtract.cdc.journal.JournalHandler;
import com.dsf.dbxtract.cdc.journal.JournalStrategy;

/**
 * <p>
 * Sample window handler (no changes are made to journal tables):
 * </p>
 * <ul>
 * <li>journal: j$test</li>
 * <li>batch size: 300 rows per batch</li>
 * <li>target query: select * from test where key1 = :key1 and key2 = :key2</li>
 * </ul>
 * 
 * @author fabio de santi
 *
 */
public class TestWindowHandler implements JournalHandler {

	private static Logger logger = LogManager.getLogger(TestWindowHandler.class.getName());

	@Override
	public String getJournalTable() {
		return "J$TEST";
	}

	@Override
	public int getBatchSize() {
		return 300;
	}

	/**
	 * Query parameters must be referenced just like in JPA's NamedQueries.
	 */
	@Override
	public String getTargetQuery() {
		return "SELECT * FROM test WHERE key1 = :key1 AND key2 = :key2";
	}

	/**
	 * Publish imported data to... (kafka, a file, a queue, another database,
	 * etc)
	 * 
	 * @param data
	 *            captured data object
	 */
	@Override
	public void publish(Data data) throws PublishException {
		logger.info("(window strategy) Data to Publish (columns/rows): " + data.getColumnNames().length + "/"
				+ data.getRows().size() + " rows");
	}

	@Override
	public JournalStrategy getStrategy() {
		return JournalStrategy.WINDOW;
	}
}
