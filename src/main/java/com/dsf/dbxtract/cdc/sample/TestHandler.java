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

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.dsf.dbxtract.cdc.Data;
import com.dsf.dbxtract.cdc.PublishException;
import com.dsf.dbxtract.cdc.journal.JournalHandler;
import com.dsf.dbxtract.cdc.journal.JournalStrategy;

/**
 * <p>
 * Sample handler (after import, journal entries are removed):
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
public class TestHandler implements JournalHandler {

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

	/**
	 * Publish imported data to... (kafka, a file, a queue, another database,
	 * etc)
	 * 
	 * @param data
	 */
	public void publish(Data data) throws PublishException {
		logger.info("(delete strategy) Data to Publish (columns/rows): " + data.getColumnNames().length + "/" + data.getRows().size()
				+ " rows");
	}

	@Override
	public JournalStrategy getStrategy() {
		return JournalStrategy.DELETE;
	}
}
