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

package com.dsf.dbxtract.cdc.journal;

import com.dsf.dbxtract.cdc.ConfigurationException;
import com.dsf.dbxtract.cdc.Data;
import com.dsf.dbxtract.cdc.PublishException;
import com.dsf.dbxtract.cdc.Publisher;

/**
 * A simple handler
 * 
 * @author fabio de santi
 * @version 0.1
 */
public class SimpleJournalHandler implements JournalHandler {

	private String journal;
	private String query;
	private int batchSize;
	private Publisher publisher;
	private JournalStrategy strategy;

	public SimpleJournalHandler(String journalTable, String query, int batchSize, Publisher publisher,
			JournalStrategy strategy) throws ConfigurationException {
		if (journalTable == null || journalTable.isEmpty())
			throw new ConfigurationException("journal table name is required");
		if (query == null || query.isEmpty())
			throw new ConfigurationException("query is required");
		if (batchSize <= 0)
			throw new ConfigurationException("batch size must be a positive integer");
		if (publisher == null)
			throw new ConfigurationException("published is required");
		this.journal = journalTable;
		this.query = query;
		this.batchSize = batchSize;
		this.publisher = publisher;
		this.strategy = strategy == null ? JournalStrategy.WINDOW : strategy;
	}

	public String getJournalTable() {
		return journal;
	}

	public int getBatchSize() {
		return batchSize;
	}

	public String getTargetQuery() {
		return query;
	}

	public void publish(Data data) throws PublishException {
		publisher.publish(data);
	}

	@Override
	public JournalStrategy getStrategy() {
		return strategy;
	}
}
