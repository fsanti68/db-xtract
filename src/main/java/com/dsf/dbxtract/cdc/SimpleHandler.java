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

/**
 * A simple handler
 * 
 * @author fabio de santi
 * @version 0.1
 */
public class SimpleHandler implements Handler {

	private String journal;
	private String query;
	private Publisher publisher;

	public SimpleHandler(String journalTable, String query, Publisher publisher) {
		this.journal = journalTable;
		this.query = query;
		this.publisher = publisher;
	}

	public String getJournalTable() {
		return journal;
	}

	public int getBatchSize() {
		return 200;
	}

	public String getTargetQuery() {
		return query;
	}

	public void publish(Data data) throws Exception {
		publisher.publish(data);
	}
}
