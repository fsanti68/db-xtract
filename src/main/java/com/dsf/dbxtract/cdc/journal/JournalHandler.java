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

import com.dsf.dbxtract.cdc.Data;

/**
 * Interface to be implemented by capture executors.
 * 
 * @author fabio de santi
 * @version 0.1
 */
public interface JournalHandler {

	/**
	 * 
	 * @return journal table's name
	 */
	public String getJournalTable();

	/**
	 * 
	 * @return number of rows to be imported for each execution
	 */
	public int getBatchSize();

	/**
	 * <p>
	 * The query to retrieve changed data can references columns from journal
	 * table as named parameters:
	 * </p>
	 * <code>select * from mytable where pk = :pk</code>
	 * 
	 * @return data retrieval query
	 */
	public String getTargetQuery();

	/**
	 * Publishes captured data (like writing a file or publishing to a kafka
	 * queue).
	 */
	public void publish(Data data) throws Exception;

	/**
	 * Establish the journal strategy:
	 * <ul>
	 * <li>DELETE - removes imported rows from journal table</li>
	 * <li>WINDOW - memorizes 
	 * </ul>
	 * 
	 * @return
	 */
	public JournalStrategy getStrategy();
}
