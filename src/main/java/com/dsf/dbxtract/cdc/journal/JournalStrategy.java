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

/**
 * There are two ways for control what has been already captured:
 * <ul>
 * <li>DELETE = imported data is removed from journal table</li>
 * <li>WINDOW = DB-Xtract keeps record of last window id, without changing
 * journal data</li>
 * </ul>
 * 
 * @author fabio de santi
 *
 */
public enum JournalStrategy {
	DELETE(1), WINDOW(0);

	private int strategy;

	private JournalStrategy(int id) {
		this.strategy = id;
	}

	/**
	 * Retrieve a strategy based on its identifier
	 * 
	 * @param id
	 *            object's identifier (enum key).
	 * @return get the strategy by an id
	 */
	public JournalStrategy getById(int id) {
		for (JournalStrategy js : values()) {
			if (js.strategy == id)
				return js;
		}
		return null;
	}
}
