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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

import com.dsf.dbxtract.cdc.journal.JournalHandler;

/**
 * This class is used by {@link JournalHandler} to receive captured data and
 * represents a list of retrieved rows.
 * 
 * @author fabio de santi
 *
 */
public class Data {

	private String[] columnNames;
	private List<Object[]> rows = new LinkedList<>();

	/**
	 * Constructs a new object with a given list of column names. No data is
	 * included at this time.
	 * 
	 * @param columnNames
	 *            list of column names
	 */
	public Data(String[] columnNames) {
		this.columnNames = columnNames;
	}

	/**
	 * Add new data from a retrieved row (ResultSet cursor)
	 * 
	 * @param rs
	 *            {@link ResultSet} object
	 * @throws SQLException
	 *             any data retrieval failure
	 */
	public void append(ResultSet rs) throws SQLException {

		Object[] values = new Object[columnNames.length];
		for (int i = 0; i < columnNames.length; i++) {
			values[i] = rs.getObject(columnNames[i]);
		}
		rows.add(values);
	}

	/**
	 * Add data from an array of objects. This array must have the same size of
	 * the {@link #getColumnNames()}.
	 * 
	 * @param values
	 *            an array of objects (column's data)
	 * @throws DataColumnMatchException
	 *             values array size differs from column names array size
	 */
	public void append(Object[] values) throws DataColumnMatchException {

		if (values == null)
			return;

		if (values.length != columnNames.length)
			throw new DataColumnMatchException("data array size differs from column's count");

		rows.add(values);
	}

	/**
	 * Retrieve the column names.
	 * 
	 * @return array of column names
	 */
	public String[] getColumnNames() {
		return columnNames;
	}

	/**
	 * Retrieve the list of data rows.
	 * 
	 * @return list of retrieved rows
	 */
	public List<Object[]> getRows() {
		return rows;
	}
}
