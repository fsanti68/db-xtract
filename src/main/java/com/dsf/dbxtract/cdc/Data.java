package com.dsf.dbxtract.cdc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * This class is used by {@link Handler} to receive captured data.
 * 
 * @author fabio de santi
 *
 */
public class Data {

	private String[] columnNames;
	private List<Object[]> rows = new LinkedList<Object[]>();

	/**
	 * 
	 * @param columnNames
	 *            list of column names
	 */
	protected Data(String[] columnNames) {
		this.columnNames = columnNames;
	}

	/**
	 * Add data from a retrieved row (ResultSet cursor)
	 * 
	 * @param rs
	 * @throws SQLException
	 */
	protected void append(ResultSet rs) throws SQLException {

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
	 * @throws Exception
	 */
	public void append(Object[] values) throws Exception {

		if (values == null)
			return;

		if (values.length != columnNames.length)
			throw new Exception("data array size differs from column's count");

		rows.add(values);
	}

	/**
	 * 
	 * @return array of column names
	 */
	public String[] getColumnNames() {
		return columnNames;
	}

	/**
	 * 
	 * @return list of retrieved rows
	 */
	public List<Object[]> getRows() {
		return rows;
	}
}
