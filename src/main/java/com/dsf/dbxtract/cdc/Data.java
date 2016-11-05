package com.dsf.dbxtract.cdc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.List;

/**
 * 
 * @author fabio de santi
 *
 */
public class Data {

	private String[] columnNames;
	private List<Object[]> rows = new LinkedList<Object[]>();

	protected Data(String[] columnNames) {
		this.columnNames = columnNames;
	}

	public void append(ResultSet rs) throws SQLException {

		Object[] values = new Object[columnNames.length];
		for (int i = 0; i < columnNames.length; i++) {
			values[i] = rs.getObject(columnNames[i]);
		}
		rows.add(values);
	}

	public void append(Object[] values) throws Exception {

		if (values == null)
			return;

		if (values.length != columnNames.length)
			throw new Exception("data array size differs from column's count");

		rows.add(values);
	}

	public String[] getColumnNames() {
		return columnNames;
	}
	
	public List<Object[]> getRows() {
		return rows;
	}
}
