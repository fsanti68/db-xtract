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

package com.dsf.utils.sql;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Convenience methods for database connections.
 * 
 * @author fabio de santi
 *
 */
public class DBUtils {

	private static final Logger logger = LogManager.getLogger(DBUtils.class.getName());

	/**
	 * 
	 */
	private DBUtils() {
	}

	/**
	 * 
	 * @param rs
	 *            {@link ResultSet} object
	 */
	public static void close(ResultSet rs) {
		if (rs != null)
			try {
				rs.close();

			} catch (SQLException sqle) {
				logger.warn("closing resultset", sqle);
			}
	}

	/**
	 * 
	 * @param stmt
	 *            {@link Statement} object
	 */
	public static void close(Statement stmt) {
		if (stmt != null)
			try {
				stmt.close();

			} catch (SQLException sqle) {
				logger.warn("closing statement", sqle);
			}
	}

	/**
	 * 
	 * @param stmt
	 *            {@link NamedParameterStatement} object
	 */
	public static void close(NamedParameterStatement stmt) {
		if (stmt != null)
			try {
				stmt.close();

			} catch (SQLException sqle) {
				logger.warn("closing namedparameterstatement", sqle);
			}
	}

	/**
	 * 
	 * @param c
	 *            {@link Connection} object
	 */
	public static void close(Connection c) {
		if (c != null)
			try {
				c.close();

			} catch (SQLException sqle) {
				logger.warn("closing connection", sqle);
			}
	}
}
