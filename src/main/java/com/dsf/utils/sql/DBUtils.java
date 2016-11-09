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

public class DBUtils {

	private DBUtils() {
	}

	public static void close(ResultSet rs) {
		if (rs != null)
			try {
				rs.close();

			} catch (SQLException sqle) {
			}
	}

	public static void close(Statement stmt) {
		if (stmt != null)
			try {
				stmt.close();

			} catch (SQLException sqle) {
			}
	}

	public static void close(NamedParameterStatement stmt) {
		if (stmt != null)
			try {
				stmt.close();

			} catch (SQLException sqle) {
			}
	}

	public static void close(Connection c) {
		if (c != null)
			try {
				c.close();

			} catch (SQLException sqle) {
			}
	}
}
