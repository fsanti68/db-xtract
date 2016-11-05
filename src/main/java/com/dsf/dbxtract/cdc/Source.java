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
 * Representa um banco de dados com tabelas a serem capturadas.
 * 
 * @author fabio de santi
 * @version 0.1
 */
public class Source {

	private String name;
	private String connection;
	private String driver;
	private String user;
	private String password;
	private String handlers;

	/**
	 * 
	 * @param name
	 *            data source arbitrary name
	 * @param connection
	 *            database connection string
	 * @param driver
	 *            jdbc driver full class name
	 * @param user
	 *            database user
	 * @param password
	 *            user's password
	 * @param handlers
	 *            comma-delimited class names implementing the interface
	 *            {@link Handler}
	 */
	public Source(String name, String connection, String driver, String user, String password, String handlers) {
		this.name = name;
		this.connection = connection;
		this.driver = driver;
		this.user = user;
		this.password = password;
		this.handlers = handlers;
	}

	public String getName() {
		return name;
	}

	public String getConnection() {
		return connection;
	}

	public String getDriver() {
		return driver;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public String getHandlers() {
		return handlers;
	}

	@Override
	public String toString() {
		return "Source [name=" + name + ", connection=" + connection + ", driver=" + driver + ", user=" + user + "/"
				+ password + "]";
	}
}
