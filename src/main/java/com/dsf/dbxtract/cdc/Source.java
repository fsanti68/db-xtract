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
