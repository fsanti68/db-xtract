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
	 * @param name nome que representa a fonte de dados
	 * @param connection string de conexão
	 * @param driver classe Java que implementa o JDBC
	 * @param user usuário na base de dados
	 * @param password senha do usuário
	 * @param handlers lista separada por vírgulas das classes que implementam a interface {@link Handler} 
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
		return "Source [name=" + name + ", connection=" + connection + ", driver=" + driver + ", user=" + user
				+ ", password=" + password + "]";
	}
}
