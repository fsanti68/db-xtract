package com.dsf.dbxtract.cdc;

/**
 * Configuration exceptions:
 * <ul>
 * <li>config properties not found</li>
 * <li>required parameter</li>
 * <li>zookeeper's connection or config repository failure</li>
 * </ul>
 * 
 * @author fabio de santi
 *
 */
public class ConfigurationException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * 
	 * @param message
	 */
	public ConfigurationException(String message) {
		super(message);
	}

	/**
	 * 
	 * @param cause
	 */
	public ConfigurationException(Throwable cause) {
		super(cause);
	}

	/**
	 * 
	 * @param message
	 * @param cause
	 */
	public ConfigurationException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * 
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public ConfigurationException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
