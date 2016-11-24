package com.dsf.dbxtract.cdc;

/**
 * 
 * @author fabio de santi
 *
 */
public class DataColumnMatchException extends Exception {

	private static final long serialVersionUID = 1L;

	/**
	 * 
	 * @param message
	 */
	public DataColumnMatchException(String message) {
		super(message);
	}

	/**
	 * 
	 * @param cause
	 */
	public DataColumnMatchException(Throwable cause) {
		super(cause);
	}

	/**
	 * 
	 * @param message
	 * @param cause
	 */
	public DataColumnMatchException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * 
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public DataColumnMatchException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
