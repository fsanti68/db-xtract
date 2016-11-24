package com.dsf.dbxtract.cdc;

public class DataColumnMatchException extends Exception {

	private static final long serialVersionUID = 1L;

	public DataColumnMatchException() {
	}

	public DataColumnMatchException(String message) {
		super(message);
	}

	public DataColumnMatchException(Throwable cause) {
		super(cause);
	}

	public DataColumnMatchException(String message, Throwable cause) {
		super(message, cause);
	}

	public DataColumnMatchException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
