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
 * The class ConfigurationException represent one of the following conditions:
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
	 * Construcs a new exception with specified detail message.
	 * 
	 * @param message
	 *            exception's message
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
	 * Constructs a new exception with the specified detail message and cause.
	 * 
	 * @param message
	 *            exception's message
	 * @param cause
	 *            causing {@link Throwable} object
	 */
	public ConfigurationException(String message, Throwable cause) {
		super(message, cause);
	}

	/**
	 * Constructs a new exception with the specified detail message, cause,
	 * suppression enabled or disabled, and writable stack trace enabled or
	 * disabled.
	 * 
	 * @param message
	 *            exception's message
	 * @param cause
	 *            causing {@link Throwable} object
	 * @param enableSuppression
	 *            whether or not suppression is enabled or disabled
	 * @param writableStackTrace
	 *            whether or not the stack trace should be writable
	 */
	public ConfigurationException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}
}
