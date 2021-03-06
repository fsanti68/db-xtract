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

package com.dsf.dbxtract.cdc.mon;

import java.io.IOException;
import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.dsf.dbxtract.cdc.Config;

/**
 * Set's up a HTTP listener for statistics and administration tasks.
 * 
 * @author fabio de santi
 * @version 0.2
 */
public class Monitor {

	private static final Logger logger = LogManager.getLogger(Monitor.class.getName());

	private static Monitor instance;

	/**
	 * Start's monitor JMX.
	 * 
	 * @param config
	 *            {@link Config} object
	 * @throws IOException
	 */
	private Monitor(Config config) throws IOException {

		// starts JMX
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		try {
			ObjectName name = new ObjectName("com.dsf.dbxtract:type=InfoMBean");
			mbs.registerMBean(new InfoMBean(config), name);

		} catch (Exception e) {
			logger.error("failed to initialized mbeans", e);
		}
	}

	/**
	 * 
	 * @param config
	 *            {@link Config} object
	 * @return instance of {@link Monitor} object
	 * @throws IOException
	 *             on configuration file access
	 */
	public static Monitor getInstance(Config config) throws IOException {
		if (instance == null) {
			instance = new Monitor(config);
		}
		return instance;
	}

	/**
	 * 
	 * @return instance of {@link Monitor} object
	 * @throws MonitorNotInitializedException
	 *             when called before Monitor initialization (see
	 *             {@link Monitor#getInstance(Config)})
	 */
	public static Monitor getInstance() throws MonitorNotInitializedException {
		if (instance == null)
			throw new MonitorNotInitializedException("Monitor not initialized.");

		return instance;
	}
}
