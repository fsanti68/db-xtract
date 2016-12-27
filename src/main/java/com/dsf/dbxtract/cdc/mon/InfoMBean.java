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

import java.util.ArrayList;
import java.util.List;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanNotificationInfo;
import javax.management.ReflectionException;
import javax.management.RuntimeOperationsException;
import javax.management.openmbean.CompositeDataSupport;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.OpenMBeanAttributeInfoSupport;
import javax.management.openmbean.OpenMBeanConstructorInfoSupport;
import javax.management.openmbean.OpenMBeanInfoSupport;
import javax.management.openmbean.OpenMBeanOperationInfoSupport;
import javax.management.openmbean.OpenType;
import javax.management.openmbean.SimpleType;
import javax.management.openmbean.TabularDataSupport;
import javax.management.openmbean.TabularType;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.dsf.dbxtract.cdc.Config;
import com.dsf.dbxtract.cdc.ConfigurationException;
import com.dsf.dbxtract.cdc.mon.Statistics.StatEntry;

/**
 * Provides basic statistics from CDC Handlers, like last run time, last capture
 * time and total rows captured during current session.
 * 
 * @author fabio de santi
 * @version 0.2
 */
public class InfoMBean implements DynamicMBean {

	private static Logger logger = LogManager.getLogger(InfoMBean.class.getName());

	public static final String ATTR_INFO = "Info";

	private static String[] itemNames = new String[] { "handler", "lastSeek", "lastRead", "readCount" };
	private static String[] itemDescriptions = new String[] { "Handler name", "Last time a change was checked",
			"Last time a new data was captured", "Total number of rows captured" };
	@SuppressWarnings("rawtypes")
	private static OpenType[] itemTypes = { SimpleType.STRING, SimpleType.STRING, SimpleType.STRING, SimpleType.LONG };
	private static String[] indexNames = new String[] { itemNames[0] };
	private static CompositeType pageType = null;
	private static TabularType pageTabularType = null;
	private TabularDataSupport pageData;
	private OpenMBeanInfoSupport openMBeanInfo;

	private ObjectMapper mapper = null;
	private Config config = null;

	static {
		try {
			pageType = new CompositeType("stat", "Statistics", itemNames, itemDescriptions, itemTypes);
			pageTabularType = new TabularType("stats", "Handler's statistics", pageType, indexNames);

		} catch (OpenDataException e) {
			logger.error(e);
		}
	}

	/**
	 * Constructor for /info request's object.
	 * 
	 * @param config
	 *            configuration object
	 */
	public InfoMBean(Config config) {
		this.config = config;

		OpenMBeanAttributeInfoSupport[] attributes = new OpenMBeanAttributeInfoSupport[] {
				new OpenMBeanAttributeInfoSupport(ATTR_INFO, "Handlers statistics", pageTabularType, true, false,
						false) };
		openMBeanInfo = new OpenMBeanInfoSupport(InfoMBean.class.getName(), "Handler Statistics OMB", attributes,
				new OpenMBeanConstructorInfoSupport[0], new OpenMBeanOperationInfoSupport[0],
				new MBeanNotificationInfo[0]);
		pageData = new TabularDataSupport(pageTabularType);
	}

	@Override
	public Object getAttribute(String attrName) throws AttributeNotFoundException, MBeanException, ReflectionException {
		if (attrName.equals(ATTR_INFO)) {
			try {
				return refreshPageData();

			} catch (ConfigurationException e) {
				logger.error("getAttribute('" + ATTR_INFO + "')", e);
			}
		}
		return null;
	}

	private Object refreshPageData() throws ConfigurationException {

		List<StatEntry> list = getInfo();
		pageData.clear();
		for (StatEntry entry : list) {
			Object[] itemValues = { entry.getName(), entry.getLastSeek(), entry.getLastRead(), entry.getReadCount() };
			try {
				pageData.put(new CompositeDataSupport(pageType, itemNames, itemValues));
			} catch (OpenDataException e) {
				logger.error(e);
			}
		}
		return pageData.clone();
	}

	@Override
	public void setAttribute(Attribute attribute)
			throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException {
		throw new AttributeNotFoundException("No attribute can be set in this MBean");
	}

	@Override
	public AttributeList getAttributes(String[] attributeNames) {
		if (attributeNames == null) {
			throw new RuntimeOperationsException(new IllegalArgumentException("attributeNames[] cannot be null"),
					"Cannot call getAttributes with null attribute names");
		}
		AttributeList resultList = new AttributeList();
		if (attributeNames.length == 0)
			return resultList;
		for (int i = 0; i < attributeNames.length; i++) {
			try {
				Object value = getAttribute(attributeNames[i]);
				resultList.add(new Attribute(attributeNames[i], value));
			} catch (Exception e) {
				logger.warn(e);
			}
		}
		return (resultList);
	}

	@Override
	public AttributeList setAttributes(AttributeList attributes) {
		return new AttributeList();
	}

	@Override
	public Object invoke(String actionName, Object[] params, String[] signature)
			throws MBeanException, ReflectionException {

		return null;
	}

	@Override
	public MBeanInfo getMBeanInfo() {
		return openMBeanInfo;
	}

	private List<StatEntry> getInfo() throws ConfigurationException {

		if (mapper == null)
			mapper = new ObjectMapper();

		List<StatEntry> list = new ArrayList<>();
		try {
			RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
			CuratorFramework client = CuratorFrameworkFactory.newClient(config.getZooKeeper(), retryPolicy);
			client.start();
			List<String> handlers = client.getChildren().forPath(Statistics.ZOOPATH);
			for (String handler : handlers) {
				byte[] b = client.getData().forPath(Statistics.ZOOPATH + "/" + handler);
				StatEntry entry = mapper.readValue(b, Statistics.StatEntry.class);
				list.add(entry);
			}
			client.close();

			return list;

		} catch (Exception e) {
			throw new ConfigurationException("failed to retrieve zk statistics at " + Statistics.ZOOPATH, e);
		}
	}
}
