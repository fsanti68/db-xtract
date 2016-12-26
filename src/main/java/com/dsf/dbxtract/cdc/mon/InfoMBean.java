package com.dsf.dbxtract.cdc.mon;

import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InvalidAttributeValueException;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.ReflectionException;

public interface InfoMBean extends DynamicMBean {

	String ATTR_INFO = "Info";

	Object getAttribute(String attrName) throws AttributeNotFoundException, MBeanException, ReflectionException;

	void setAttribute(Attribute attribute)
			throws AttributeNotFoundException, InvalidAttributeValueException, MBeanException, ReflectionException;

	AttributeList getAttributes(String[] attributeNames);

	AttributeList setAttributes(AttributeList attributes);

	Object invoke(String actionName, Object[] params, String[] signature) throws MBeanException, ReflectionException;

	MBeanInfo getMBeanInfo();
}