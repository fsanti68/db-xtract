package com.dsf.dbxtract.cdc;

import junit.framework.Test;
import junit.framework.TestSuite;

public class AllTests {

	public static Test suite() {
		TestSuite suite = new TestSuite(AllTests.class.getName());
		//$JUnit-BEGIN$
		suite.addTestSuite(ConfigTest.class);
		suite.addTestSuite(DataTest.class);
		suite.addTestSuite(AppTest.class);
		suite.addTestSuite(SourceTest.class);
		//$JUnit-END$
		return suite;
	}
}
