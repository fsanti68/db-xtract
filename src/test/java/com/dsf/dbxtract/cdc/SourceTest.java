package com.dsf.dbxtract.cdc;

import junit.framework.TestCase;

public class SourceTest extends TestCase {

	private static Source src = new Source("a", "c", "d", "u", "p", "h1,h2,h3, h4 ,com.t.h5");

	public void testSource() {
		assertNotNull(src);
	}

	public void testGetName() {
		assertEquals("a", src.getName());
	}

	public void testGetConnection() {
		assertEquals("c", src.getConnection());
	}

	public void testGetDriver() {
		assertEquals("d", src.getDriver());
	}

	public void testGetUser() {
		assertEquals("u", src.getUser());
	}

	public void testGetPassword() {
		assertEquals("p", src.getPassword());
	}

	public void testGetHandlers() {
		assertEquals("h1,h2,h3, h4 ,com.t.h5", src.getHandlers());
	}

}
