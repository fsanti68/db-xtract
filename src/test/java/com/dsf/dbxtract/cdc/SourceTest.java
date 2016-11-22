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

import java.util.Arrays;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class SourceTest {

	private static Source src;

	@BeforeTest
	public void setUp() throws Exception {
		src = new Source("a", "c", "d", "u", "p", Arrays.asList("h1", "h2", "h3", "h4", "com.t.h5"));
	}

	@Test
	public void testSource() {
		Assert.assertNotNull(src);
	}

	@Test
	public void testGetName() {
		Assert.assertEquals("a", src.getName());
	}

	@Test
	public void testGetConnection() {
		Assert.assertEquals("c", src.getConnection());
	}

	@Test
	public void testGetDriver() {
		Assert.assertEquals("d", src.getDriver());
	}

	@Test
	public void testGetUser() {
		Assert.assertEquals("u", src.getUser());
	}

	@Test
	public void testGetPassword() {
		Assert.assertEquals("p", src.getPassword());
	}

	@Test
	public void testGetHandlers() {
		Assert.assertEquals(src.getHandlers().size(), 5);
		Assert.assertEquals(src.getHandlers().get(1), "h2");
	}
}
