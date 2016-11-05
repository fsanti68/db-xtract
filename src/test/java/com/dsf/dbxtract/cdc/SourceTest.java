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
