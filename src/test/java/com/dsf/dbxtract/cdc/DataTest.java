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

import java.util.List;

import junit.framework.TestCase;

public class DataTest extends TestCase {

	public void testData() throws Exception {
		Data data = new Data(new String[] { "f1", "f2" });
		for (int i = 0; i < 5; i++)
			data.append(new Object[] { "1" + i, "2" + i });
		assertEquals(5, data.getRows().size());
		assertEquals("f2", data.getColumnNames()[1]);
	}

	public void testAppendResultSet() {
		// TODO
	}

	public void testAppendObjectArray() throws Exception {
		Data data = new Data(new String[] { "f1", "f2" });
		data.append(new Object[] { "1", "2" });
		try {
			data.append(new Object[] { "3", "4", "5" });
			fail("cannot accept more data than declared columns");
		} catch (Exception e) {
		}
		try {
			data.append(new Object[] { "6" });
			fail("cannot accept less data than declared columns");
		} catch (Exception e) {
		}
	}

	public void testGetColumnNames() {
		Data data = new Data(new String[] { "f1", "f2" });
		assertEquals("f1", data.getColumnNames()[0]);
		assertEquals("f2", data.getColumnNames()[1]);
		assertEquals(2, data.getColumnNames().length);
	}

	public void testGetRows() throws Exception {
		Data data = new Data(new String[] { "f1", "f2" });
		List<Object[]> rows = data.getRows();
		data.append(new Object[] { "1", "2" });
		boolean hasObjectArrayData = false;
		for (Object[] row : rows) {
			if (row[0].equals("1") && row[1].equals("2")) {
				hasObjectArrayData = true;
			}
		}
		assertTrue(hasObjectArrayData);
	}
}
