package com.dsf.utils.sql;

import java.util.HashMap;
import java.util.Map;

import org.testng.annotations.Test;

public class NamedParameterStatementTest {

	@Test
	public NamedParameterStatementTest() {
		String source = "select *, \"pippo\" as name from test where k = :kparam and y = :yparam and status = 'OPEN' and x = :kparam";
		String expect = "select *, \"pippo\" as name from test where k = ? and y = ? and status = 'OPEN' and x = ?";

		Map<?, ?> paramMap = new HashMap<>();
		String result = NamedParameterStatement.parse(source, paramMap);
		assert expect.equals(result);
		assert paramMap.containsKey("kparam");
		assert paramMap.containsKey("yparam");
		int[] k = (int[]) paramMap.get("kparam");
		int[] y = (int[]) paramMap.get("yparam");
		assert k[0] == 1 && k[1] == 3;
		assert y[0] == 2;
	}
}
