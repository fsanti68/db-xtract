package com.dsf.utils.sql;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

public class NamedParameterStatementTest {

	@Test
	public NamedParameterStatementTest() {
		String source = "select *, \"pippo\" as name from test where k = :kparam and y = :yparam and status = 'OPEN' and x = :kparam";
		String expect = "select *, \"pippo\" as name from test where k = ? and y = ? and status = 'OPEN' and x = ?";

		Map<String, List<Integer>> paramMap = new HashMap<>();
		String result = NamedParameterStatement.parse(source, paramMap);
		assert expect.equals(result);
		assert paramMap.containsKey("kparam");
		assert paramMap.containsKey("yparam");
		List<Integer> k = paramMap.get("kparam");
		List<Integer> y = paramMap.get("yparam");
		assert k.size() == 2 && k.get(0).intValue() == 1 && k.get(1).intValue() == 3;
		assert y.size() == 1 && y.get(0).intValue() == 2;
	}
}
