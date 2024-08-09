package com.unilog.prime.commons.util;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MapUtil {

	@SafeVarargs
	public static <K,V> List<K> findEmptyValuesfor(Map<K, V> map, K ...keys) {
		
		if (keys == null || keys.length == 0) return Collections.emptyList();
		if (map == null || map.isEmpty()) return Arrays.asList(keys);		
		return Arrays.stream(keys).filter(k -> !map.containsKey(k) || map.get(k) == null ).collect(Collectors.toList());
	}
	
	private MapUtil() {}
}
