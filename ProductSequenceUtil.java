package com.unilog.prime.commons.util;

import java.util.HashMap;
import java.util.Map;

public class ProductSequenceUtil {

	public ProductSequenceUtil() {

	}

	// This is written only to get Products in map format
	public static Map<String, Object> getMap(String line) {
		Map<String, Object> productJsonMap = new HashMap<>();

		int length = line.length();
		int cs = 0;
		int c = 0;
		int i = 0;
		int startIndex = 0;
		int endIndex = 0;
		String tempName = null;
		String tempValue = null;
		boolean has = false;

		while ((i + 1) < length) {
			char ch = line.charAt(i);
			if (ch == '{')
				cs = 1;
			else if (ch == '}') {
				cs = 0;
				c = 0;
			}
			if (cs == 1 && ch == '\"' && i > 0) {
				if (line.charAt(i - 1) == '{' || line.charAt(i - 1) == ',') {
					c = 2;
					startIndex = i + 1;
				} else if (line.charAt(i - 1) == '\\') {
					has = true;
				} else if ((line.charAt(i - 1) == ':') || (line.charAt(i - 2) == ':')) {
					c = 3;
					startIndex = i + 1;
				} else {
					endIndex = i;
					if (c == 2) {
						tempName = line.substring(startIndex, endIndex);
						productJsonMap.put(tempName, null);
					} else if (c == 3) {
						tempValue = line.substring(startIndex, endIndex);
						tempValue = replaceString(tempValue, has);
						has = false;

						productJsonMap.put(tempName, tempValue);
					}
					c = 1;
				}
			} else if ((ch == ' ' && line.charAt(i - 1) == ':' && line.charAt(i + 1) != '\"')) {
				int val = line.indexOf(",", i +1);
				if (val == -1)
					val = length - 1;
				String value = line.substring(i+1, val);
				if (value.equals("null"))
					value = null;
				productJsonMap.put(tempName, value);
			}
			i++;
		}
		return productJsonMap;
	}

	public static String replaceString(String substring, boolean has) {
		if (!has)
			return substring;

		StringBuilder sb = new StringBuilder();
		int length = substring.length();
		int i = 0;
		char c;
		while (i < length) {
			c = substring.charAt(i++);
			if (c == '\\')
				continue;
			sb.append(c);
		}
		return sb.toString();
	}
}
