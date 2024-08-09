package com.unilog.prime.commons.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IntegerUtil {

	private static Logger logger = LoggerFactory.getLogger(IntegerUtil.class);
	
	private IntegerUtil() {}
	
	public static Integer safeValueOf(Object number, Integer ...defaultValue) {
		
		if (number == null)
			return (defaultValue == null || defaultValue.length == 0) ? 0 : defaultValue[0];
		
		try {
			return Integer.valueOf(number.toString());
		}catch (Exception ex) {
			logger.debug("Cannot convert the value {} into integer.", number);
		}
		
		return (defaultValue == null || defaultValue.length == 0) ? 0 : defaultValue[0];
	}
	
	public static Integer safeValueOfWithNulls(Object number, Integer ...defaultValue) {
		
		if (number == null)
			return (defaultValue == null || defaultValue.length == 0) ? null : defaultValue[0];
		
		try {
			return Integer.valueOf(number.toString());
		}catch (Exception ex) {
			logger.debug("Cannot convert the value {} into integer.", number);
		}
		
		return (defaultValue == null || defaultValue.length == 0) ? null : defaultValue[0];
	}

	public static boolean isInteger(String str) {
		if (str == null) {
			return false;
		}
		int length = str.length();
		if (length == 0) {
			return false;
		}
		int i = 0;
		if (str.charAt(0) == '-') {
			if (length == 1) {
				return false;
			}
			i = 1;
		}
		for (; i < length; i++) {
			char c = str.charAt(i);
			if (c < '0' || c > '9') {
				return false;
			}
		}
		return true;
	}
}
