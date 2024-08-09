package com.unilog.prime.commons.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DoubleUtil {

	private static Logger logger = LoggerFactory.getLogger(DoubleUtil.class);
	
	private DoubleUtil() {}
	
	public static Double safeValueOf(Object number, Double ...defaultValue) {
		
		if (number == null)
			return (defaultValue == null || defaultValue.length == 0) ? 0 : defaultValue[0];
		
		try {
			return Double.valueOf(number.toString());
		}catch (Exception ex) {
			logger.debug("Cannot convert the value {} into Double.", number);
		}
		
		return (defaultValue == null || defaultValue.length == 0) ? 0 : defaultValue[0];
	}
	
	public static Double safeValueOfWithNulls(Object number, Double ...defaultValue) {
		
		if (number == null)
			return (defaultValue == null || defaultValue.length == 0) ? null : defaultValue[0];
		
		try {
			return Double.valueOf(number.toString());
		}catch (Exception ex) {
			logger.debug("Cannot convert the value {} into Double.", number);
		}
		
		return (defaultValue == null || defaultValue.length == 0) ? null : defaultValue[0];
	}
}
