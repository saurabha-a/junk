package com.unilog.prime.commons.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LongUtil {

	private static Logger logger = LoggerFactory.getLogger(IntegerUtil.class);

	private LongUtil() {
	}

	public static Long safeValueOf(Object number, Long... defaultValue) {

		if (number == null)
			return (defaultValue == null || defaultValue.length == 0) ? 0 : defaultValue[0];

		try {
			return Long.valueOf(number.toString());
		} catch (Exception ex) {
			logger.debug("Cannot convert the value {} into integer.", number);
		}

		return (defaultValue == null || defaultValue.length == 0) ? 0 : defaultValue[0];
	}

	public static Long safeValueOfWithNulls(Object number, Long... defaultValue) {

		if (number == null)
			return (defaultValue == null || defaultValue.length == 0) ? null : defaultValue[0];

		try {
			return Long.valueOf(number.toString());
		} catch (Exception ex) {
			logger.debug("Cannot convert the value {} into integer.", number);
		}

		return (defaultValue == null || defaultValue.length == 0) ? null : defaultValue[0];
	}
}
