package com.unilog.prime.commons.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnumUtil {

	private static Logger logger = LoggerFactory.getLogger(EnumUtil.class);

	private EnumUtil() {
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static <T> boolean equals(T value, Object anything) {

		if ((anything == null) || (value.getClass().equals(anything.getClass())))
			return value == anything;
		T targetValue = null;

		try {
			//Hoping for all enum values to be uppercase
			targetValue = (T) Enum.valueOf((Class<? extends Enum>) value.getClass(), anything.toString().toUpperCase());
		} catch (Exception e) {
			logger.debug("Cannot convert the value {} into enum of class {}.", anything, value.getClass());
		}
		return value == targetValue;
	}

	public static <T extends Enum<T>> T safeValueOf(Class<T> clazz, Object anything) {

		try {
			return Enum.valueOf(clazz, anything.toString());
		} catch (Exception e) {
			logger.debug("Cannot convert the value {} into enum of class {}.", anything, clazz);
		}
		return null;
	}
	
	public static <T extends Enum<T>> T safeValueOf(Class<T> clazz, Object anything, T defaultValue) {

		T value = safeValueOf(clazz, anything);
		if (value == null) 
			value = defaultValue;
		
		return value;
	}
}
