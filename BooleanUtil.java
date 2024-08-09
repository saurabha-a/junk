package com.unilog.prime.commons.util;

import org.apache.commons.lang3.StringUtils;

public class BooleanUtil {

	private BooleanUtil() {

	}

	private static final Byte BYTE_1 = Byte.valueOf((byte) 1);

	public static boolean convertToBoolean(Object object) {
		boolean returnValue = false;

		if (object == null)
			return false;

		if (object instanceof Boolean)
			return (Boolean) object;

		if (object instanceof Byte)
			return BYTE_1.equals(object);

		String value = object.toString();

		if (!StringUtils.isEmpty(value)) {
			if ("yes".equalsIgnoreCase(value) || "y".equalsIgnoreCase(value) || "true".equalsIgnoreCase(value)
					|| "t".equalsIgnoreCase(value) || "on".equalsIgnoreCase(value)) {
				returnValue = true;
			} else {
				try {
					int val = Integer.parseInt(value);
					if (val != 0) {
						returnValue = true;
					}
				} catch (NumberFormatException nfe) {
					return returnValue;
				}
			}
		}
		return returnValue;
	}

	public static boolean safeValueOf(Object object, Boolean... defaultValue) {

		if (object == null) {
			return (defaultValue != null && defaultValue.length > 0) ? defaultValue[0] : null;
		}

		return convertToBoolean(object);
	}
	
	public static int compareTrueFirst(boolean x, boolean y) {
		if (x == y)
			return 0;
		return (x ? -1 : 1);
	}

}
