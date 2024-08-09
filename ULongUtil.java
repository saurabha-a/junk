package com.unilog.prime.dbcore.util;

import org.jooq.types.ULong;

public class ULongUtil {

	public static ULong valueOf(Object value, ULong... defaultValues) {

		
		if (value == null) {
			if (defaultValues != null && defaultValues.length != 0)
				return defaultValues[0];
			return null;
		}
		
		if (value instanceof ULong) return (ULong) value;
		
		return ULong.valueOf(value.toString());
	}

	private ULongUtil() {
	}
}
