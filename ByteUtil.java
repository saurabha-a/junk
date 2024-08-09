package com.unilog.prime.commons.util;

public class ByteUtil {

	public static final Byte BYTE_ONE = Byte.valueOf((byte) 1);
	public static final Byte BYTE_ZERO = Byte.valueOf((byte) 0);

	public static Byte valueAsBooleanOf(Object obj, Byte... defaultValue) {

		if (obj == null) {
			if (defaultValue == null || defaultValue.length == 0)
				return null;
			else
				return defaultValue[0];
		}

		boolean b = BooleanUtil.convertToBoolean(obj.toString());
		return (byte) (b ? 1 : 0);
	}

	private ByteUtil() {
	}
}
