package com.unilog.prime.commons.util;

import java.util.ArrayList;

public class ArrayUtil {

	private ArrayUtil() {

	}

	public static final String[] EMPTY_STRING_ARRAY = new String[0];

	public static <E> ArrayList<E> getArrayList() {
		return new ArrayList<>();
	}

}
