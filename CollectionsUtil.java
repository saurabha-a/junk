package com.unilog.prime.commons.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

@SuppressWarnings("rawtypes")
public class CollectionsUtil {

	private static Integer INTEGER = Integer.valueOf(1);

	private CollectionsUtil() {

	}

	public static <E> Set<E> getHashSet() {
		return new HashSet<E>();
	}

	public static <E> Set<E> getTreeSet() {
		return new TreeSet<E>();
	}

	public static <K extends Comparable, V> TreeMap<K, V> getTreeMap() {
		return new TreeMap<K, V>();
	}

	public static Map<Object, Integer> getCardinalityMap(final Collection collection) {
		Map<Object, Integer> count = new HashMap<Object, Integer>();
		Iterator iterator = collection.iterator();
		while (iterator.hasNext()) {
			Object obj = iterator.next();
			Integer c = count.get(obj);
			if (c == null) {
				count.put(obj, INTEGER);
			} else {
				count.put(obj, Integer.valueOf(INTEGER + 1));
			}
		}
		return count;
	}

	private static final int getFreq(final Object obj, final Map map) {
		Integer count = (Integer) map.get(obj);
		if (count != null) {
			return count.intValue();
		}
		return 0;
	}

	public static boolean isEqualCollection(final Collection a, final Collection b) {
		if (a.size() != b.size()) {
			return false;
		} else {
			Map mapA = getCardinalityMap(a);
			Map mapB = getCardinalityMap(b);
			if (mapA.size() != mapB.size()) {
				return false;
			} else {
				Iterator it = mapA.keySet().iterator();
				while (it.hasNext()) {
					Object obj = it.next();
					if (getFreq(obj, mapA) != getFreq(obj, mapB)) {
						return false;
					}
				}
			}
			return true;
		}
	}
}
