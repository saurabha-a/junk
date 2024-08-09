package com.unilog.prime.commons.util;

public class TimeUtil {

	private static final long[] TIME_FACTORS = { 1, 1000, 60, 60, 24 };
	private static final String[] TIME_LABELS = { "milli second", "second", "minute", "hour", "day" };

	public static String timeElapsedInStringFromMillis(long from) {

		long total = System.currentTimeMillis() - from;
		if (total < 0)
			total *= -1;

		int i = 0;
		StringBuilder sb = new StringBuilder();

		do {

			long r;
			if (i + 1 != TIME_FACTORS.length) {
				r = total % TIME_FACTORS[i + 1];
				total /= TIME_FACTORS[i + 1];
			} else {
				r = total;
				total = 0;
			}
			if (r != 0)
				sb.insert(0, " ").insert(0, (r != 1 ? "s" : "")).insert(0, TIME_LABELS[i]).insert(0, " ").insert(0, r);
			i++;
		} while (total != 0);

		return sb.toString().trim();
	}

	private TimeUtil() {
	}
}