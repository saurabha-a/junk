package com.unilog.prime.commons.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TSVParseUtil {
	private TSVParseUtil() {
		throw new UnsupportedOperationException();
	}

	public static String columnsToString(List<String> columns) {
		return columns.stream().map(TSVParseUtil::singleColumnConvert).collect(Collectors.joining("\t", "", "\n"));
	}

	public static List<String> stringToColumns(String line) {
		int length = line.length();
		if (length == 0)
			return Collections.emptyList();

		ArrayList<String> list = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		boolean withIn = false;
		int i = 0;
		while ((i + 1) < length) {
			char ch = line.charAt(i);
			if (ch == '\t' && !withIn) {
				list.add(sb.toString());
				sb = new StringBuilder();
			} else if (ch == '"') {
				int j = i;
				while ((line.charAt(j) == '"') && (j + 1 < length))
					j++;
				if ((j - i) % 2 == 1)
					withIn = !withIn;
				if (j - i != 1) {
					for (int k = 0; k < ((j - i) / 2); k++)
						sb.append('"');
					i = j - 1;
				}
			} else
				sb.append(ch);
			i++;
		}

		sb.append(line.charAt(i) != '\t' ? line.charAt(i) : "");
		if (sb.length() > 0)
			list.add(sb.toString());

		return list;
	}

	private static String singleColumnConvert(String e) {
		String str = e;
		boolean hasDoubleQuotes = e.indexOf('"') != -1;
		if (hasDoubleQuotes)
			str = e.replaceAll("\"", "\"\"");
		boolean surround = hasDoubleQuotes || (e.indexOf('\t') != -1) || (e.indexOf('\n') != -1)
				|| (e.indexOf('\r') != -1);
		if (!surround)
			return str;
		return '"' + str + '"';
	}
}
