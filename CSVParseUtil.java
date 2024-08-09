package com.unilog.prime.commons.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVParseUtil {
	
	private CSVParseUtil() {
		throw new UnsupportedOperationException();
	}
	
	// I don't understand how to break this up to reduce the cognitive complexity to 15.
	public static List<String> stringToColumns(String line) { //NOSONAR
		
		int length = line.length();
		if (length == 0) return Collections.emptyList();
		
		ArrayList<String> list = new ArrayList<>();
		StringBuilder sb = new StringBuilder();
		boolean withIn = false;
		int i=0;
		while ((i+1)<length) {			
			char ch = line.charAt(i);
			if (ch == ',' && !withIn) {
				list.add(sb.toString());
				sb = new StringBuilder();				
			}else if (ch == '"') {
				int j = i;
				while ( (line.charAt(j) == '"') && (j+1<length) ) j++;
				if ((j-i) % 2 == 1)
					withIn = !withIn;
				if (j-i != 1){
					for (int k = 0; k<((j-i)/2); k++) 
						sb.append('"');
					i = j-1;
				}
			}else
				sb.append(ch);
			i++;
		}
		
		sb.append(line.charAt(i) != '\n' ? line.charAt(i) : "");
		if (sb.length() > 0) list.add(sb.toString());
		
		return list;
	}
	
	private static String columnsToString(Stream<String> stream) {
		
		return stream.map(CSVParseUtil::singleColumnConvert).collect(Collectors.joining(",","","\n"));
	}
	
	public static String singleColumnConvert(String e) {
		
		boolean hasDoubleQuotes = e.indexOf('"') != -1;
		String str = e;
		if (hasDoubleQuotes) str = e.replaceAll("\"", "\"\"");
		boolean surround = hasDoubleQuotes || (e.indexOf(',') != -1) || (e.indexOf('\n') != -1) || (e.indexOf('\r') != -1);
		if (!surround) return str;
		return '"'+str+'"';
	}
	
	public static String columnsToString(String []columns) {
		
		return columnsToString(Arrays.stream(columns));
	}

	public static String columnsToString(List<String> columns) {
		
		return columnsToString(columns.stream());
	}	
}