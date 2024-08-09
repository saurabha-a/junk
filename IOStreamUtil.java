package com.unilog.prime.commons.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

public class IOStreamUtil {

	public static String inputStreamToString(InputStream os) throws IOException {
		
		return readerToString(new InputStreamReader(os));
	}
	
	public static String readerToString(Reader reader) throws IOException {
	
		BufferedReader in = new BufferedReader(reader);
		String line;
		StringBuffer buffer = new StringBuffer();
		while ((line = in.readLine()) != null)
			buffer.append(line);
		return buffer.toString();
	}
	
	private IOStreamUtil() {}
}
