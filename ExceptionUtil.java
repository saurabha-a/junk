package com.unilog.prime.commons.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ExceptionUtil {
	
	public static String createExceptionId() {
		return System.nanoTime()+"_"+Math.round((Math.random() * 10000));
	}
	
	public static String getStackTrace(Exception exception) {
		StringWriter stackTrace = new StringWriter();
		exception.printStackTrace(new PrintWriter(stackTrace));
	    return stackTrace.toString();
	}
	private ExceptionUtil() {}
	
}
