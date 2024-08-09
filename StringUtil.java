package com.unilog.prime.commons.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class StringUtil {

	private static final String EMPTY = "";
	private static final int STRING_BUILDER_SIZE = 256;

	private StringUtil() {

	}

	public static String convertToAlphaNumeric(String value) {
		return value.replaceAll("[^A-Za-z0-9]", "");
	}

	public static String[] split(String selectClause) {
		String[] columnsWithSpaces = selectClause.split(",");
		String[] trimmed = new String[columnsWithSpaces.length];

		for (int i = 0; i < columnsWithSpaces.length; i++) {
			trimmed[i] = columnsWithSpaces[i].trim();
		}
		return trimmed;
	}

	public static String convertToStringWithDelimiter(String[] strings, char delimiter) {
		return convertToStringWithDelimiter(strings, String.valueOf(delimiter));
	}

	public static String convertToStringWithDelimiter(String[] strings, String delimiter) {

		if (strings == null)
			return null;

		return convertToStringWithDelimiter(Arrays.stream(strings), delimiter);
	}

	private static String convertToStringWithDelimiter(Stream<String> strings, String delimiter) {
		return strings.filter(Objects::nonNull).collect(Collectors.joining(delimiter));
	}

	public static Set<String> convertCommaSeparatedStringToSet(String commaSeparatedString) {
		Set<String> setOfString = new HashSet<>();
		if (commaSeparatedString != null && !commaSeparatedString.isEmpty()) {
			String[] stringArray = commaSeparatedString.split(",");
			for (String str : stringArray) {
				setOfString.add(str.trim());
			}
		}
		return setOfString;
	}

	public static List<String> convertCommaSeparatedStringToList(String commaSeparatedString) {
		List<String> listOfString = new ArrayList<>();
		if (commaSeparatedString != null && !commaSeparatedString.isEmpty()) {
			String[] stringArray = commaSeparatedString.split(",");
			for (String str : stringArray) {
				listOfString.add(str.trim());
			}
		}
		return listOfString;
	}

	public static List<String> convertDelimitedStringToList(String delimitedString, char delimiter) {
		return convertDelimitedStringToList(delimitedString, String.valueOf(delimiter));
	}

	public static List<String> convertDelimitedStringToList(String delimitedString, String delimiter) {
		List<String> listOfString = new ArrayList<>();
		if (delimitedString != null && !delimitedString.isEmpty()) {
			String[] stringArray = delimitedString.split(delimiter);
			for (String str : stringArray) {
				listOfString.add(str.trim());
			}
		}
		return listOfString;
	}

	public static String convertToCommaSeparatedString(Collection<String> listOfStrings) {

		if (listOfStrings == null || listOfStrings.isEmpty())
			return null;

		return convertToStringWithDelimiter(listOfStrings.stream(), ",");
	}

	/**
	 * Returns the string itself if there is no delimiter on the string
	 * 
	 * @param delimitedString
	 * @param delimiter
	 * @return String
	 */
	public static String getFirstPropertyFromDelimitedProperties(String delimitedString, String delimiter) {
		String returnString = null;
		List<String> listOfReturnString = convertDelimitedStringToList(delimitedString, delimiter);
		if (listOfReturnString != null && !listOfReturnString.isEmpty()) {
			returnString = listOfReturnString.get(0);
		} else {
			returnString = delimitedString;
		}
		return returnString;

	}

	/**
	 * Returns the string itself if there is no delimiter on the string
	 * 
	 * @param delimitedString
	 * @param delimiter
	 * @return String
	 */
	public static String getLastPropertyFromDelimitedProperties(String delimitedString, String delimiter) {
		String returnString = null;
		List<String> listOfReturnString = convertDelimitedStringToList(delimitedString, delimiter);
		if (listOfReturnString != null && !listOfReturnString.isEmpty()) {
			returnString = listOfReturnString.get(listOfReturnString.size() - 1);
		} else {
			returnString = delimitedString;
		}
		return returnString;
	}

	/**
	 * Returns null if the string is not delimited
	 * 
	 * @param delimitedString
	 * @param delimiter
	 * @return String
	 */
	public static String omitFirstPropertyFromDelimitedProperties(String delimitedString, String delimiter) {
		String returnString = null;
		int index = -1;
		if ((index = delimitedString.indexOf(delimiter)) > 0) {
			returnString = delimitedString.substring(index + delimiter.length());
		}
		return returnString;
	}

	/**
	 * Returns null if the string is not delimited
	 * 
	 * @param delimitedString
	 * @param delimiter
	 * @return String
	 */
	public static String omitLastPropertyFromDelimitedProperties(String delimitedString, String delimiter) {
		String returnString = null;
		int lastIndex = -1;
		if ((lastIndex = delimitedString.lastIndexOf(delimiter)) > 0) {
			returnString = delimitedString.substring(0, lastIndex);
		}
		return returnString;
	}

	/**
	 * @param cacheKeysList
	 * @return String []
	 */
	public static String[] convertToStringArray(List<String> cacheKeysList) {
		String[] strings = null;
		if (cacheKeysList != null) {
			strings = cacheKeysList.stream().toArray(String[]::new);
		}
		return strings;
	}

	public static String convertHumanReadableStringIntoCamelCase(String string) {
		// Check if the input string is null or empty
        if (string == null || string.isEmpty()) {
            return "";
        }

        StringBuilder camelCaseString = new StringBuilder();

        // Split the input string into words
        String[] words = string.split("\\s+");

        // Capitalize the first letter of each word (except the first word) and append to the camelCaseString
        for (int i = 0; i < words.length; i++) {
            String word = words[i];
            if (i == 0) {
                camelCaseString.append(word.toLowerCase()); // Convert first word to lowercase
            } else {
                camelCaseString.append(Character.toUpperCase(word.charAt(0)));
                camelCaseString.append(word.substring(1).toLowerCase());
            }
        }

        return camelCaseString.toString();
	}

	public static String prefixString(Object string, String... prefix) {

		if (string == null)
			return null;
		if (prefix == null)
			return string.toString();

		StringBuilder sb = new StringBuilder();
		for (String pre : prefix)
			sb.append(pre);
		sb.append(string.toString());
		return sb.toString();
	}

	public static String safeValueOf(Object object, String... defaultValue) {

		if (object == null)
			if (defaultValue == null || defaultValue.length == 0)
				return null;
			else
				return defaultValue[0];

		if (object instanceof char[])
			return new String((char[]) object);

		if (object instanceof byte[])
			return new String((byte[]) object);

		return object.toString().trim();
	}

	public static Object notEmptyObjetctTrim(Object str) {
		if (str != null) {
			str = str.toString().trim();
			if (str.toString().isEmpty()) {
				str = null;
			}
		}
		return str;
	}

	public static String safeTrim(String str) {

		if (str == null)
			return str;
		return str.trim();
	}

	public static String safeTrimEmptyNull(String str) {
		return (String) notEmptyObjetctTrim(str);
	}

	public static String ifEmptyMakeItNull(String str) {
		if (str != null) {
			str = str.trim();
			if (str.isEmpty()) {
				str = null;
			}
		}
		return str;
	}

	public static boolean safeEquals(Object str1, Object str2) {

		if (str1 == null && str2 == null)
			return true;
		else if (str1 == null || str2 == null)
			return false;
		else
			return str1.toString().equalsIgnoreCase(str2.toString());

	}

	public static String trimPrice(Object object) {
		if (object == null || object.toString().trim().equals(""))
			return null;
		StringBuilder sb = new StringBuilder(object.toString());
		while (sb.length() != 0 && (sb.charAt(sb.length() - 1) == '0' || sb.charAt(sb.length() - 1) == '.')) {
			if (sb.charAt(sb.length() - 1) == '.') {
				sb.deleteCharAt(sb.length() - 1);
				break;
			}
			sb.deleteCharAt(sb.length() - 1);
		}
		return sb.length() == 0 ? "0" : sb.toString();
	}

	public static String safeSubstring(String string, int beginIndex, int endIndex) {

		if (string.length() < endIndex)
			endIndex = string.length();
		if (beginIndex < 0)
			beginIndex = 0;

		return string.substring(beginIndex, endIndex);
	}

	public static String escapeBackslash(String string) {
		if (string == null)
			return null;
		if (string.contains("\\"))
			string = string.replaceAll("\\\\", "\\\\\\\\");

		return string;
	}

	// before using this service have to escape the backslash(\) as well
	public static String escapeInchesCharacter(String value) {
		StringBuilder sb = new StringBuilder();
		int count = 0;
		for (int i = 0; i < value.length(); i++) {
			char c = value.charAt(i);
			if (c == '\"')
				count++;
			if (count == 2 && (value.charAt(i + 1) == ':' || value.charAt(i + 1) == ',' || value.charAt(i + 1) == '}'))
				count = 0;
			if (c == '\\' && (value.charAt(i + 1) == '\"' || value.charAt(i + 2) == '\"'))
				continue;
			if (count == 2 && c == '\"') {
				sb.append("\\\"");
				count = 1;
				continue;
			}
			sb.append(c);
		}
		return sb.toString();
	}

	public static boolean isNullOrEmpty(String value) {
		value = ifEmptyMakeItNull(value);
		if (value == null)
			return true;
		else
			return false;
	}

	public static boolean isNumeric(String value) {
		try {
			Double.parseDouble(value);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	public static boolean isIntegerNumeric(String value) {
		try {
			Integer.parseInt(value);
			return true;
		} catch (NumberFormatException e) {
			return false;
		}
	}

	// checks equality of two charSequences if both are null/empty return true
	// else checks each character in sequence
	public static boolean checkEquals(final CharSequence cs1, final CharSequence cs2) {

		if (cs1 == cs2)
			return true;

		if (cs1 == null || cs2 == null)
			return false;

		if (cs1.length() != cs2.length())
			return false;

		if (cs1 instanceof String && cs2 instanceof String)
			return cs1.equals(cs2);

		final int length = cs1.length();
		for (int i = 0; i < length; i++) {
			if (cs1.charAt(i) != cs2.charAt(i)) {
				return false;
			}
		}
		return true;
	}

	public static String join(final Iterable<?> iterable, final String separator) {
		if (iterable == null)
			return null;

		return join(iterable.iterator(), separator);
	}

	public static String join(final Iterator<?> iterator, final String separator) {
		if (iterator == null)
			return null;
		if (!iterator.hasNext())
			return EMPTY;

		// one element in the iterator
		final Object first = iterator.next();
		if (!iterator.hasNext()) {
			return Objects.toString(first, EMPTY);
		}

		// more than one element in the iterator
		StringBuffer buf = new StringBuffer(STRING_BUILDER_SIZE);
		if (first != null)
			buf.append(first);

		while (iterator.hasNext()) {
			if (separator != null) {
				buf.append(separator);
			}
			Object obj = iterator.next();
			if (obj != null)
				buf.append(obj);
		}
		return buf.toString();
	}

	public static String captilize(final String str) {
		int strLen;
		if (str == null || ((strLen = str.length()) == 0))
			return str;

		// check already captilized
		final int firstCodePoint = str.codePointAt(0);
		final int captilizedCodePoint = Character.toTitleCase(firstCodePoint);

		if (firstCodePoint == captilizedCodePoint)
			return str;

		final int newCodePoints[] = new int[strLen]; // cannot be longer than the char array
		int outOffset = 0;
		newCodePoints[outOffset++] = captilizedCodePoint; // copy the first codepoint
		for (int inOffset = Character.charCount(firstCodePoint); inOffset < strLen;) {
			final int codepoint = str.codePointAt(inOffset);
			newCodePoints[outOffset++] = codepoint; // copy the remaining ones
			inOffset += Character.charCount(codepoint);
		}
		return new String(newCodePoints, 0, outOffset);
	}

	public static String[] split(final String str, final String separator) {
		int max = -1;
		boolean preserveAllTokens = false;
		if (str == null)
			return null;

		final int strLen = str.length();
		if (strLen == 0)
			return ArrayUtil.EMPTY_STRING_ARRAY;
		final List<String> list = new ArrayList<>();
		int sizePlus1 = 1;
		int i = 0, start = 0;
		boolean match = false;
		boolean lastMatch = false;
		if (separator == null) {
			while (i < strLen) {
				if (Character.isWhitespace(str.charAt(i))) {
					if (match || preserveAllTokens) {
						lastMatch = true;
						if (sizePlus1++ == max) {
							i = strLen;
							lastMatch = false;
						}
						list.add(str.substring(start, i));
						match = false;
					}
					start = ++i;
					continue;
				}
				lastMatch = false;
				match = true;
				i++;
			}
		} else if (separator.length() == 1) {
			// Optimise 1 character case
			final char sep = separator.charAt(0);
			while (i < strLen) {
				if (str.charAt(i) == sep) {
					if (match || preserveAllTokens) {
						lastMatch = true;
						if (sizePlus1++ == max) {
							i = strLen;
							lastMatch = false;
						}
						list.add(str.substring(start, i));
						match = false;
					}
					start = ++i;
					continue;
				}
				lastMatch = false;
				match = true;
				i++;
			}
		} else {
			// standard case
			while (i < strLen) {
				if (separator.indexOf(str.charAt(i)) >= 0) {
					if (match || preserveAllTokens) {
						lastMatch = true;
						if (sizePlus1++ == max) {
							i = strLen;
							lastMatch = false;
						}
						list.add(str.substring(start, i));
						match = false;
					}
					start = ++i;
					continue;
				}
				lastMatch = false;
				match = true;
				i++;
			}
		}
		if (match || preserveAllTokens && lastMatch) {
			list.add(str.substring(start, i));
		}
		return list.toArray(new String[list.size()]);
	}
	
	public static Map<String, String[]> trimWhiteSpaceInPayload(Map<String, String[]> parameterMap ){
		for (Map.Entry<String, String[]> map : parameterMap.entrySet()) {
			for (int i = 0; i < map.getValue().length; i++)
				map.getValue()[i] = map.getValue()[i].trim();
		}
		return parameterMap;
	}
}
