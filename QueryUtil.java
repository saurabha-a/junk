package com.unilog.prime.commons.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class QueryUtil {

	private static final String REPLACE_CONSTANT = "###REPLACE###";
	private static final String PN_REPLACE_CONSTANT = "a1.ID IN";
	private static final String WHERE = "WHERE";
	private static final String FROM = "FROM";
	private static final String UPDATED_FROM = "a1.UPDATED_AT AS UPDATED_AT FROM";
	private static final String REPLACE_CONSTANT_1 = "a1.id IN (###REPLACE###)";
	private static final String REPLACE_CONSTANT_2 = "a1.ID IN (###REPLACE###)";
	private static final String REPLACE_CONSTANT_3 = "a.id IN (###REPLACE###)";
	private static final String REPLACE_CONSTANT_4 = "a.ID IN (###REPLACE###)";

	public static final String DUMMY_WHERE_CLAUSE = "1 = 1";
	public static final String ITEM = "ITEM";
	public static final String EXPORT = "EXPORT";
	public static final String PRECOMPUTE = "PRECOMPUTE";
	public static final String MERGE = "MERGE";

	public static String replaceWithItemIds(String query, String... ids) {

		if (ids == null || ids.length == 0)
			return query;

		return query.replace(REPLACE_CONSTANT, Arrays.stream(ids).collect(Collectors.joining("','", "'", "'")));
	}

	public static String replaceWithItemIds(String query, Collection<String> ids) {

		if (ids == null || ids.isEmpty())
			return query;

		return query.replace(REPLACE_CONSTANT, ids.stream().collect(Collectors.joining("','", "'", "'")));
	}

	public static String replaceWithQuery(String query, String replaceQuery) {

		query = query.replace(REPLACE_CONSTANT_1, REPLACE_CONSTANT);
		query = query.replace(REPLACE_CONSTANT_2, REPLACE_CONSTANT);
		query = query.replace(REPLACE_CONSTANT_3, REPLACE_CONSTANT);
		query = query.replace(REPLACE_CONSTANT_4, REPLACE_CONSTANT);

		return query.replace(REPLACE_CONSTANT, replaceQuery);
	}

	public static String replaceWithQuery(String query, String replaced, String replaceWith) {

		return query.replace(replaced, replaceWith);
	}

	public static String replacePartNumberConstant(String query, String replaceQuery) {
		
		return query.replace(PN_REPLACE_CONSTANT, replaceQuery);
	}

	public static String getConstructedQuery(String query, String conditions, String sort, Integer limit, String join,
			String columns) {
		StringBuilder replaceString = new StringBuilder();
		if (!query.contains("UNION ALL"))
			query = query.replace("UNION", ") UNION (");
		query = removeExtraBracesonUnion(query);
		query = "(" + query + ")";

		replaceString.append(conditions != null && !conditions.isEmpty() ? "(" + conditions + ")" : DUMMY_WHERE_CLAUSE);
		replaceString.append(" ");
		replaceString.append(sort != null && !sort.isEmpty() ? sort : "");
		replaceString.append(limit != null && limit > 0 ? " LIMIT " + limit + " " : "");

		query = query.replace(REPLACE_CONSTANT_1, REPLACE_CONSTANT);
		query = query.replace(REPLACE_CONSTANT_2, REPLACE_CONSTANT);
		query = query.replace(REPLACE_CONSTANT_3, REPLACE_CONSTANT);
		query = query.replace(REPLACE_CONSTANT_4, REPLACE_CONSTANT);

		if (columns != null && !columns.isEmpty())
			query = query.replace(UPDATED_FROM, columns + ", " + UPDATED_FROM);

		query = (join != null && !join.isEmpty()) ? query.replace(WHERE, join + " " + WHERE) : query;
		return query.replace(REPLACE_CONSTANT, replaceString.toString());
	}

	private static String removeExtraBracesonUnion(String query) {
		if (query.contains(
				"(SELECT 1 n ) UNION ( SELECT 2 ) UNION ( SELECT 3 ) UNION ( SELECT 4 ) UNION ( SELECT 5 ) UNION ( SELECT 6 ) UNION ( SELECT 7 ) UNION ( SELECT 8 ) UNION ( SELECT 9 ) UNION ( SELECT 10)")) {
			query = query.replace(
					"(SELECT 1 n ) UNION ( SELECT 2 ) UNION ( SELECT 3 ) UNION ( SELECT 4 ) UNION ( SELECT 5 ) UNION ( SELECT 6 ) UNION ( SELECT 7 ) UNION ( SELECT 8 ) UNION ( SELECT 9 ) UNION ( SELECT 10)",
					"(SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10)");
		}
		return query;
	}
	
	/**
	 * Function to replace or delete the columns from the provided select Query.
	 * 
	 * @Parameter query : The provided query in which the column needs to replaced
	 * 
	 * @Parameter newColumns : If it is null or empty then all columns will be
	 *            removed except the first column in the query. Else the columns
	 *            will be replace with provided new columns except the first column
	 *            in the query
	 * 
	 *            *Note First column in the query will be default column.
	 * 
	 **/

	public static String replaceColumnsInQuery(final String query, final String newColumns) {
		try {
			String tempQuery = query;
			if (tempQuery == null || tempQuery.isBlank())
				return tempQuery;

			// remove unwanted unions in column section
			tempQuery = removeExtraBracesonUnion(tempQuery);
			if (tempQuery.contains(
					"(SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10)")) {
				tempQuery = tempQuery.replaceAll(Pattern.quote(
						"(SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10)"),
						"");
			}
			// start replacing the columns
			StringBuilder finalQuery = new StringBuilder();
			String baseCondition = " UNION ";

			List<String> subQueries = Arrays.asList(tempQuery.split(baseCondition));
			subQueries.stream().forEach(subQuery -> {
				subQuery = subQuery.replaceAll(
						Pattern.quote(subQuery.substring(subQuery.indexOf(","), subQuery.lastIndexOf(FROM) - 1)),
						newColumns == null || newColumns.isBlank() ? "" : "," + newColumns) + baseCondition;
				finalQuery.append(subQuery);
			});
			finalQuery.delete(finalQuery.lastIndexOf(baseCondition),
					finalQuery.lastIndexOf(baseCondition) + baseCondition.length());
			return finalQuery.toString();
		} catch (Exception exception) {
			return query;
		}
	}
	
	public static String getReplacedQuery(String query, String replaceString, String indexValue) {
		String[] json = query.split(indexValue);
		String mainQuery = "";
		for (int i = 0; i < json.length; i++) {
			if (i == json.length - 1)
				mainQuery = mainQuery + json[i];
			else {
				json[i] = json[i].replace(json[i].substring(json[i].indexOf("SELECT a.ID AS ID"), json[i].length()),
						replaceString);
				mainQuery = mainQuery + json[i] + indexValue;
			}
		}
		query = mainQuery;
		return query;
	}

	public static String replaceColumnsInLinkedItemExportQuery(final String query, final String newColumns) {
		try {
			String tempQuery = query;
			if (tempQuery == null || tempQuery.isBlank())
				return tempQuery;

			// remove unwanted unions in column section
			tempQuery = removeExtraBracesonUnion(tempQuery);
			if (tempQuery.contains(
					"(SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10)")) {
				tempQuery = tempQuery.replaceAll(Pattern.quote(
						"(SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9 UNION SELECT 10)"),
						"");
			}
			// start replacing the columns
			StringBuilder finalQuery = new StringBuilder();
			String baseCondition = " UNION ";

			List<String> subQueries = Arrays.asList(tempQuery.split(baseCondition));
			subQueries.stream().forEach(subQuery -> {
				if(subQuery.indexOf("CHAR_LENGTH(") > 0) {
					int idx = subQuery.indexOf("FROM");
			        String sub = subQuery.substring(idx+1);
			        int idx1 = sub.indexOf("FROM");
			        String subStr = sub.substring(idx1+1);
			        int fromIndex = subStr.indexOf("FROM");
			        
					subQuery = subQuery.replaceAll(
							Pattern.quote(subQuery.substring(subQuery.indexOf(","), fromIndex+idx+idx1)),
							newColumns == null || newColumns.isBlank() ? "" : "," + newColumns) + baseCondition;
					
					finalQuery.append(subQuery);
				} else {
					int idx = subQuery.indexOf("FROM");
			        String sub = subQuery.substring(idx+1);
			        int fromIndex = sub.indexOf("FROM");
			        
					subQuery = subQuery.replaceAll(
							Pattern.quote(subQuery.substring(subQuery.indexOf(","), fromIndex+idx)),
							newColumns == null || newColumns.isBlank() ? "" : "," + newColumns) + baseCondition;
					
					finalQuery.append(subQuery);
				}
			});
			
			finalQuery.delete(finalQuery.lastIndexOf(baseCondition),
					finalQuery.lastIndexOf(baseCondition) + baseCondition.length());
			
			return finalQuery.toString();
			
		} catch (Exception exception) {
			return query;
		}
	}
	
	private QueryUtil() {
	}
}
