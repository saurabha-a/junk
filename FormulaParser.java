package com.unilog.cx1.pim.commons.util;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.unilog.prime.commons.util.StringUtil;

public class FormulaParser {
	
	private static final Logger logger = LoggerFactory.getLogger(FormulaParser.class);	
	private static final String ILLEGAL_LEXICON_FOUND = "Illegal lexicon found near %s at position %s in the given formula %s";
	private static final String DOT_LABEL = ".Label";
	
	private FormulaParser() {
		logger.info("Don't let anyone to get instance of this class...");
	}
	
	public static List<String> parseFormula(String line) throws ParseException {
		logger.info("Parsing the formula definition: {}", line);
		line = StringUtil.ifEmptyMakeItNull(line);
		if(line == null) { throw new IllegalArgumentException("Formula cannot be null... "); }
        List<String> qu = extractFormulaLexicons(line);
        logger.info("The extracted formula lexicons are: {}", qu);
        int i =0;
        while(i< qu.size()) {
            String str = qu.get(i);
            switch(str) {
                case "[": 
                	validateOpenSquareBracketLexiconPosition(i,line,qu);
                    i++;
                    break;
                case "]": 
                	validateCloseSquareBracketLexiconPosition(i,line,qu);
                    i++;
                    break;
                case "&": 
                	validateAmpersandLexiconPosition(i,line,qu);
                    i++;
                    break;
                case "\"": 
                	validateQuotesLexiconPosition(i,line,qu);
                    i+=2;
                    break;
                case DOT_LABEL: 
                	validateDotLabelLexiconPosition(i,line,qu);
                    i++;
                    break; 
                default:
                	i++;
                	break;
            }
        }
        List<String> parsedFields = extractFields(line);
        logger.info("The extracted fields from the formula are: {}", parsedFields);
        String reverseEngineeredFormula = getReverseEngineeredFormula(parsedFields);
        logger.info("The reverse engineered formula from the extracted fields is: {}", reverseEngineeredFormula);
        if (!reverseEngineeredFormula.equals(line)) { 
        	throw new ParseException(
        			String.format("Given formula %s does not match the reverse engineered formula %s",
        					line, reverseEngineeredFormula), 
        			0); 
        }
        return parsedFields;
	}
	
	private static String getReverseEngineeredFormula(List<String> parsedFields) {
		StringBuilder sb = new StringBuilder();
		for(int i =0;i< parsedFields.size(); i++) {
			if(i == parsedFields.size()-1 || parsedFields.get(i+1).equals(DOT_LABEL)) {
			    sb.append(parsedFields.get(i));
			}
			else {
				sb.append(parsedFields.get(i) + "&");
			}
		}
		return sb.toString();
	}

	private static void validateOpenSquareBracketLexiconPosition(int i, String line, List<String> qu) throws ParseException {
	    if(i == qu.size()-1 || !qu.get(i+1).equals("]")) {
            throw new ParseException(String.format(ILLEGAL_LEXICON_FOUND, "[", i, line), i);
        }
	}
	
	private static void validateCloseSquareBracketLexiconPosition(int i, String line, List<String> qu) throws ParseException {
		if(i ==0 || (i < qu.size()-1 && !qu.get(i+1).equals("&") && !qu.get(i+1).equals(DOT_LABEL))) {
            throw new ParseException(String.format(ILLEGAL_LEXICON_FOUND, "]", i, line), i);
        }
	}
	
	private static void validateAmpersandLexiconPosition(int i, String line, List<String> qu) throws ParseException {
		if(i ==0 || i ==qu.size()-1 || (!qu.get(i+1).equals("\"") && !qu.get(i+1).equals("["))) {
            throw new ParseException(String.format(ILLEGAL_LEXICON_FOUND, "&", i, line), i);
        }
	}
	
	private static void validateQuotesLexiconPosition(int i, String line, List<String> qu) throws ParseException {
		if(i ==qu.size()-1 || !qu.get(i+1).equals("\"") || (i< qu.size()-2 && !qu.get(i+2).equals("&"))) {
            throw new ParseException(String.format(ILLEGAL_LEXICON_FOUND, "\"", i, line), i);
        }
	}
	
	private static void validateDotLabelLexiconPosition(int i, String line, List<String> qu) throws ParseException {
		if(i ==0 || (i < qu.size()-1 && !qu.get(i+1).equals("&"))) {
            throw new ParseException(String.format(ILLEGAL_LEXICON_FOUND, DOT_LABEL, i, line), i);
        }
	}
		
	private static List<String> extractFormulaLexicons(String line) throws ParseException {
		List<String> qu = new LinkedList<>();
		int i =0;
		while(i< line.length()) {
			if(line.charAt(i) == '[') {
				qu.add("[");
				int j=i+1;
				for(; j<line.length();j++) {
					if(line.charAt(j) == ']') {
						qu.add("]");
						break;
					}
				}
				i=j;
			}
			else if(line.charAt(i) == '"') {
				qu.add("\"");
				int j=i+1;
				for(; j<line.length();j++) {
					if(line.charAt(j) == '"') {
						qu.add("\"");
						break;
					}
				}
				i=j;	
			}
			else if(line.charAt(i) == '&') {
				qu.add("&");
			}
			else if(line.charAt(i) == '.') {
				if(i>= line.length()-5 || !line.substring(i, i+6).equals(DOT_LABEL)) { 
					throw new ParseException(String.format(
							"Required .Label lexicon component Label is missing in the formula %s at position %s", line, i),
							i); 
				}
			    qu.add(DOT_LABEL);
			    i+=5;
			}
			i++;
		}
		return qu;
	}
	
	private static List<String> extractFields(String fd) throws ParseException {
		List<String> ls = new ArrayList<>();
		int i=0;
		while(i < fd.length()) {
            StringBuilder sb = new StringBuilder();
            Character car = fd.charAt(i);
            switch(car) {
                case '[':
                    i = extractFieldInsideSquareBrackets(i,fd,sb);
                    break;
                case '"':
                	i = extractFieldInsideQuotes(i,fd,sb);
                    break;
                case '.':
                    i = extractFieldInsideDot(i,fd,sb);
                    break;
                default :
                	i++;
                	break;
            }
            if(sb.length() > 0) ls.add(sb.toString());
        }
        return ls;
    }
	
	private static int extractFieldInsideSquareBrackets(int i, String fd, StringBuilder sb) throws ParseException {
		StringBuilder strb = new StringBuilder();
        for(int j=i+1;;j++) {
            if(fd.charAt(j) == ']') {
                i=j+1;
                break;
            }
            strb.append(fd.charAt(j));
        }
        if(strb.length() <1 || !strb.toString().equals(strb.toString().trim())) { 
        	throw new ParseException(String.format(
        			"Illegal to have blank field inside square bracket in formula %s at position %s", fd, i),
        			i); 
        }
        sb.append("[");
        sb.append(strb.toString().trim());
        sb.append("]");
		return i;	
	}
	
	private static int extractFieldInsideQuotes(int i, String fd, StringBuilder sb) {
		sb.append("\"");
        for(int j=i+1;;j++) {
            if(fd.charAt(j) == '"') {
            	sb.append("\"");
                i=j+1;
                break;
            }
            sb.append(fd.charAt(j));
        }
		return i;	
	}
	
	private static int extractFieldInsideDot(int i, String fd, StringBuilder sb) {
		sb.append(fd.substring(i, i+6));
		i+=6;
        return i; 	
	}

}
