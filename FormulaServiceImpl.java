package com.unilog.cx1.pim.commons.service.impl;

import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.CATEGORY_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.CATEGORY_CODE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ATTRIBUTES;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ATTRIBUTE_NAME;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ATTRIBUTE_UOM;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ATTRIBUTE_VALUE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.COUNTRY_OF_SALE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.CUSTOM_KEYWORDS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.CUSTOM_KEYWORD_TYPE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.DB_STANDARD_APPROVALS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.FEATURE_BULLETS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.HEIGHT;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.INCLUDES;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.INVC_DESCRIPTION;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.KEYWORDS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.LENGTH;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MANUFACTURER_PART_NUMBER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MFR_PART_NUMBER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MIN_ORDER_QTY;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MRKT_DESCRIPTION;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ORDER_QTY_INTERVAL;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PACKAGE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PACKAGE_DESCRIPTION;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PACKAGE_QTY;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PACK_DESCRIPTION;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PARTNUMBER_KEYWORD_TYPE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER_KEYWORDS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.QTY_AVAILABLE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.SUBSCRIBER_CUSTOM_KEYWORD_TYPE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.VOLUME;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.WEIGHT;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.WIDTH;
import static com.unilog.prime.jooq.tables.SiClientXField.SI_CLIENT_X_FIELD;
import static com.unilog.prime.jooq.tables.SiSubsetQuery.SI_SUBSET_QUERY;
import static com.unilog.prime.jooq.tables.TmbCategoryField.TMB_CATEGORY_FIELD;
import static com.unilog.prime.jooq.tables.TmbClientAttribute.TMB_CLIENT_ATTRIBUTE;
import static com.unilog.prime.jooq.tables.TmbClientCategory.TMB_CLIENT_CATEGORY;
import static com.unilog.prime.jooq.tables.TmbFormula.TMB_FORMULA;
import static com.unilog.prime.jooq.tables.TmbClientTaxonomyTreeNode.TMB_CLIENT_TAXONOMY_TREE_NODE;
import static com.unilog.prime.jooq.tables.TmbLuFieldNames.TMB_LU_FIELD_NAMES;
import static org.jooq.impl.DSL.table;
import static com.unilog.prime.jooq.tables.SiClientItem.SI_CLIENT_ITEM;
import static com.unilog.prime.jooq.tables.SiItemCategory.SI_ITEM_CATEGORY;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.lang.StringUtils;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.impl.DSL;
import org.jooq.Record;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unilog.cx1.pim.commons.service.IFormulaService;
import com.unilog.prime.commons.enumeration.PrimeResponseCode;
import com.unilog.prime.commons.exception.PrimeException;
import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.commons.util.StringUtil;
import com.unilog.prime.jooq.tables.records.SiClientXFieldRecord;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;
import com.unilog.prime.jooq.tables.records.TmbCategoryFieldRecord;
import com.unilog.prime.jooq.tables.records.TmbClientAttributeRecord;
import com.unilog.prime.jooq.tables.records.TmbClientCategoryRecord;
import com.unilog.prime.jooq.tables.records.TmbFormulaRecord;
import com.unilog.prime.jooq.tables.records.TmbLuFieldNamesRecord;

/**
 * @author Abhinav Saurabh
 *
 */
@Service(FormulaServiceImpl.BEAN_ID)
public class FormulaServiceImpl implements IFormulaService {
	
	public static final String BEAN_ID = "formulaService";
	private static final Logger logger = LoggerFactory.getLogger(FormulaServiceImpl.class);	
	private static final String ILLEGAL_LEXICON_FOUND = "Illegal lexicon found near %s at position %s in the given formula %s";
	private static final String DOT_LABEL = ".Label";
	private static final String FIELD_VALUE = "FIELD_VALUE";
	private static final String FIELD_ID = "FIELD_ID";
	private static final String X_FIELD_VALUES = "X_FIELD_VALUES";
	private static final String KEYWORD = "keyword";
	private static final String KEYWORD_TYPE = "keywordType";
	private static final String REPLACE_CONSTANT = "###REPLACE###";
	private static final String LABEL = "_Label";
	private static final Map<String, String> BASE_FIELDS_MAP = new HashMap<>();
	private static final Map<ULong,String> BASE_FIELDS = new HashMap<>();
	
	static {
		BASE_FIELDS_MAP.put("ITEM_ID", "ID");
		BASE_FIELDS_MAP.put(MANUFACTURER_PART_NUMBER, MFR_PART_NUMBER);
		BASE_FIELDS_MAP.put(PACKAGE_DESCRIPTION, PACK_DESCRIPTION);
		BASE_FIELDS_MAP.put(LENGTH, PACKAGE + LENGTH);
		BASE_FIELDS_MAP.put(VOLUME, VOLUME);
		BASE_FIELDS_MAP.put(HEIGHT, PACKAGE + HEIGHT);
		BASE_FIELDS_MAP.put(WIDTH, PACKAGE + WIDTH);
		BASE_FIELDS_MAP.put(WEIGHT, PACKAGE + WEIGHT);
		BASE_FIELDS_MAP.put("MIN_ORDER_QUANTITY", MIN_ORDER_QTY);
		BASE_FIELDS_MAP.put("ORDER_QUANTITY_AVAILABLE", ORDER_QTY_INTERVAL);
		BASE_FIELDS_MAP.put("PACKAGE_QUANTITY", PACKAGE_QTY);
		BASE_FIELDS_MAP.put("QUANTITY_AVAILABLE", QTY_AVAILABLE);
		BASE_FIELDS_MAP.put("INVOICE_DESCRIPTION", INVC_DESCRIPTION);
		BASE_FIELDS_MAP.put("MARKETING_DESCRIPTION", MRKT_DESCRIPTION);
		BASE_FIELDS_MAP.put("COUNTRY_OF_SALES", COUNTRY_OF_SALE);
		BASE_FIELDS_MAP.put("CERTIFICATIONS/STANDARDS", DB_STANDARD_APPROVALS);
		BASE_FIELDS_MAP.put("PACKAGE/BOX_INCLUDES", INCLUDES);
		BASE_FIELDS_MAP.put("FEATURES", FEATURE_BULLETS);
		
		BASE_FIELDS.put(ULong.valueOf(1), "Data Source");
		BASE_FIELDS.put(ULong.valueOf(2), "Application");
		BASE_FIELDS.put(ULong.valueOf(3), "Country of origin");
		BASE_FIELDS.put(ULong.valueOf(4), "Country of sales");
		BASE_FIELDS.put(ULong.valueOf(5), "Features");
		BASE_FIELDS.put(ULong.valueOf(6), "Keywords");
		BASE_FIELDS.put(ULong.valueOf(7), "Part Number Keywords");
		BASE_FIELDS.put(ULong.valueOf(8), "Custom Keywords");
		BASE_FIELDS.put(ULong.valueOf(9), "Length");
		BASE_FIELDS.put(ULong.valueOf(10), "Width");
		BASE_FIELDS.put(ULong.valueOf(11), "Height");
		BASE_FIELDS.put(ULong.valueOf(12), "Volume");
		BASE_FIELDS.put(ULong.valueOf(13), "Weight");
		BASE_FIELDS.put(ULong.valueOf(14), "UPC");
		BASE_FIELDS.put(ULong.valueOf(15), "UNSPSC");
		BASE_FIELDS.put(ULong.valueOf(16), "Long description");
		BASE_FIELDS.put(ULong.valueOf(17), "Marketing description");
		BASE_FIELDS.put(ULong.valueOf(18), "Invoice description");
		BASE_FIELDS.put(ULong.valueOf(19), "Short description");
		BASE_FIELDS.put(ULong.valueOf(20), "Manufacturer part number");
		BASE_FIELDS.put(ULong.valueOf(21), "Warranty");
		BASE_FIELDS.put(ULong.valueOf(22), "Manufacturer Status");
		BASE_FIELDS.put(ULong.valueOf(23), "Quantity available");
		BASE_FIELDS.put(ULong.valueOf(24), "Min order quantity");
		BASE_FIELDS.put(ULong.valueOf(25), "My part number");
		BASE_FIELDS.put(ULong.valueOf(26), "Order quantity interval");
		BASE_FIELDS.put(ULong.valueOf(27), "Sales UOM");
		BASE_FIELDS.put(ULong.valueOf(28), "Package description");
		BASE_FIELDS.put(ULong.valueOf(29), "Package quantity");
		BASE_FIELDS.put(ULong.valueOf(30), "Package/box includes");
		BASE_FIELDS.put(ULong.valueOf(31), "Certifications/Standards");
		BASE_FIELDS.put(ULong.valueOf(32), "Brand Name");
		BASE_FIELDS.put(ULong.valueOf(33), "Manufacturer Name");
		BASE_FIELDS.put(ULong.valueOf(34), "Enriched Indicator");
		BASE_FIELDS.put(ULong.valueOf(35), "Alternate part number 1");
		BASE_FIELDS.put(ULong.valueOf(36), "Alternate part number 2");
		BASE_FIELDS.put(ULong.valueOf(37), "Discontinued MPN");
	}

	@Autowired 
	private DSLContext dslContext;
	
	@Autowired 
	private ObjectMapper objectMapper;
		
	@Override
	public List<String> parseFormula(String line) throws ParseException {
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
	
	@Override
	public Tuple<String,String> getEncodedNDecodedFormulaPair(ULong clientId, List<String> parsedFields) throws ParseException {
		Set<String> valueFields = new HashSet<>();
		parsedFields.forEach(e-> {
			StringBuilder firstNLast = new StringBuilder();
        	firstNLast.append(e.charAt(0));
        	firstNLast.append(e.charAt(e.length()-1));
			if(firstNLast.toString().equals("[]")) {
				valueFields.add(e.substring(1, e.length()-1));
			}
		});
		final Map<ULong, String> attributes = this.getAttributesMap(clientId, valueFields);
		final Map<ULong, String> xFields = this.getXFieldsMap(clientId);
		final Map<ULong, String> baseFields = BASE_FIELDS;
		final Map<String, ULong> invertedAttributes = this.getInvertedMap(attributes);
		final Map<String, ULong> invertedXFields = this.getInvertedMap(xFields);
		final Map<String, ULong> invertedBaseFields = this.getInvertedMap(baseFields);
		String encodedFormula = this.getEncodedFormula(parsedFields, invertedAttributes, invertedXFields, invertedBaseFields);
		String decodedFormula = this.getDecodedFormula(this.parseFormula(encodedFormula), attributes, xFields, baseFields);
		return new Tuple<>(encodedFormula, decodedFormula);
	}
	
    private String getEncodedFormula(List<String> parsedFields, final Map<String, ULong> invertedAttributes, 
    		final Map<String, ULong> invertedXFields, final Map<String, ULong> invertedBaseFields) {
		List<String> encodedFieldsList = new ArrayList<>();
		int i= 0;
		while(i< parsedFields.size()) {
			String field = parsedFields.get(i);
			StringBuilder firstNLast = new StringBuilder();
        	firstNLast.append(field.charAt(0));
        	firstNLast.append(field.charAt(field.length()-1));
			if(firstNLast.toString().equals("[]")) {
				String fieldName = field.substring(1, field.length()-1);
				if(invertedAttributes.containsKey(fieldName))
					encodedFieldsList.add("[aid:" + invertedAttributes.get(fieldName) + "]");
				else if(invertedXFields.containsKey(fieldName))
					encodedFieldsList.add("[xid:" + invertedXFields.get(fieldName) + "]");
				else if(invertedBaseFields.containsKey(fieldName))
					encodedFieldsList.add("[bid:" + invertedBaseFields.get(fieldName) + "]");
				else if(i< parsedFields.size()-1 && parsedFields.get(i+1).equals(DOT_LABEL))
					i++;
			} else if(firstNLast.toString().equals("\"\"")) {
				encodedFieldsList.add(field);
			} else if(field.equals(DOT_LABEL)) {
				int lastIndex = encodedFieldsList.size()-1;
				String lastValue = encodedFieldsList.get(lastIndex);
				encodedFieldsList.set(lastIndex, lastValue + field);
			}
			i++;
		}
		return encodedFieldsList.stream().collect(Collectors.joining("&"));
	}
	
	private String getDecodedFormula(List<String> encodedFields, final Map<ULong, String> attributes, 
			final Map<ULong, String> xFields, final Map<ULong, String> baseFields) {
		List<String> decodedFieldsList = new ArrayList<>();
		int i= 0;
		while(i< encodedFields.size()) {
			String field = encodedFields.get(i);
			StringBuilder firstNLast = new StringBuilder();
        	firstNLast.append(field.charAt(0));
        	firstNLast.append(field.charAt(field.length()-1));
			if(firstNLast.toString().equals("[]")) {
				String fieldIndicator = field.substring(1,4);
				ULong fieldId = ULong.valueOf(field.substring(5, field.length()-1));
				if(fieldIndicator.equals("aid") && attributes.containsKey(fieldId))
					decodedFieldsList.add("[" + attributes.get(fieldId) + "]");
				else if(fieldIndicator.equals("xid") && xFields.containsKey(fieldId))
					decodedFieldsList.add("[" + xFields.get(fieldId) + "]");
				else if(fieldIndicator.equals("bid") && baseFields.containsKey(fieldId))
					decodedFieldsList.add("[" + baseFields.get(fieldId) + "]");
				else if(i< encodedFields.size()-1 && encodedFields.get(i+1).equals(DOT_LABEL))
					i++;	
			} else if(firstNLast.toString().equals("\"\"")) {
				decodedFieldsList.add(field);
			} else if(field.equals(DOT_LABEL)) {
				int lastIndex = decodedFieldsList.size()-1;
				String lastValue = decodedFieldsList.get(lastIndex);
				decodedFieldsList.set(lastIndex, lastValue + field);
			}
			i++;
		}
		return decodedFieldsList.stream().collect(Collectors.joining("&"));
	}
	
	private Map<String, ULong> getInvertedMap(Map<ULong, String> map) {
		return map.entrySet()
			       .stream()
			       .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey)); 
	}
	
	private Map<ULong,String> getAttributesMap(ULong clientId, Set<String> fields) {
		Map<ULong, String> attributes = new HashMap<>();
		final Map<ULong, TmbClientAttributeRecord> attributeRecords = this.getAttributes(clientId,fields);
		attributeRecords.keySet().stream().forEach(id -> 
			attributes.put(id, attributeRecords.get(id).getAttributeName()));
		return attributes;
	}
	
	private Map<ULong, String> getXFieldsMap(ULong clientId) {
		Map<ULong, String> xFields = new HashMap<>();
		final Map<ULong, SiClientXFieldRecord> xFieldRecords = this.getXFields(clientId);
		xFieldRecords.keySet().stream().forEach(id -> 
			xFields.put(id, xFieldRecords.get(id).getFieldName()));
		return xFields;
	}
	
	private String getReverseEngineeredFormula(List<String> parsedFields) {
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

	private void validateOpenSquareBracketLexiconPosition(int i, String line, List<String> qu) throws ParseException {
	    if(i == qu.size()-1 || !qu.get(i+1).equals("]")) {
            throw new ParseException(String.format(ILLEGAL_LEXICON_FOUND, "[", i, line), i);
        }
	}
	
	private void validateCloseSquareBracketLexiconPosition(int i, String line, List<String> qu) throws ParseException {
		if(i ==0 || (i < qu.size()-1 && !qu.get(i+1).equals("&") && !qu.get(i+1).equals(DOT_LABEL))) {
            throw new ParseException(String.format(ILLEGAL_LEXICON_FOUND, "]", i, line), i);
        }
	}
	
	private void validateAmpersandLexiconPosition(int i, String line, List<String> qu) throws ParseException {
		if(i ==0 || i ==qu.size()-1 || (!qu.get(i+1).equals("\"") && !qu.get(i+1).equals("["))) {
            throw new ParseException(String.format(ILLEGAL_LEXICON_FOUND, "&", i, line), i);
        }
	}
	
	private void validateQuotesLexiconPosition(int i, String line, List<String> qu) throws ParseException {
		if(i ==qu.size()-1 || !qu.get(i+1).equals("\"") || (i< qu.size()-2 && !qu.get(i+2).equals("&"))) {
            throw new ParseException(String.format(ILLEGAL_LEXICON_FOUND, "\"", i, line), i);
        }
	}
	
	private void validateDotLabelLexiconPosition(int i, String line, List<String> qu) throws ParseException {
		if(i ==0 || (i < qu.size()-1 && !qu.get(i+1).equals("&"))) {
            throw new ParseException(String.format(ILLEGAL_LEXICON_FOUND, DOT_LABEL, i, line), i);
        }
	}
	
	private List<String> extractFormulaLexicons(String line) throws ParseException {
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
	
	private List<String> extractFields(String fd) throws ParseException {
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
	
	private int extractFieldInsideSquareBrackets(int i, String fd, StringBuilder sb) throws ParseException {
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
	
	private int extractFieldInsideQuotes(int i, String fd, StringBuilder sb) {
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
	
	private int extractFieldInsideDot(int i, String fd, StringBuilder sb) {
		sb.append(fd.substring(i, i+6));
		i+=6;
        return i; 	
	}

	@Override
    public ULong getClientAgainstFormula(ULong id) {
		Record1<ULong> clientIdRecord = this.dslContext.select(TMB_CLIENT_CATEGORY.CLIENT_ID).from(TMB_CLIENT_CATEGORY)
				.innerJoin(TMB_CATEGORY_FIELD).on(TMB_CATEGORY_FIELD.CATEGORY_ID.eq(TMB_CLIENT_CATEGORY.ID))
				.innerJoin(TMB_FORMULA).on(TMB_CATEGORY_FIELD.ID.eq(TMB_FORMULA.CATEGORY_FIELD_ID))
				.where(TMB_FORMULA.ID.eq(id)).fetchOne();
		if(clientIdRecord == null) {
			throw new PrimeException(HttpStatus.PRECONDITION_FAILED,
					String.format(PrimeResponseCode.RESOURCE_NOT_FOUND.getResponseMsg(), id.toString()),
					PrimeResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
		}
    	return clientIdRecord.into(ULong.class);
	}
    
    @Override
    public ULong getClientAgainstCategoryField(ULong id) {
		Record1<ULong> clientIdRecord = this.dslContext.select(TMB_CLIENT_CATEGORY.CLIENT_ID).from(TMB_CLIENT_CATEGORY)
				.innerJoin(TMB_CATEGORY_FIELD).on(TMB_CATEGORY_FIELD.CATEGORY_ID.eq(TMB_CLIENT_CATEGORY.ID))
				.where(TMB_CATEGORY_FIELD.ID.eq(id)).fetchOne();
		if(clientIdRecord == null) {
			throw new PrimeException(HttpStatus.PRECONDITION_FAILED,
					String.format(PrimeResponseCode.RESOURCE_NOT_FOUND.getResponseMsg(), id.toString()),
					PrimeResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
		}
    	return clientIdRecord.into(ULong.class);
    }
    
    @Override
    public TmbFormulaRecord getFormulaById(ULong id) {
    	return this.dslContext.selectFrom(TMB_FORMULA).where(TMB_FORMULA.ID.eq(id)).fetchOne();	
    }
    
    @Override
    public TmbCategoryFieldRecord getCategoryFieldById(ULong id) {
    	return this.dslContext.selectFrom(TMB_CATEGORY_FIELD).where(TMB_CATEGORY_FIELD.ID.eq(id)).fetchOne();	
    }
    
    @Override
    public TmbClientCategoryRecord getLeafCategoryRecord(ULong categoryId, ULong clientId) {
    	if(isLeafNode(categoryId, clientId)) 
    	    return this.dslContext.selectFrom(TMB_CLIENT_CATEGORY)
    			    .where(TMB_CLIENT_CATEGORY.CLIENT_ID.in(clientId)
    					    .and(TMB_CLIENT_CATEGORY.ID.eq(categoryId)))
    			    .fetchOne();
    	else return null;
    }
    
    @Override
    public List<ULong> getLeafNodeIds(ULong clientId) {
    	return this.dslContext.selectDistinct(TMB_CLIENT_TAXONOMY_TREE_NODE.CATEGORY_ID)
    			.from(TMB_CLIENT_TAXONOMY_TREE_NODE)
    			.where(TMB_CLIENT_TAXONOMY_TREE_NODE.CLIENT_ID.eq(clientId)
    					.and(TMB_CLIENT_TAXONOMY_TREE_NODE.IS_LEAF_NODE.eq(Byte.valueOf("1"))))
    			.fetchInto(ULong.class);
    }
    
    @Override
    public List<ULong> getCategoryFieldIds(ULong clientId) {
    	return this.dslContext.select(TMB_CATEGORY_FIELD.ID)
    			.from(TMB_CATEGORY_FIELD)
    			.innerJoin(TMB_CLIENT_CATEGORY)
    			.on(TMB_CATEGORY_FIELD.CATEGORY_ID.eq(TMB_CLIENT_CATEGORY.ID))
    			.where(TMB_CLIENT_CATEGORY.CLIENT_ID.eq(clientId))
    			.fetchInto(ULong.class);
    }
    
    @Override
    public List<ULong> getCategoryFieldCategoryFieldIds(ULong clientId) {
    	return this.dslContext.select(TMB_CATEGORY_FIELD.CATEGORY_ID)
    			.from(TMB_CATEGORY_FIELD)
    			.innerJoin(TMB_CLIENT_CATEGORY)
    			.on(TMB_CATEGORY_FIELD.CATEGORY_ID.eq(TMB_CLIENT_CATEGORY.ID))
    			.where(TMB_CLIENT_CATEGORY.CLIENT_ID.eq(clientId))
    			.fetchInto(ULong.class);
    }
    
    @Override
    public boolean isLeafNode(ULong categoryId, ULong clientId) {
        List<Integer> isLeafNode = this.dslContext.select(TMB_CLIENT_TAXONOMY_TREE_NODE.IS_LEAF_NODE)
    			.from(TMB_CLIENT_TAXONOMY_TREE_NODE)
    			.where(TMB_CLIENT_TAXONOMY_TREE_NODE.CLIENT_ID.eq(clientId)
    					.and(TMB_CLIENT_TAXONOMY_TREE_NODE.CATEGORY_ID.eq(categoryId)))
    			.fetchInto(Integer.class);
    	return isLeafNode.contains(1); 
    }
    
    @Override
    public void incrementOrDecrementFormulaCount(ULong id, boolean increment) {
    	int incree = (increment)? 1 : -1; 
    	TmbCategoryFieldRecord categoryFieldRecord = this.getCategoryFieldById(id);
		this.dslContext.update(TMB_CATEGORY_FIELD)
		.set(TMB_CATEGORY_FIELD.FORMULA_COUNT, categoryFieldRecord.getFormulaCount() + incree)
		.where(TMB_CATEGORY_FIELD.CATEGORY_ID.eq(categoryFieldRecord.getCategoryId()))
		.execute();
    }
    
    @Override
    public TmbLuFieldNamesRecord getFieldNamesById(ULong id) {
    	return this.dslContext.selectFrom(TMB_LU_FIELD_NAMES).where(TMB_LU_FIELD_NAMES.ID.eq(id)).fetchOne();	
    }
    
    @Override
    public TmbLuFieldNamesRecord getFieldNamesByFieldName(String name) {
    	return this.dslContext.selectFrom(TMB_LU_FIELD_NAMES).where(TMB_LU_FIELD_NAMES.FIELD_NAME.eq(name)).fetchOne();	
    }
    
    @Override
    public Result<TmbLuFieldNamesRecord> getFieldNamesByFieldNamePartial(String name) {
    	return this.dslContext.selectFrom(TMB_LU_FIELD_NAMES).where(TMB_LU_FIELD_NAMES.FIELD_NAME.like(name + "%")).fetch();	
    }
    
    @Override
    public Record getFormulaByUniqueIndex(ULong categoryFieldId, ULong fieldId) {
    	return this.dslContext.select(TMB_FORMULA.fields())
				.from(TMB_FORMULA)
				.innerJoin(TMB_LU_FIELD_NAMES)
				.on(TMB_FORMULA.FIELD_ID.eq(TMB_LU_FIELD_NAMES.ID))
				.where(TMB_FORMULA.CATEGORY_FIELD_ID.eq(categoryFieldId)
						.and(TMB_LU_FIELD_NAMES.ID.eq(fieldId)))
				.fetchOne();	
    }
    
    @Override
    public String calculateFieldValueFromFormula(ULong clientId, String itemId, List<String> parsedFields, Set<String> valueFields) {
		String exportQuery = this.getExportQuery(ULong.valueOf(1)).replace(REPLACE_CONSTANT, "\"" + itemId + "\"");
		Map<String, Object> data = this.dslContext.fetchOne(exportQuery).intoMap();
		this.processData(clientId, data);
		this.processAttributeLabels(clientId, valueFields, data);
		return appendValues(parsedFields, data);
    }
    
    private String appendValues(List<String> parsedFields, Map<String, Object> data) { 
    	StringBuilder processFormula = new StringBuilder();
        for(int i=0; i < parsedFields.size(); i++) {
        	String field = parsedFields.get(i);
        	String fieldNormal = field.substring(1, field.length()-1);
        	String fieldUpperSnake = field.substring(1, field.length()-1).replace(" ", "_").toUpperCase();
        	StringBuilder firstNLast = new StringBuilder();
        	firstNLast.append(field.charAt(0));
        	firstNLast.append(field.charAt(field.length()-1));
        	if(firstNLast.toString().equals("[]") && i < parsedFields.size()-1 && parsedFields.get(i+1).equals(DOT_LABEL) 
        			&& data != null && !data.isEmpty()) {
        		appendValuesForDotlabel(processFormula, data, fieldNormal);
        	}
        	else if(firstNLast.toString().equals("[]") && data != null && !data.isEmpty()) {
        		if(data.containsKey(fieldUpperSnake) && data.get(fieldUpperSnake) != null)
        			processFormula.append(data.get(fieldUpperSnake));
        		else if (data.containsKey(fieldNormal) && data.get(fieldNormal) != null)
        			processFormula.append(data.get(fieldNormal));	
        	}
        	else if(firstNLast.toString().equals("\"\"")) {
        		processFormula.append(fieldNormal);
        	}
        }
		return processFormula.toString();		
    }
    
    private void appendValuesForDotlabel(StringBuilder processFormula, Map<String,Object> data, String field) {
    	if(data.containsKey(field + LABEL) && data.get(field + LABEL) != null) { processFormula.append(data.get(field + LABEL)); }
    	else { processFormula.append(field); }
    }
	
	private void processData(ULong clientId, Map<String, Object> data) {
		this.processBaseFields(data);
		this.processKeywords(data);
		this.processAttributes(data);
		final Map<ULong, SiClientXFieldRecord> xFields = this.getXFields(clientId);
		if(xFields !=null && !xFields.isEmpty()) { this.processXFields(data, xFields); }	
	}
	
	private void processAttributes(Map<String, Object> data) {
		String attributes = StringUtil.safeValueOf(data.get(ATTRIBUTES));
		if (attributes == null)
			return;
		try {
			@SuppressWarnings("rawtypes")
			HashMap[] objs = this.objectMapper.readValue(attributes, HashMap[].class);
			IntStream.range(0, objs.length).forEach(e -> {
				StringBuilder sb = new StringBuilder();
				if(objs[e].get(ATTRIBUTE_VALUE) != null) {
				    sb.append(objs[e].get(ATTRIBUTE_VALUE));
				    if(objs[e].get(ATTRIBUTE_UOM) != null && !objs[e].get(ATTRIBUTE_UOM).toString().isBlank()) {
				        sb.append(" ");
				        sb.append(objs[e].get(ATTRIBUTE_UOM));
				    }
				    data.put(objs[e].get(ATTRIBUTE_NAME).toString(), sb);
				}
			});
		} catch (IOException e) {
			logger.error("Unable to convert the attributes : {}", attributes);
		}
	}
	
	@SuppressWarnings("rawtypes")
	private void processXFields(Map<String, Object> data, Map<ULong, SiClientXFieldRecord> xFields) {
		Object fieldJSONObject = data.get(X_FIELD_VALUES);
		if (fieldJSONObject == null)
			return;
		try {
			HashMap[] objs = this.objectMapper.readValue(fieldJSONObject.toString(), HashMap[].class);
			Stream.of(objs).filter(e -> Objects.nonNull(e.get(FIELD_ID)))
					.filter(e -> Objects.nonNull(e.get(FIELD_VALUE)))
					.filter(e -> xFields.containsKey(ULong.valueOf(e.get(FIELD_ID).toString()))).forEach(e -> {
						SiClientXFieldRecord xfieldRecord = xFields.get(ULong.valueOf(e.get(FIELD_ID).toString()));
						data.put(xfieldRecord.getFieldName(),e.get(FIELD_VALUE));
					});
		} catch (IOException e) {
			logger.error("Unable to convert the xFields json : {}", fieldJSONObject);
		}
	}
	
	@SuppressWarnings("unchecked")
	private void processKeywords(Map<String, Object> data) {
		String keywords = StringUtil.safeValueOf(data.get(KEYWORDS));
		if (keywords == null)
			return;
		try {
			keywords = StringUtil.escapeBackslash(keywords);
			keywords = StringUtil.escapeInchesCharacter(keywords);
			@SuppressWarnings("rawtypes")
			Map[] objs = this.objectMapper.readValue(keywords, HashMap[].class);
			Object partNumberKeyword = Stream.of(objs).filter(e -> e.get(KEYWORD_TYPE).equals(PARTNUMBER_KEYWORD_TYPE))
					.map(e -> e.getOrDefault(KEYWORD, "")).findFirst().orElse("");
			data.put(PART_NUMBER_KEYWORDS, partNumberKeyword);
			Object customKeyword = Stream.of(objs).filter(e -> e.get(KEYWORD_TYPE).equals(CUSTOM_KEYWORD_TYPE))
					.map(e -> e.getOrDefault(KEYWORD, "")).findFirst().orElse("");
			data.put(KEYWORDS, customKeyword);
			Object subscriberCustomKeyword = Stream.of(objs).filter(e -> e.get(KEYWORD_TYPE).equals(SUBSCRIBER_CUSTOM_KEYWORD_TYPE))
					.map(e -> e.getOrDefault(KEYWORD, "")).findFirst().orElse("");
			data.put(CUSTOM_KEYWORDS, subscriberCustomKeyword);
		} catch (IOException e) {
			logger.error("Unable to convert the keywords json : {}", keywords);
		}
	}
    
	private void processBaseFields(Map<String, Object> data) {
		BASE_FIELDS_MAP.keySet().stream().forEach(key -> {
			if (key.equals(LENGTH) || key.equals(VOLUME) || key.equals(WIDTH) 
					|| key.equals(WEIGHT) || key.equals(HEIGHT)) {
				StringBuilder sb = new StringBuilder();
				String uomKey = BASE_FIELDS_MAP.get(key) + "_UOM";
				if(data.get(BASE_FIELDS_MAP.get(key)) != null && data.get(uomKey) != null) {
					sb.append(data.get(BASE_FIELDS_MAP.get(key)));
					sb.append(" ");
					sb.append(data.get(uomKey));
					data.put(key, sb);
				} else {
					data.put(key, data.get(BASE_FIELDS_MAP.get(key)));
				}	
			} else {
			    data.put(key, data.get(BASE_FIELDS_MAP.get(key)));
			}
		});
	}
	
	private Map<ULong, TmbClientAttributeRecord> getAttributes(ULong clientId, Set<String> fields) {
		return this.dslContext.selectFrom(TMB_CLIENT_ATTRIBUTE).where(TMB_CLIENT_ATTRIBUTE.CLIENT_ID.eq(clientId)
				.and(TMB_CLIENT_ATTRIBUTE.ATTRIBUTE_NAME.in(fields)))
				.fetchStream().collect(Collectors.toMap(TmbClientAttributeRecord::getId, Function.identity()));
	}
	
	private Map<ULong, SiClientXFieldRecord> getXFields(ULong clientId) {
		if (clientId == null)
			return Collections.emptyMap();
		return this.dslContext.selectFrom(SI_CLIENT_X_FIELD).where(SI_CLIENT_X_FIELD.CLIENT_ID.eq(clientId))
				.fetchStream().collect(Collectors.toMap(SiClientXFieldRecord::getId, Function.identity()));
	}
	
	private void processAttributeLabels(ULong clientId, Set<String> valueFields, Map<String,Object> data) {
		List<TmbClientAttributeRecord> attrRecord =this.dslContext.select(TMB_CLIENT_ATTRIBUTE.fields())
				.from(TMB_CLIENT_ATTRIBUTE)
				.where(TMB_CLIENT_ATTRIBUTE.CLIENT_ID.eq(clientId)
						.and(TMB_CLIENT_ATTRIBUTE.ATTRIBUTE_NAME.in(valueFields)))
				.fetchInto(TmbClientAttributeRecord.class);
		if (attrRecord !=null && !attrRecord.isEmpty()) {
		    attrRecord.stream().forEach(e ->
		    	data.put(e.getAttributeName() + LABEL, e.getAbbreviation())
		    );
		}
	}
	
	private String getExportQuery(ULong subsetId) {
		return this.dslContext.select(SI_SUBSET_QUERY.ITEM_HISTORY)
				.from(SI_SUBSET_QUERY)
				.where(SI_SUBSET_QUERY.SUBSET_ID.eq(subsetId)
						.and(SI_SUBSET_QUERY.TABLE_TYPE.eq("ITEM"))
						.and(SI_SUBSET_QUERY.VIEW_TYPE.eq("EXPORT")))
				.fetchOneInto(String.class);
	}
	
	@Override
	public Map<String, String> getInvertedBaseFieldsMap() {
		return BASE_FIELDS_MAP.entrySet()
			       .stream()
			       .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
	}
	
	@Override
	public Tuple<Map<String, Object>, List<String>> getDataNHeaders(ULong clientId, Map<String,Object> data) {
		List<String> headers = new ArrayList<>();
		List<String> xFieldNames = getXFieldNames(clientId);
		Map<String,Object> modifiedDataMap = new HashMap<>();
		data.keySet().stream().forEach(key -> {
			if(xFieldNames.contains(key)) {
				headers.add(key);
				modifiedDataMap.put("EX_" + key, data.get(key));
			}
			else {
				if(BASE_FIELDS_MAP.keySet().contains(key.replace(" ", "_").toUpperCase())) {
					headers.add(BASE_FIELDS_MAP.get(key.replace(" ", "_").toUpperCase()));	
					modifiedDataMap.put(BASE_FIELDS_MAP.get(key.replace(" ", "_").toUpperCase()), data.get(key));
				} else {
					headers.add(key.replace(" ", "_").toUpperCase());
					modifiedDataMap.put(key.replace(" ", "_").toUpperCase(), data.get(key));
				}
			}	
		});
		return new Tuple<>(modifiedDataMap, headers);	
	}
	
	private List<String> getXFieldNames(ULong clientId) {
		return this.dslContext.select(SI_CLIENT_X_FIELD.FIELD_NAME)
				.from(SI_CLIENT_X_FIELD)
				.where(SI_CLIENT_X_FIELD.CLIENT_ID.eq(clientId))
				.fetchInto(String.class);
	}
	
	@Override
	public void buildComputedFormulaImportMap(ULong clientId, Map<String,Object> data, ULong subsetId) {
		subsetId = (subsetId == null) ? ULong.valueOf(1) : subsetId;
		String exportQuery = this.getExportQuery(subsetId).replace(REPLACE_CONSTANT, "\"" + data.get("ID").toString() + "\"");
		Map<String, Object> itemData;
		try {
			itemData = this.dslContext.fetchOne(exportQuery).intoMap();
		} catch(NullPointerException nullPointerException) {
			throw new NullPointerException("No precompute record found for the item");
		}
		this.processData(clientId, itemData);
		data.keySet().stream().forEach(key -> {
			if(!key.equalsIgnoreCase("ID") && !key.equalsIgnoreCase("PART_NUMBER")
					&& !key.equalsIgnoreCase("__INT_INDEX")) {
				String computedValue = computeFormula(clientId,data.get(key).toString(),itemData);
				data.put(key, computedValue);
			}
		});
	}
	
	@Override
	public List<String> getBaseFieldsList() {
		List<String> baseFieldsList = new ArrayList<>();
		BASE_FIELDS.keySet().stream().forEach(key ->
			baseFieldsList.add(BASE_FIELDS.get(key)));
		return baseFieldsList;
	}
	
    private String computeFormula(ULong clientId, String line, Map<String, Object> itemData) {
    	List<String> parsedFields = new ArrayList<>(); 
		try {
			parsedFields = parseFormula(line);
		} catch (Exception ex) {
			logger.error("Exception occured while parsing the formula: {}", line);
			return null;	
		}
		logger.info("The extracted fields are: {}", parsedFields);
		Set<String> valueFields = new HashSet<>();
		parsedFields.forEach(e-> {
			StringBuilder firstNLast = new StringBuilder();
        	firstNLast.append(e.charAt(0));
        	firstNLast.append(e.charAt(e.length()-1));
			if(firstNLast.toString().equals("[]")) {
				valueFields.add(e.substring(1, e.length()-1));
			}
		});
		this.processAttributeLabels(clientId, valueFields, itemData);
		return appendValues(parsedFields, itemData);
    }
    
    @Override
    public void refreshFormulaForCategoryField(ULong clientId, ULong categoryId) {
    	final Map<ULong,String> xFields = this.getXFieldsMap(clientId);
    	final Map<ULong,String> baseFields = BASE_FIELDS;
    	List<TmbFormulaRecord> formulaRecords = this.dslContext.select(TMB_FORMULA.fields())
    			.from(TMB_FORMULA)
    			.innerJoin(TMB_CATEGORY_FIELD)
    			.on(TMB_CATEGORY_FIELD.ID.eq(TMB_FORMULA.CATEGORY_FIELD_ID))
    			.where(TMB_CATEGORY_FIELD.CATEGORY_ID.eq(categoryId))
    			.fetchInto(TmbFormulaRecord.class);
    	formulaRecords.stream().forEach(formulaRecord -> {
    		Tuple<List<String>, List<ULong>> parsedFieldNAttributeIds = 
    				this.getParsedFieldsNAttributeIdsFromEncodedFormula(formulaRecord.getEncodedFormula());
    		final Map<ULong,String> attributes = this.getAttributesMap(parsedFieldNAttributeIds.getSecondValue());
    		String decodedFormula = this.getDecodedFormula(parsedFieldNAttributeIds.getFirstValue(), 
    				attributes, xFields, baseFields);
    		if(!decodedFormula.equals(formulaRecord.getFormulaDefinition())) {
    			this.updateFormulaDefinition(formulaRecord.getId(), decodedFormula);
    		}
    	});
    }
    
    private void updateFormulaDefinition(ULong id, String formulaDefinition) {
    	this.dslContext.update(TMB_FORMULA).set(TMB_FORMULA.FORMULA_DEFINITION, formulaDefinition)
    	.where(TMB_FORMULA.ID.eq(id))
    	.execute();
    }
    
    private Map<ULong, TmbClientAttributeRecord> getAttributes(List<ULong> attributeIds) {
		return this.dslContext.selectFrom(TMB_CLIENT_ATTRIBUTE)
				.where(TMB_CLIENT_ATTRIBUTE.ID.in(attributeIds))
				.fetchStream().collect(Collectors.toMap(TmbClientAttributeRecord::getId, Function.identity()));
	}
    
    private Map<ULong,String> getAttributesMap(List<ULong> attributeIds) {
		Map<ULong, String> attributes = new HashMap<>();
		final Map<ULong, TmbClientAttributeRecord> attributeRecords = this.getAttributes(attributeIds);
		attributeRecords.keySet().stream().forEach(id -> 
			attributes.put(id, attributeRecords.get(id).getAttributeName()));
		return attributes;
	}
    
    private Tuple<List<String>, List<ULong>> getParsedFieldsNAttributeIdsFromEncodedFormula(String encodedFormula) {
    	List<ULong> attributeIds = new ArrayList<>();
    	List<String> parsedFields = new ArrayList<>();
    	try {
    		parsedFields = parseFormula(encodedFormula);
    		parsedFields.stream().forEach(field ->{ 
    			StringBuilder firstNLast = new StringBuilder();
    	        firstNLast.append(field.charAt(0));
    	        firstNLast.append(field.charAt(field.length()-1));
    	        if(firstNLast.toString().equals("[]")) {
    				String fieldIndicator = field.substring(1,4);
    				ULong fieldId = ULong.valueOf(field.substring(5, field.length()-1));
    				if(fieldIndicator.equals("aid")) { attributeIds.add(fieldId); }
    	        } 
    		 });
    	} catch(Exception ex) {
    		logger.error("Exception occured while parsing the encoded formula: {}", ex.getMessage());
    	}
    	return new Tuple<>(parsedFields,attributeIds);
    }
    
    @Override
    public void buildDataInRequiredFormatForRetrieve(Map<String,Object> data, List<String> errors) {
        ULong categoryId = ULong.valueOf(data.get(CATEGORY_ID).toString());
        data.remove(CATEGORY_ID);
    	Map<String,Object> allFields = fetchItemAssociatedFieldsWithFormula(categoryId);
    	List<String> fields = objectMapper.convertValue(data.get("FIELDS"), new TypeReference<List<String>>(){});
    	data.remove("FIELDS");
    	List<Integer> ignore0 = new ArrayList<>(1);
    	ignore0.add(1);
    	fields.forEach(field-> {
    		if(allFields.containsKey(field) && !allFields.get(field).toString().isBlank()) {
    			data.put(field, allFields.get(field));
    			ignore0.set(0,0);
    		} else {
    			errors.add(String.format("Formula does not exist for field '%s'", field));
    		}
    	});
    	if(ignore0.get(0) == 1) {
    		throw new NullPointerException(StringUtils.join(errors, "\n"));
    	}
    }
    
    @Override
    public void buildDataInRequiredFormat(Map<String,Object> data) {
    	ULong categoryId = ULong.valueOf(data.get(CATEGORY_ID).toString());
    	data.remove(CATEGORY_ID);
    	Map<String,Object> fieldsNFormula = fetchItemAssociatedFieldsWithFormula(categoryId);
    	if(fieldsNFormula.isEmpty()) { throw new NullPointerException("Formula do not exist"); }
    	data.putAll(fieldsNFormula);
    }
    
    @Override
    public Map<String,Object> fetchItemAssociatedFieldsWithFormula(ULong categoryId) {
		List<TmbFormulaRecord> defaultCategoryFieldFormulaRecordList = this.dslContext.select(TMB_FORMULA.fields())
				.from(TMB_FORMULA)
				.innerJoin(TMB_CATEGORY_FIELD)
				.on(TMB_CATEGORY_FIELD.ID.eq(TMB_FORMULA.CATEGORY_FIELD_ID))
				.where(TMB_CATEGORY_FIELD.CATEGORY_ID.eq(categoryId))
				.fetchInto(TmbFormulaRecord.class);		
		Set<ULong> fieldIds = new HashSet<>();
		defaultCategoryFieldFormulaRecordList.stream().forEach(formulaRecord -> fieldIds.add(formulaRecord.getFieldId()));
		List<TmbLuFieldNamesRecord> fieldNames = this.dslContext.select(TMB_LU_FIELD_NAMES.fields())
				.from(TMB_LU_FIELD_NAMES)
				.where(TMB_LU_FIELD_NAMES.ID.in(fieldIds))
				.fetchInto(TmbLuFieldNamesRecord.class);
		Map<ULong,String> fieldNamesMap = new HashMap<>();
		fieldNames.stream().forEach(fieldName -> fieldNamesMap.put(fieldName.getId(),fieldName.getFieldName()));
		return buildFieldNamesWithFormula(fieldNamesMap, defaultCategoryFieldFormulaRecordList);
	}
	
	private Map<String, Object> buildFieldNamesWithFormula(Map<ULong,String> fieldNamesMap,
			List<TmbFormulaRecord> defaultCategoryFieldFormulaRecordList) {
		Map<String,Object> data = new HashMap<>();
		defaultCategoryFieldFormulaRecordList.stream().forEach(formulaRecord -> 
			data.put(fieldNamesMap.get(formulaRecord.getFieldId()), formulaRecord.getFormulaDefinition()));
		return data;
	}
	
	@Override
	public void validateFieldsForRetrieve(ULong clientId,Object fieldsObject,List<String> errors) {
		List<String> fields = objectMapper.convertValue(fieldsObject,new TypeReference<List<String>>(){});
		List<String> luFields = this.dslContext.select(TMB_LU_FIELD_NAMES.FIELD_NAME)
				.from(TMB_LU_FIELD_NAMES)
				.leftJoin(SI_CLIENT_X_FIELD)
				.on(TMB_LU_FIELD_NAMES.FIELD_NAME.eq(SI_CLIENT_X_FIELD.FIELD_NAME))
				.where(SI_CLIENT_X_FIELD.CLIENT_ID.eq(clientId)
						.or(SI_CLIENT_X_FIELD.CLIENT_ID.isNull()))
				.fetchInto(String.class);
		fields.stream().forEach(field->{
			if(!luFields.contains(field)) { errors.add(String.format("Field '%s' does not exist", field)); }
		});
		if(fields.isEmpty() || fields.size() == errors.size()) {
			throw new NullPointerException(StringUtils.join(errors, "\n"));
		}
	}
	
	@Override
	public Map<String,Object> calculateAttributes(ULong clientId,final Map<String,Object> finalData) {
		Map<String,Object> data = new HashMap<>();
		data.putAll(finalData);
		String categoryName = null;
		String categoryCode = null;
		int count = 0;
		final int MAX_COUNT = 10; 
		while (count <= MAX_COUNT) {
			String defaultCategoryKey = (count == 0) ? "DEFAULT_CATEGORY" : "DEFAULT_CATEGORY_" + count;
			String categoryNameKey = (count == 0) ? "CATEGORY_NAME" : "CATEGORY_NAME_" + count;
			String categoryCodeKey = (count == 0) ? CATEGORY_CODE : "CATEGORY_CODE_" + count;
			if(data.containsKey(defaultCategoryKey) && Boolean.TRUE.equals(data.get(defaultCategoryKey))) {
				categoryName = data.get(categoryNameKey).toString();
				categoryCode = data.get(categoryCodeKey).toString();
				break;
			} else {
				count++;
			}
		}
		if(categoryCode == null && data.containsKey(CATEGORY_CODE)) {
			categoryCode = data.get(CATEGORY_CODE).toString();
			categoryName = data.get("CATEGORY_NAME").toString();
		}
		TmbCategoryFieldRecord categoryFieldRecord = this.dslContext.select(TMB_CATEGORY_FIELD.fields())
				.from(TMB_CATEGORY_FIELD)
				.innerJoin(TMB_CLIENT_CATEGORY)
				.on(TMB_CATEGORY_FIELD.CATEGORY_ID.eq(TMB_CLIENT_CATEGORY.ID))
				.where(TMB_CLIENT_CATEGORY.CATEGORY_NAME.eq(categoryName)
						.and(TMB_CLIENT_CATEGORY.CATEGORY_CODE.eq(categoryCode))
						.and(TMB_CLIENT_CATEGORY.CLIENT_ID.eq(clientId)))
				.fetchOneInto(TmbCategoryFieldRecord.class);
		if(categoryFieldRecord == null || categoryFieldRecord.getFormulaCount()<1) { return Collections.emptyMap(); }
		Map<String,Object> fieldNFormulaMap = new HashMap<>();
		fieldNFormulaMap.put(CATEGORY_ID,categoryFieldRecord.getCategoryId());
		buildDataInRequiredFormat(fieldNFormulaMap);
		Map<String,Object> tempData = new HashMap<>();
		this.processBaseFields(data);
		this.processAttributesForTemplateData(data);
		this.processXFieldsForTemplateData(data,tempData);
		data.putAll(tempData);
		tempData.clear();
		fieldNFormulaMap.keySet().stream().forEach(key -> {
			String computedValue = computeFormula(clientId,fieldNFormulaMap.get(key).toString(),data);
			tempData.put(key, computedValue);
		});
		Tuple<Map<String,Object>, List<String>> dataNHeaders = getDataNHeaders(clientId,tempData);
		removeFields(data, dataNHeaders);
		return dataNHeaders.getFirstValue();
	}
	
	@Override
	public Tuple<Map<String,Object>, List<String>> calculateAttributes(ULong clientId, SiSubsetDefinitionRecord definition, final Map<String,Object> finalData, 
			boolean isOverwrite) {
		Map<String,Object> data = new HashMap<>();
		Tuple<ULong, Map<String,Object>> tuple;
		data.putAll(finalData);
		if(isOverwrite && data.containsKey(CATEGORY_CODE) && data.get(CATEGORY_CODE) == null)
			return new Tuple<>(); 
		if(itemExistsInTheSubset(definition,data.get("ID").toString())) 
			tuple = getItemData(definition,data.get("ID").toString());
		else 
			tuple = getItemDataFromParent(definition,data.get("ID").toString());
		Map<String,Object> itemData = tuple.getSecondValue();
		ULong subsetId = tuple.getFirstValue();
		if(itemData != null && !itemData.isEmpty())
			this.processData(clientId, itemData);
		TmbCategoryFieldRecord categoryFieldRecord = null;
		if(data.containsKey(CATEGORY_CODE) && data.get(CATEGORY_CODE) != null) {
		    categoryFieldRecord = this.dslContext.select(TMB_CATEGORY_FIELD.fields())
		    		.from(TMB_CATEGORY_FIELD)
				    .innerJoin(TMB_CLIENT_CATEGORY)
				    .on(TMB_CATEGORY_FIELD.CATEGORY_ID.eq(TMB_CLIENT_CATEGORY.ID))
				    .where(TMB_CLIENT_CATEGORY.CATEGORY_CODE.eq(data.get(CATEGORY_CODE).toString())
						    .and(TMB_CLIENT_CATEGORY.CLIENT_ID.eq(clientId)))
				    .fetchOneInto(TmbCategoryFieldRecord.class);
		} else if(itemData != null) {
			ULong categoryId = getDefaultCategoryOfTheItem(subsetId, data.get("ID").toString());
			categoryFieldRecord = this.dslContext.select(TMB_CATEGORY_FIELD.fields())
		    		.from(TMB_CATEGORY_FIELD)
				    .where(TMB_CATEGORY_FIELD.CATEGORY_ID.eq(categoryId))
				    .fetchOneInto(TmbCategoryFieldRecord.class);
		}
		if(categoryFieldRecord == null || categoryFieldRecord.getFormulaCount()<1) { return new Tuple<>(); }
		Map<String,Object> fieldNFormulaMap = new HashMap<>();
		fieldNFormulaMap.put(CATEGORY_ID,categoryFieldRecord.getCategoryId());
		buildDataInRequiredFormat(fieldNFormulaMap);
		Map<String,Object> tempData = new HashMap<>();
		this.processItemFeatures(data);
		this.processBaseFields(data);
		this.processAttributesForTemplateData(data);
		this.processXFieldsForTemplateData(data,tempData);
		data.putAll(tempData);
		tempData.clear();
		if(itemData!= null) prioritizeFields(itemData,data);
		Map<String,Object> referenceData = (itemData !=null)? itemData : data;
		fieldNFormulaMap.keySet().stream().forEach(key -> {
			String computedValue = computeFormula(clientId,fieldNFormulaMap.get(key).toString(),referenceData);
			tempData.put(key, computedValue);
		});	
		Tuple<Map<String,Object>, List<String>> dataNHeaders = getDataNHeaders(clientId,tempData);
		if(isOverwrite) removeFieldsOverwrite(data,dataNHeaders);
		else removeFields(data,dataNHeaders);
		return dataNHeaders;
	}
	
	private ULong getDefaultCategoryOfTheItem(ULong subsetId, String itemId) {
		if(subsetId.equals(ULong.valueOf(1))) {
			return dslContext.select(SI_ITEM_CATEGORY.CATEGORY_ID)
					.from(SI_ITEM_CATEGORY)
					.innerJoin(SI_CLIENT_ITEM)
					.on(SI_CLIENT_ITEM.ID.eq(SI_ITEM_CATEGORY.ITEM_ID))
					.where(SI_CLIENT_ITEM.ID.eq(itemId)
							.and(SI_ITEM_CATEGORY.DEFAULT_CATEGORY.eq(Byte.valueOf("1"))))
					.fetchOneInto(ULong.class);
		}
		Optional<Map<String, Object>> defaultCategoryData = Optional.empty();
		List<Map<String, Object>> categoryData = null;
		String categoryQuery = this.dslContext.select(SI_SUBSET_QUERY.QUERY)
				.from(SI_SUBSET_QUERY)
				.where(SI_SUBSET_QUERY.SUBSET_ID.eq(subsetId)
						.and(SI_SUBSET_QUERY.TABLE_TYPE.eq("CATEGORY"))
						.and(SI_SUBSET_QUERY.VIEW_TYPE.eq("MERGE")))
				.fetchOneInto(String.class);
		categoryQuery = categoryQuery.replace(REPLACE_CONSTANT, "\"" + itemId + "\"");
		try {
		    categoryData = this.dslContext.fetch(categoryQuery).intoMaps();
		    defaultCategoryData = categoryData.stream().filter(e -> e.get("DEFAULT_CATEGORY").equals(Byte.valueOf("1"))).findFirst();
		    if(defaultCategoryData.isPresent()) return ULong.valueOf(defaultCategoryData.get().get(CATEGORY_ID).toString());
		} catch(NullPointerException nptrex) {
			logger.error("Exception occured while fetching categoryData");
		}
		return null;
	}
	
	private Tuple<ULong,Map<String,Object>> getItemData(SiSubsetDefinitionRecord definition, String itemId) {
		ULong subsetId = definition.getId();
		String exportQuery = null;
		Map<String, Object> itemData = null;
		exportQuery = this.getExportQuery(subsetId).replace(REPLACE_CONSTANT, "\"" + itemId + "\"");
		try {
		    itemData = this.dslContext.fetchOne(exportQuery).intoMap();
		} catch(NullPointerException nptrex) {
			logger.error("Exception occured while fetching itemData");
		}
		return new Tuple<>(subsetId,itemData);
	}

	private Tuple<ULong,Map<String,Object>> getItemDataFromParent(SiSubsetDefinitionRecord definition, String itemId) {
		ULong subsetId = definition.getParentId();
		String exportQuery = null;
		Map<String, Object> itemData = null;
		if(subsetId != null) {
			exportQuery = this.getExportQuery(subsetId).replace(REPLACE_CONSTANT, "\"" + itemId + "\"");
			try {
			    itemData = this.dslContext.fetchOne(exportQuery).intoMap();
			} catch(NullPointerException nptrex) {
				logger.error("Exception occured while fetching itemData from subset id:{}",subsetId);
			}
		}
		if(subsetId == null || itemData == null || itemData.isEmpty()) {
			exportQuery = this.getExportQuery(ULong.valueOf(1)).replace(REPLACE_CONSTANT, "\"" + itemId + "\"");
			try {
			    itemData = this.dslContext.fetchOne(exportQuery).intoMap();
			} catch(NullPointerException nptrex) {
				logger.error("Exception occured while fetching itemData from subset id:{}",1);
			}
			return new Tuple<>(ULong.valueOf(1),itemData);
		}
		return new Tuple<>(subsetId,itemData);
	}
	
	private boolean itemExistsInTheSubset(SiSubsetDefinitionRecord definition, String itemId) {
		String itemId1 = null;
		if(definition.getId().equals(ULong.valueOf(1))) {
			itemId1 = dslContext.select(SI_CLIENT_ITEM.ID)
					.from(SI_CLIENT_ITEM)
					.where(SI_CLIENT_ITEM.ID.eq(itemId))
					.fetchOneInto(String.class);
		} else {
			Field<String> idField = DSL.field(DSL.name(definition.getSchemaName(), 
					definition.getItemMembersTableName(), "ID"), String.class);
			itemId1 = dslContext.select(idField)
					.from(table(DSL.name(definition.getSchemaName(), 
							definition.getItemMembersTableName())))
					.where(idField.eq(itemId))
					.fetchOneInto(String.class);
			if (itemId1 == null) {
				idField = DSL.field(DSL.name(definition.getSchemaName(), 
						definition.getPrivateMembersTableName(), "ID"), String.class);
				itemId1 = dslContext.select(idField)
						.from(table(DSL.name(definition.getSchemaName(), 
								definition.getPrivateMembersTableName())))
						.where(idField.eq(itemId))
						.fetchOneInto(String.class);
			}
		}
		return (itemId1 != null);	
	}
	
	private void removeFields(final Map<String,Object> data, Tuple<Map<String,Object>, List<String>> dataNHeaders) {
		Set<String> rejectedFields = new HashSet<>();
		Set<String> rejectedHeaders = new HashSet<>();
		Map<String,Object> calculatedData = dataNHeaders.getFirstValue();
		calculatedData.keySet().stream().forEach(key->{
			if(data.containsKey(key) && data.get(key) != null) {
				rejectedFields.add(key);
				if(key.length() >3 && key.substring(0, 3).equals("EX_")) {
					rejectedHeaders.add(key.substring(3,key.length()));
				}
				else {
					rejectedHeaders.add(key);
				}
			}
		});
		calculatedData.keySet().removeAll(rejectedFields);
		dataNHeaders.getSecondValue().removeAll(rejectedHeaders);
	}
	
	private void removeFieldsOverwrite(final Map<String,Object> data, Tuple<Map<String,Object>, List<String>> dataNHeaders) {
		Set<String> rejectedFields = new HashSet<>();
		Set<String> rejectedHeaders = new HashSet<>();
		Map<String,Object> calculatedData = dataNHeaders.getFirstValue();
		calculatedData.keySet().stream().forEach(key->{
			if(data.containsKey(key)) {
				rejectedFields.add(key);
				if(key.length() >3 && key.substring(0, 3).equals("EX_")) {
					rejectedHeaders.add(key.substring(3,key.length()));
				}
				else {
					rejectedHeaders.add(key);
				}
			}
		});
		calculatedData.keySet().removeAll(rejectedFields);
		dataNHeaders.getSecondValue().removeAll(rejectedHeaders);
	}
	
	private void processItemFeatures(Map<String,Object> data) {
		int count =1;
		StringBuilder featureBuilder = new StringBuilder();
		boolean isFirstInsertion = true;
		while(true) {	
			String key = "ITEM_FEATURES_" + count;
			if(data.containsKey(key) && data.get(key) != null && isFirstInsertion) {
				featureBuilder.append(data.get(key));
				isFirstInsertion = false;
				count++;
			} else if(data.containsKey(key) && data.get(key) != null && !isFirstInsertion) {
				featureBuilder.append("|").append(data.get(key));
				count++;
			} else if(!data.containsKey(key)) {
				break;
			} else {
				count++;
			}
		}	
		data.put("FEATURE_BULLETS",(featureBuilder.toString().isEmpty()) ? null : featureBuilder.toString());
	}
	
	private void prioritizeFields(Map<String,Object> itemData,Map<String,Object> data) {
		data.keySet().stream().forEach(key ->{
			if(itemData.containsKey(key) && data.get(key) !=null) {
				itemData.remove(key);
				itemData.put(key,data.get(key));
			}
		});
	}
	
    private void processAttributesForTemplateData(Map<String,Object> data) {
    	int count =1;
		while(true) {
			String attributeUomKey = "ATTRIBUTE_UOM_" + count;
			String attributeNameKey = "ATTRIBUTE_NAME_" + count;
			String attributeValueKey = "ATTRIBUTE_VALUE_" + count;
			if(data.containsKey(attributeValueKey) && data.get(attributeValueKey) != null) {
				StringBuilder sb = new StringBuilder();
				sb.append(data.get(attributeValueKey));
				if(data.get(attributeUomKey) != null && !data.get(attributeUomKey).toString().isBlank())
					sb.append(" ").append(data.get(attributeUomKey));
				data.put(data.get(attributeNameKey).toString(), sb);
				count++;
			} else if(!data.containsKey(attributeNameKey)) {
				break;
			} else {
				count++;
			}
		}
    }
    
    private void processXFieldsForTemplateData(Map<String,Object> data, Map<String,Object> tempData) {
    	data.keySet().stream().forEach(key-> {
			if(key.length() >3 && key.substring(0,3).equals("EX_")) 
				tempData.put(key.substring(3,key.length()), data.get(key));
		});
    }
	
}
