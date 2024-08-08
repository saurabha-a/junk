package com.unilog.cx1.pim.commons.util;

import static com.unilog.prime.jooq.tables.LuGlobalFilterableAttributes.LU_GLOBAL_FILTERABLE_ATTRIBUTES;
import static com.unilog.prime.jooq.tables.SiClientContentRules.SI_CLIENT_CONTENT_RULES;
import static com.unilog.prime.jooq.tables.SiClientXField.SI_CLIENT_X_FIELD;
import static com.unilog.prime.jooq.tables.SiSubsetDefinition.SI_SUBSET_DEFINITION;
import static com.unilog.prime.jooq.tables.SiSubsetRulesSubscriberTaxonomy.SI_SUBSET_RULES_SUBSCRIBER_TAXONOMY;
import static com.unilog.prime.jooq.tables.TmbBrand.TMB_BRAND;
import static com.unilog.prime.jooq.tables.TmbManufacturer.TMB_MANUFACTURER;
import static org.jooq.types.ULong.valueOf;
import static org.springframework.util.StringUtils.isEmpty;

import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.LikeEscapeStep;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.springframework.http.HttpStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.unilog.cx1.pim.commons.model.Tables;
import com.unilog.prime.commons.exception.PrimeException;
import com.unilog.prime.commons.util.BooleanUtil;
import com.unilog.prime.commons.util.DateUtil;
import com.unilog.prime.commons.util.TextUtil;

public class ItemListFilter {

	private static final String RECORD_STATUS_STRING = "record_status";
	private static final String MFR_PART_NUMBER2 = "MFR_PART_NUMBER";
	private static final String PART_NUMBER2 = "PART_NUMBER";
	private static final String PARTNUMBER3 = "PART_NUMBER3";
	private static final String FIELD_NAME = "fieldName";
	private static final String GLOBAL_ATTRIBUTE_NAME = "attributeName";
	private static final String WITH_VALUE_DISPLAY = "With Value";
	private static final String NO_VALUE_DISPLAY = "No Value";
	
	private static final String ALT_PART_NUMBER1 = "ALTERNATE_PART_NUMBER_1";
	private static final String ALT_PART_NUMBER2 = "ALTERNATE_PART_NUMBER_2";

	public static final Field<String> PART_NUMBER = DSL.field("a.PART_NUMBER", String.class);
	public static final Field<String> PART_NUMBER3 = DSL.field("b.PART_NUMBER", String.class);
	private static final Field<String> MY_PART_NUMBER = DSL.field("a.MY_PART_NUMBER", String.class);
	private static final Field<String> MFR_PART_NUMBER = DSL.field("a.MFR_PART_NUMBER", String.class);
	private static final Field<String> ALT_PART_NUMBER_1 = DSL.field("a.ALTERNATE_PART_NUMBER_1", String.class);
	private static final Field<String> ALTERNATE_PART_NUMBER_1 = DSL.field("b.PARTNUMBER_TYPE", String.class);
	private static final Field<String> ALT_PART_NUMBER_2 = DSL.field("a.ALTERNATE_PART_NUMBER_2", String.class);
	private static final Field<String> KEYWORDS = DSL.field("a.KEYWORDS", String.class);
	
	
	private static final Field<String> ENRICHED_INDICATOR = DSL.field("a.ENRICHED_INDICATOR", String.class);
	private static final Field<Timestamp> UPDATED_AT = DSL.field("a.UPDATED_AT", Timestamp.class);
	private static final Field<Timestamp> CREATED_AT = DSL.field("a.CREATED_AT", Timestamp.class);
	private static final Field<Timestamp> DATE_ENHANCED = DSL.field("a.DATE_ENHANCED", Timestamp.class);
	private static final Field<String> UPC = DSL.field("a.UPC", String.class);
	private static final Field<String> ATTRIBUTES = DSL.field("a.ATTRIBUTES", String.class);
	public static final Field<Byte> SOURCE_CHANGED = DSL.field("a.SOURCE_CHANGED", Byte.class);
	public static final Field<String> COUNTRY_OF_SALE = DSL.field("a.COUNTRY_OF_SALE", String.class);
	public static final Field<String> ID = DSL.field("a.ID", String.class);
	public static final Field<String> RECORD_STATUS = DSL.field("a1.RECORD_STATUS", String.class);
	public static final Field<ULong> CATEGORY_ID = DSL.field("a.CATEGORY_ID", ULong.class);
	public static final Field<ULong> CATEGORY_TARGET_ID = DSL.field("ww.CATEGORY_TARGET_ID", ULong.class);
	public static final Field<ULong> PRODUCT_ID = DSL.field("a.PRODUCT_ID", ULong.class);
	public static final Field<String> MANUFACTURER_STATUS = DSL.field("a.MANUFACTURER_STATUS", String.class);
	public static final Field<ULong> BRAND_ID = DSL.field("a.BRAND_ID", ULong.class);
	public static final Field<Byte> CAT_INDICATOR = DSL.field("a.CAT", Byte.class);
	public static final Field<ULong> LEAF_CATEGORY_ID = DSL.field("LEAF_CATEGORY_ID", ULong.class);
	public static final Field<String> X_FIELD_VALUES = DSL.field("a.X_FIELD_VALUES", String.class);

	public static final Map<String, Field<?>> SORT_PARAMETERS_FIELD_MAP = new HashMap<>(); // NOSONAR
	private static final Map<String, Field<Byte>> INC_BOOLEAN_FIELD_MAP = new HashMap<>();
	private static final Map<Field<Timestamp>, String[]> DATE_FIELD_MAP = new HashMap<>();
	private static final Map<String, Field<String>> PART_NUMBER_FIELD_MAP = new HashMap<>();
	private static final Map<String, Field<String>> PART_NUMBER_FIELD_MAP2 = new HashMap<>();
	
	public static final Map<String, Field<String>> GLOBAL_FILTERABLE_ATTRIBUTES_MAP = new HashMap<>();

	static {
		SORT_PARAMETERS_FIELD_MAP.put("partNumber", DSL.field("i.PART_NUMBER", String.class));
		SORT_PARAMETERS_FIELD_MAP.put("mfrPartNumber", DSL.field("i.MFR_PART_NUMBER", String.class));
		SORT_PARAMETERS_FIELD_MAP.put("manufacturer.manufacturerName", DSL.field("i.MANUFACTURER_NAME", String.class));
		SORT_PARAMETERS_FIELD_MAP.put("brand.brandName", DSL.field("i.BRAND_NAME", String.class));
		SORT_PARAMETERS_FIELD_MAP.put("enrichedIndicator", DSL.field("i.ENRICHED_INDICATOR", String.class));
		SORT_PARAMETERS_FIELD_MAP.put("updatedAt", DSL.field("i.UPDATED_AT", Timestamp.class));
		SORT_PARAMETERS_FIELD_MAP.put("createdAt", DSL.field("i.CREATED_AT", Timestamp.class));
		SORT_PARAMETERS_FIELD_MAP.put("upc", DSL.field("i.UPC", String.class));
		SORT_PARAMETERS_FIELD_MAP.put("countryOfSale", DSL.field("i.COUNTRY_OF_SALE", String.class));
		SORT_PARAMETERS_FIELD_MAP.put("enhancedDate", DSL.field("i.DATE_ENHANCED", Timestamp.class));
		SORT_PARAMETERS_FIELD_MAP.put("myPartNumber", DSL.field("i.MY_PART_NUMBER", String.class));
		SORT_PARAMETERS_FIELD_MAP.put("manufacturerStatus", DSL.field("i.MANUFACTURER_STATUS", String.class));
		SORT_PARAMETERS_FIELD_MAP.put("productName", DSL.field(" json_extract(i.PRODUCTS,'$.NAME')", String.class));
		SORT_PARAMETERS_FIELD_MAP.put("displaySequence", DSL.field(" json_extract(i.PRODUCTS,'$.SEQ')", String.class));
		
		INC_BOOLEAN_FIELD_MAP.put("incImages", DSL.field("a.IMG", Byte.class));
		INC_BOOLEAN_FIELD_MAP.put("incDocuments", DSL.field("a.DOC", Byte.class));
		INC_BOOLEAN_FIELD_MAP.put("incAttributes", DSL.field("a.ATTR", Byte.class));
		INC_BOOLEAN_FIELD_MAP.put("incProducts", DSL.field("a.PROD", Byte.class));
		INC_BOOLEAN_FIELD_MAP.put("privateItems", DSL.field("a.PRIVATE", Byte.class));
		INC_BOOLEAN_FIELD_MAP.put("changedItems", SOURCE_CHANGED);
		//INC_BOOLEAN_FIELD_MAP.put("incDescriptions", DSL.field("a.DESCR", Byte.class));
		INC_BOOLEAN_FIELD_MAP.put("incXFields", DSL.field("a.XFLD", Byte.class));

		DATE_FIELD_MAP.put(CREATED_AT, new String[] { "dateAdded", "dateAddedEnd" });
		DATE_FIELD_MAP.put(UPDATED_AT, new String[] { "dateUpdated", "dateUpdatedEnd" });
		DATE_FIELD_MAP.put(DATE_ENHANCED, new String[] { "enhancedDate", "enhancedDateEnd" });

		PART_NUMBER_FIELD_MAP.put(PART_NUMBER2, PART_NUMBER);
		PART_NUMBER_FIELD_MAP.put(MFR_PART_NUMBER2, MFR_PART_NUMBER);
		PART_NUMBER_FIELD_MAP.put(ALT_PART_NUMBER1, ALT_PART_NUMBER_1);
		PART_NUMBER_FIELD_MAP.put(ALT_PART_NUMBER2, ALT_PART_NUMBER_2);
		PART_NUMBER_FIELD_MAP.put("KEYWORDS", KEYWORDS);
		PART_NUMBER_FIELD_MAP.put("UPC", UPC);
		PART_NUMBER_FIELD_MAP.put("MY_PART_NUMBER", MY_PART_NUMBER);
		
		PART_NUMBER_FIELD_MAP2.put(PART_NUMBER2, PART_NUMBER);
		PART_NUMBER_FIELD_MAP2.put(MFR_PART_NUMBER2, MFR_PART_NUMBER);
		PART_NUMBER_FIELD_MAP2.put(ALT_PART_NUMBER1, ALTERNATE_PART_NUMBER_1);
		PART_NUMBER_FIELD_MAP2.put(ALT_PART_NUMBER2, ALTERNATE_PART_NUMBER_1);
		PART_NUMBER_FIELD_MAP2.put("UPC", UPC);
		PART_NUMBER_FIELD_MAP2.put("MY_PART_NUMBER", MY_PART_NUMBER);
		PART_NUMBER_FIELD_MAP2.put(PARTNUMBER3, PART_NUMBER3);
		
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("APPLICATION", DSL.field("a1.APPLICATION", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("ENRICHED_INDICATOR", DSL.field("a.ENRICHED_INDICATOR", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("MANUFACTURER_STATUS", DSL.field("a.MANUFACTURER_STATUS", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("COUNTRY_OF_SALE", DSL.field("a.COUNTRY_OF_SALE", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("SHORT_DESCRIPTION", DSL.field("a.SHORT_DESCRIPTION", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("MY_PART_NUMBER", DSL.field("a.MY_PART_NUMBER", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("COUNTRY_OF_ORIGIN", DSL.field("a1.COUNTRY_OF_ORIGIN", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("INVC_DESCRIPTION", DSL.field("a1.INVC_DESCRIPTION", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("PACKAGE_LENGTH", DSL.field("a1.PACKAGE_LENGTH", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("LONG_DESCRIPTION", DSL.field("a1.LONG_DESCRIPTION", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("MFR_PART_NUMBER", DSL.field("a1.MFR_PART_NUMBER", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("MRKT_DESCRIPTION", DSL.field("a1.MRKT_DESCRIPTION", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("MIN_ORDER_QTY", DSL.field("a1.MIN_ORDER_QTY", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("ORDER_QTY_INTERVAL", DSL.field("a1.ORDER_QTY_INTERVAL", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("PACK_DESCRIPTION", DSL.field("a1.PACK_DESCRIPTION", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("PACKAGE_QTY", DSL.field("a1.PACKAGE_QTY", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("INCLUDES", DSL.field("a1.INCLUDES", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("QTY_AVAILABLE", DSL.field("a1.QTY_AVAILABLE", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("SALES_UOM", DSL.field("a1.SALES_UOM", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("VOLUME", DSL.field("a1.VOLUME", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("WARRANTY", DSL.field("a1.WARRANTY", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("PACKAGE_WEIGHT", DSL.field("a1.PACKAGE_WEIGHT", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("PACKAGE_WIDTH", DSL.field("a1.PACKAGE_WIDTH", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("PACKAGE_HEIGHT", DSL.field("a1.PACKAGE_HEIGHT", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("UPC", DSL.field("a1.UPC", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("UNSPSC", DSL.field("a1.UNSPSC", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("STANDARD_APPROVALS", DSL.field("a1.STANDARD_APPROVALS", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("KEYWORDS", DSL.field("a.KEYWORDS", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("CUSTOM_KEYWORDS", DSL.field("a.KEYWORDS", String.class));
		GLOBAL_FILTERABLE_ATTRIBUTES_MAP.put("PART_NUMBER_KEYWORDS", DSL.field("a.KEYWORDS", String.class));
	}

	public static List<Condition> conditionsFrom(ULong subsetId, Map<String, Object> searchQuery, Tables tables,
			DSLContext dslContext, Set<BigInteger> clientIds, BigInteger loggedInUserclientId) {

		var conditions = new ArrayList<Condition>();
		var partialSearch = BooleanUtil.convertToBoolean(searchQuery.get("partial"));

		textBasedFilterOptions(searchQuery, conditions, partialSearch);
		brandsAndManufacturerOptions(searchQuery, conditions, clientIds, dslContext);
		externalBrandsOptions(searchQuery, conditions);
		inclusionFilterOptions(searchQuery, conditions, dslContext, tables.getTaxonomyId());
		textPartnumber(searchQuery, conditions, partialSearch);
		keyWordBasedFilterOptions(searchQuery, conditions, partialSearch);
		

		
		/*List<Object> value = getValues(searchQuery, "enhancedItem");
		if (value != null && !value.isEmpty() && !value.contains("undefined"))
			conditions.add(ENRICHED_INDICATOR.in(getInStrings(value)));*/

		for (var entry : DATE_FIELD_MAP.entrySet()) {
			var dates = getValues(searchQuery, entry.getValue()[0], entry.getValue()[1]);
			if (dates == null)
				continue;
			if (dates.length > 0)
				conditions.add(entry.getKey().ge(dates[0]));
			if (dates.length == 2)
				conditions.add(entry.getKey().le(dates[1]));
		}

		if (searchQuery.containsKey("categories")) {
			categoryFilterOptions(searchQuery, conditions, subsetId, dslContext, loggedInUserclientId);
		}
		
		if (searchQuery.containsKey("products")) {
			productFilterOptions(searchQuery, conditions);
		}

		if (searchQuery.containsKey("attributes")) {
			attributeFilterOptions(searchQuery, conditions);
		}
		
		if (searchQuery.containsKey("xFields")) {
			xFieldsFilterOptions(searchQuery, conditions, dslContext, clientIds);
		}
		
		if (searchQuery.containsKey("globalAttributes")) {
			globalAttributesFilterOptions(searchQuery, conditions, dslContext, clientIds);
		}

		processRecordStatus(searchQuery, tables, dslContext, conditions);

		return conditions;
	}

	private static void externalBrandsOptions(Map<String, Object> searchQuery, ArrayList<Condition> conditions) {
		List<Object> values = getExternalBrandsId(searchQuery, "externalBrands");
		if (!values.isEmpty())
			conditions.add(BRAND_ID.in(getInULongs(values)));
	}

	@SuppressWarnings({ "unchecked" })
	private static void xFieldsFilterOptions(Map<String, Object> searchQuery, ArrayList<Condition> conditions,
			DSLContext dslContext, Set<BigInteger> clientIds) {

		List<Map<Object, Object>> xFieldsObject = (List<Map<Object, Object>>) searchQuery.getOrDefault("xFields",
				Collections.emptyList());
		Set<String> xFieldNames = new HashSet<>();
		xFieldsObject.stream().forEach(e -> {
			e.values().stream().forEach(i -> {
				if (i instanceof Map) {
					Map<Object, Object> j = (Map<Object, Object>) i;
					xFieldNames.add((String) j.get(FIELD_NAME));
				}
			});
		});

		Map<String, ULong> map = dslContext.select(SI_CLIENT_X_FIELD.FIELD_NAME, SI_CLIENT_X_FIELD.ID)
				.from(SI_CLIENT_X_FIELD)
				.where(SI_CLIENT_X_FIELD.FIELD_NAME.in(xFieldNames).and(SI_CLIENT_X_FIELD.CLIENT_ID.in(clientIds)))
				.fetchMap(SI_CLIENT_X_FIELD.FIELD_NAME, SI_CLIENT_X_FIELD.ID);

		Set<Condition> orConditions = new HashSet<>();
		xFieldsObject.parallelStream().filter(Objects::nonNull).filter(e -> e.containsKey("xFieldName")).forEach(e -> {
			orConditions.add(processEachXField(map.get(((Map<String, String>) e.get("xFieldName")).get(FIELD_NAME)),
					e.get("xFieldValue") == null ? null : e.get("xFieldValue").toString()));
		});
		if (!orConditions.isEmpty())
			conditions.add(DSL.and(orConditions));
	}

	private static Condition processEachXField(ULong xFieldId, String xFieldValue) {
		if (xFieldValue == null)
			xFieldValue = "WITH_VALUE";

		if (xFieldId == null || isEmpty(xFieldId))
			return null;

		StringBuilder sb = new StringBuilder("%\"FIELD_ID\": \"");
		sb.append(xFieldId).append("\", \"FIELD_VALUE\": \"");
		if ("NO_VALUE".equals(xFieldValue)) {
			xFieldValue = "";
			sb.append(xFieldValue);
			return (X_FIELD_VALUES.like(sb.append("\"%").toString()))
					.or((X_FIELD_VALUES.notLike("%\"FIELD_ID\": \"" + xFieldId + "\"%")).or(X_FIELD_VALUES.isNull()));
		} else if ("WITH_VALUE".equals(xFieldValue)) {
			return X_FIELD_VALUES.like(sb.append("%").toString())
					.and(X_FIELD_VALUES.notLike(sb.replace(sb.lastIndexOf("%"), sb.length(), "\"%").toString()));
		} else if (!xFieldValue.isEmpty())
			sb.append(xFieldValue);
		sb.append("\"%");
		return X_FIELD_VALUES.like(sb.toString());
	}

	private static void processRecordStatus(Map<String, Object> searchQuery, Tables tables, DSLContext dslContext,
			ArrayList<Condition> conditions) {
		if (searchQuery.containsKey(RECORD_STATUS_STRING)
				&& !searchQuery.get(RECORD_STATUS_STRING).toString().equalsIgnoreCase("ALL")) {
			var table = DSL.table(DSL.name(tables.getDatabaseName(), tables.getShadowITEMTable())).as("a1");
			List<String> changedItemIds = dslContext.select(DSL.field("a1.ID")).from(table)
					.where(RECORD_STATUS.eq(searchQuery.get(RECORD_STATUS_STRING).toString())).fetchInto(String.class);
			if (!changedItemIds.isEmpty())
				conditions.add(ID.in(changedItemIds));
			else
				conditions.add(ID.in(""));
		}
	}
	
	private static void productFilterOptions(Map<String, Object> searchQuery, ArrayList<Condition> conditions) {
		List<Object> values = getValues(searchQuery, "products");
		if (!values.isEmpty()) {
			var ids = values.stream().filter(Objects::nonNull).map(Object::toString).collect(Collectors.joining(","));
			if (!ids.isEmpty()) {
				conditions.add(PRODUCT_ID.in(values));
			}
		}
	}

	private static void categoryFilterOptions(Map<String, Object> searchQuery, List<Condition> conditions,
			ULong subsetId, DSLContext dslContext, BigInteger loggedInUserclientId) {
		List<Object> values = getValues(searchQuery, "categories");
		if (!values.isEmpty() && !values.contains("undefined")) {
			if(null == loggedInUserclientId) {
				loggedInUserclientId = BigInteger.ZERO;
			}
			boolean isMultipleAllowed = false;
			ULong multipleCategoriesAllowed = dslContext.select(SI_CLIENT_CONTENT_RULES.MULTIPLE_CATEGORIES_ALLOWED)
					.from(SI_CLIENT_CONTENT_RULES)
					.where(SI_CLIENT_CONTENT_RULES.CLIENT_ID.eq(valueOf(loggedInUserclientId)))
					.fetchOneInto(ULong.class);
			if (multipleCategoriesAllowed != null) {
				isMultipleAllowed = multipleCategoriesAllowed.equals(ULong.valueOf(1));
			}
			if(isMultipleAllowed)	{
				var ids = values.stream().filter(Objects::nonNull).map(Object::toString).collect(Collectors.joining(","));
				if (!ids.isEmpty()) {

					String categoryQuery = "prime.si_category_leaf_mapping WHERE prime.si_category_leaf_mapping.CATEGORY_ID IN ("
							+ ids + ") AND prime.si_category_leaf_mapping.SUBSET_ID = " + subsetId;
					List<ULong> idList = dslContext.selectDistinct(LEAF_CATEGORY_ID).from(categoryQuery).fetchInto(ULong.class);
					Boolean isDynamicSubet= BooleanUtil.convertToBoolean(searchQuery.get("dynamicSubset"));
					if (isDynamicSubet != null && isDynamicSubet) {
						ULong taxonomyId = dslContext.select(SI_SUBSET_DEFINITION.TAXONOMY_ID).from(SI_SUBSET_DEFINITION)
								.where(SI_SUBSET_DEFINITION.ID.eq(subsetId)).fetchOneInto(ULong.class);

						List<ULong> isId = dslContext.selectDistinct(SI_SUBSET_RULES_SUBSCRIBER_TAXONOMY.CATEGORY_SOURCE_ID)
								.from(SI_SUBSET_RULES_SUBSCRIBER_TAXONOMY)
								.where(SI_SUBSET_RULES_SUBSCRIBER_TAXONOMY.TAXONOMY_TARGET_ID.eq(taxonomyId))
								.and(SI_SUBSET_RULES_SUBSCRIBER_TAXONOMY.CATEGORY_TARGET_ID.in(idList))
								.fetchInto(ULong.class);

						if (isId != null && !isId.isEmpty())
							idList = isId;
					}
					conditions.add(CAT_INDICATOR.eq(Byte.valueOf((byte) 1)));
					Condition categoryCondition = null;
					if (idList != null && !idList.isEmpty()) {
						for (ULong id : idList) {
							if (categoryCondition == null)
								categoryCondition = DSL.condition("FIND_IN_SET('" + id + "', a.CATEGORY_LIST)");
							else
								categoryCondition = categoryCondition
										.or(DSL.condition("FIND_IN_SET('" + id + "', a.CATEGORY_LIST)"));
							categoryCondition = categoryCondition
									.or(DSL.condition("FIND_IN_SET(' " + id + "', a.CATEGORY_LIST)"));
						}
						conditions.add(categoryCondition);
					}
				}
			} else {
				conditions.add(CAT_INDICATOR.eq(Byte.valueOf((byte) 1)));
				conditions.add(CATEGORY_ID.in(getInULongs(values)));
			}
			
		}
	}

	@SuppressWarnings("unchecked")
	private static void attributeFilterOptions(Map<String, Object> searchQuery, ArrayList<Condition> conditions) {
		Object attributesObject = searchQuery.getOrDefault("attributes", Collections.emptyList());
		List<Map<String, Object>> attributesValue;
		if (attributesObject instanceof List)
			attributesValue = (List<Map<String, Object>>) attributesObject;
		else {
			attributesValue = new ArrayList<>();
			attributesValue.add((Map<String, Object>) attributesObject);
		}

		if (attributesValue.isEmpty())
			return;

		Set<Condition> orConditions = attributesValue.parallelStream().filter(Objects::nonNull)
				.filter(e -> e.containsKey("attributeId")).map(ItemListFilter::processEachAttribute)
				.filter(Objects::nonNull).collect(Collectors.toSet());
		if (!orConditions.isEmpty())
			conditions.add(DSL.and(orConditions));
	}

	@SuppressWarnings("unchecked")
	private static LikeEscapeStep processEachAttribute(Map<String, Object> e) {
		String attributeValue = getValue(e, "attributeValue");
		if (attributeValue == null || org.springframework.util.StringUtils.isEmpty(attributeValue))
			attributeValue = "";

		Map<String, Object> attribute = (Map<String, Object>) e.get("attributeId");
		if (attribute == null)
			return null;
		if (!attribute.containsKey("attributeName"))
			return null;

		String attributeName = getValue(attribute, "attributeName");
		if (attributeName == null)
			return null;

		StringBuilder sb = new StringBuilder("%\"");
		sb.append(attributeName).append("\", \"ATTRIBUTE_VALUE\": \"%");
		if (!attributeValue.isEmpty())
			sb.append(attributeValue);
		sb.append("%\"%");
		return ATTRIBUTES.like(sb.toString());
	}

	private static void inclusionFilterOptions(Map<String, Object> searchQuery, ArrayList<Condition> conditions,
			DSLContext dslContext, ULong taxonomyId) {

		for (Entry<String, Field<Byte>> each : INC_BOOLEAN_FIELD_MAP.entrySet()) {

			String value = getValue(searchQuery, each.getKey());
			if (value == null || value.equalsIgnoreCase("ALL"))
				continue;
			conditions
					.add(DSL.field(each.getValue()).eq(Byte.valueOf((byte) (value.equalsIgnoreCase("WITH") ? 1 : 0))));
		}

		String value = getValue(searchQuery, "incCategories");
		if (value != null && !value.equalsIgnoreCase("ALL"))
			getCategoryCondition(conditions, value, dslContext, taxonomyId);

	}

	private static void getCategoryCondition(ArrayList<Condition> conditions, String value,
			DSLContext dslContext, ULong taxonomyId) {

		List<ULong> categoryIds = dslContext.select(SI_SUBSET_RULES_SUBSCRIBER_TAXONOMY.CATEGORY_SOURCE_ID)
				.from(SI_SUBSET_RULES_SUBSCRIBER_TAXONOMY)
				.where(SI_SUBSET_RULES_SUBSCRIBER_TAXONOMY.TAXONOMY_TARGET_ID.eq(taxonomyId)).fetchInto(ULong.class);

		if (!categoryIds.isEmpty()) {
			String ids = categoryIds.stream().filter(Objects::nonNull).map(Object::toString)
					.collect(Collectors.joining(","));

			if (value.equalsIgnoreCase("WITH")) {
				if (!ids.isEmpty()) {
					conditions.add(CAT_INDICATOR.eq(Byte.valueOf((byte) 1)));
					conditions.add(CATEGORY_ID.in(categoryIds));
				}
			} else if (value.equalsIgnoreCase("WITHOUT") && !ids.isEmpty()) {
				conditions.add(CATEGORY_ID.notIn(categoryIds));
			}
			if (ids == null || ids.isEmpty())
				conditions.add(CAT_INDICATOR.eq((byte) 0));
		} else {
			if (value.equalsIgnoreCase("WITH"))
				conditions.add(CAT_INDICATOR.eq(Byte.valueOf((byte) 1)));
			else if (value.equalsIgnoreCase("WITHOUT"))
				conditions.add(CAT_INDICATOR.eq(Byte.valueOf((byte) 0)));
		}
	}

	private static void brandsAndManufacturerOptions(Map<String, Object> searchQuery, ArrayList<Condition> conditions, Set<BigInteger> clientIds, DSLContext dslContext) {

		List<Object> values = getValues(searchQuery, "brands");
		if (!values.isEmpty() && !values.contains("undefined"))
			conditions.add(BRAND_ID.in(getInULongs(values)));

		List<Object> value = getValues(searchQuery, "brand_name");
		if (value != null && !value.isEmpty() && !value.contains("undefined")) {
			String[] brandNames = value.stream()
	                .map(convertedBrand-> TextUtil.anyToDBFormat((String)convertedBrand)).toArray(String[]::new);		
			List<Object> brandId = dslContext.select(TMB_BRAND.ID).from(TMB_BRAND).join(TMB_MANUFACTURER)
					.on(TMB_BRAND.MANUFACTURER_ID.eq(TMB_MANUFACTURER.ID)).where(TMB_BRAND.BRAND_NAME.in(brandNames))
					.and(TMB_MANUFACTURER.CLIENT_ID.in(clientIds)).fetchInto(Object.class);
			conditions.add(BRAND_ID.in(getInULongs(brandId)));
		if(brandId==null || brandId.isEmpty()) 
			throw new PrimeException(HttpStatus.BAD_REQUEST, "Brand name not found");}
		
		value = getValues(searchQuery, "manufacturer_name");
		if (value != null && !value.isEmpty() && !value.contains("undefined")) {
			String[] manufacturerNames = value.stream()
	                .map(convertedManufacturer-> TextUtil.anyToDBFormat((String)convertedManufacturer)).toArray(String[]::new);
			List<Object> brandId = dslContext.select(TMB_BRAND.ID).from(TMB_BRAND).join(TMB_MANUFACTURER)
					.on(TMB_BRAND.MANUFACTURER_ID.eq(TMB_MANUFACTURER.ID)).where(TMB_MANUFACTURER.MANUFACTURER_NAME.in(manufacturerNames))
					.and(TMB_MANUFACTURER.CLIENT_ID.in(clientIds)).fetchInto(Object.class);
			conditions.add(BRAND_ID.in(getInULongs(brandId)));	
			if(brandId==null || brandId.isEmpty()) 	
			throw new PrimeException(HttpStatus.BAD_REQUEST, "Manufacturer name not found");}
	}

	public static void textBasedFilterOptions(Map<String, Object> searchQuery, ArrayList<Condition> conditions,
			boolean partialSearch) {
		for (String each : new String[] { "MY_PART_NUMBER", MFR_PART_NUMBER2, "UPC", "ALTERNATE_PART_NUMBER_1", "ALTERNATE_PART_NUMBER_2" }) {

			List<Object> values = getValues(searchQuery, each);
			if (values == null || values.isEmpty())
				continue;
			
			values = values.stream().map(e -> escapeBackslash(e.toString()))
					.collect(Collectors.toList());
			
			if (values.size() == 1) {
				if (!values.get(0).toString().trim().isEmpty())
					conditions.add(partialSearch ? PART_NUMBER_FIELD_MAP.get(each).like("%" + values.get(0) + "%")
							: PART_NUMBER_FIELD_MAP.get(each).eq(values.get(0).toString()));
			} else {
				conditions.add(PART_NUMBER_FIELD_MAP.get(each).in(getInStrings(values)));
			}
		}
	}
	
	public static void textBasedFilterOptions2(Map<String, Object> searchQuery, List<Condition> conditions,
			boolean partialSearch) {
		Map<String, String> mp = new HashMap<>();
		mp.put(ALT_PART_NUMBER1, "ALT1");
		mp.put(ALT_PART_NUMBER2, "ALT2");
		for (String each : new String[] { "MY_PART_NUMBER", MFR_PART_NUMBER2, "UPC", ALT_PART_NUMBER1, ALT_PART_NUMBER2}) {
			List<Object> values = getValues(searchQuery, each);
			if (values.isEmpty())
				continue;			
			values = values.stream().map(e -> escapeBackslash(e.toString()))
					.collect(Collectors.toList());
			if(each.equalsIgnoreCase(ALT_PART_NUMBER1) || each.equalsIgnoreCase(ALT_PART_NUMBER2)) {
			    conditions.add(PART_NUMBER_FIELD_MAP2.get(each).eq(mp.get(each)));
				each = PARTNUMBER3;
			}
			if (values.size() == 1 && !values.get(0).toString().trim().isEmpty()) {
				conditions.add(partialSearch ? PART_NUMBER_FIELD_MAP2.get(each).like("%" + values.get(0) + "%")
						: PART_NUMBER_FIELD_MAP2.get(each).eq(values.get(0).toString()));
			} else {
				conditions.add(PART_NUMBER_FIELD_MAP2.get(each).in(getInStrings(values)));
			}
		}
	}
	
	public static void keyWordBasedFilterOptions(Map<String, Object> searchQuery, ArrayList<Condition> conditions,
			boolean partialSearch) {
		
		String value = getValue(searchQuery, "part_number_keywords");
		if (value == null)
			return;
		value = escapeBackslash(value);
		if (!value.trim().isEmpty())
			conditions.add(PART_NUMBER_FIELD_MAP.get("MY_PART_NUMBER").like("%" + value + "%").or(PART_NUMBER_FIELD_MAP.get(MFR_PART_NUMBER2).like("%" + value + "%"))
					.or(PART_NUMBER_FIELD_MAP.get("ALTERNATE_PART_NUMBER_1").like("%" + value + "%")).or(PART_NUMBER_FIELD_MAP.get("ALTERNATE_PART_NUMBER_2").like("%" + value + "%"))
					.or(PART_NUMBER_FIELD_MAP.get("KEYWORDS").like("%" + value + "%").or(PART_NUMBER_FIELD_MAP.get(PART_NUMBER2).like("%" + value + "%")
					.or(PART_NUMBER_FIELD_MAP.get("UPC").like("%" + value + "%")))));
		
	}
	
	public static void keyWordBasedFilterOptions2(Map<String, Object> searchQuery, List<Condition> conditions) {
		String value = getValue(searchQuery, "part_number_keywords");
		if (value == null)
			return;
		value = escapeBackslash(value);
		if (!value.trim().isEmpty())
			conditions.add(PART_NUMBER_FIELD_MAP2.get("MY_PART_NUMBER").like("%" + value + "%")
					.or(PART_NUMBER_FIELD_MAP2.get(MFR_PART_NUMBER2).like("%" + value + "%"))
					.or(PART_NUMBER_FIELD_MAP2.get(PART_NUMBER2).like("%" + value + "%")
					.or(PART_NUMBER_FIELD_MAP2.get(PARTNUMBER3).like("%" + value + "%")
					.or(PART_NUMBER_FIELD_MAP2.get("UPC").like("%" + value + "%")))));
		
	}

	public static void textPartnumber(Map<String, Object> searchQuery, ArrayList<Condition> conditions,
			boolean partialSearch) {
		List<Object> value = getValues(searchQuery, PART_NUMBER2);
		if (value.isEmpty())
			return;
		
		value = value.stream().map(e -> escapeBackslash(e.toString()))
				.collect(Collectors.toList());
		if (value.size() == 1) {
			if (!value.get(0).toString().trim().isEmpty())
				conditions.add(partialSearch ? PART_NUMBER.like("%" + value.get(0) + "%")
						: PART_NUMBER.eq(value.get(0).toString()));
		} else {
			conditions.add(PART_NUMBER.in(getInStrings(value)));
		}
	}

	public static String escapeBackslash(String value) {
		if (value == null)
			return null;
		if (value.contains("\\"))
			value = value.replaceAll("\\\\", "\\\\\\\\");
		
		return value;
	}

	private static Collection<ULong> getInULongs(List<Object> values) {

		return values.stream().filter(Objects::nonNull).map(Object::toString).map(ULong::valueOf)
				.collect(Collectors.toSet());
	}
	
	private static Collection<String> getInStrings(List<Object> values) {

		return values.stream().filter(Objects::nonNull).map(Object::toString).collect(Collectors.toSet());
	}

	private static Timestamp[] getValues(Map<String, Object> searchQuery, String fromKey, String toKey) {

		String from = getValue(searchQuery, fromKey);
		if (from == null)
			return new Timestamp[0];

		Timestamp fromDate = DateUtil.convertDateIntoSQLDate(from + " 00:00:00.000",
				DateUtil.DDMMYYYY_HHMMSS_DATE_FORMAT);
		if (fromDate == null)
			fromDate = DateUtil.convertDateIntoSQLDate(from, DateUtil.UTC_DATE_FORMAT);
		if (fromDate == null)
			return new Timestamp[0];

		String to = getValue(searchQuery, toKey);
		if (to == null)
			return new Timestamp[] { fromDate };

		Timestamp toDate = DateUtil.convertDateIntoSQLDate(to + " 23:59:59.999", DateUtil.DDMMYYYY_HHMMSS_DATE_FORMAT);
		if (toDate == null)
			toDate = DateUtil.convertDateIntoSQLDate(to, DateUtil.UTC_DATE_FORMAT);
		if (toDate == null)
			return new Timestamp[] { fromDate };

		return new Timestamp[] { fromDate, toDate };
	}

	public static String getValue(Map<String, Object> searchQuery, String keyName) {
		Object valueObject = searchQuery.get(keyName);
		if (valueObject == null) {
			valueObject = searchQuery.get(keyName.toLowerCase());
		}
		if (valueObject != null && !StringUtils.isBlank(valueObject.toString())) {
			return valueObject.toString().trim();
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static List<Object> getValues(Map<String, Object> searchQuery, String keyName) {

		if (!searchQuery.containsKey(keyName))
			keyName = keyName.toLowerCase();
		Object valueObject = searchQuery.getOrDefault(keyName, Collections.emptyList());
		List<Object> values;

		if (valueObject instanceof List)
			values = ((List<Object>) valueObject);
		else {
			values = new ArrayList<>();
			values.add(valueObject);
		}

		return values;
	}
	
	@SuppressWarnings("unchecked")
	private static List<Object> getExternalBrandsId(Map<String, Object> searchQuery, String keyName) {

		if (!searchQuery.containsKey(keyName))
			keyName = keyName.toLowerCase();
		Object valueObject = searchQuery.getOrDefault(keyName, Collections.emptyList());
		List<Object[]> values;
		List<Object> ids = new ArrayList<>();

		if (valueObject instanceof List) {
			values = ((List<Object[]>) valueObject);
			for(Object obj : values) {
				ObjectMapper objectMapper = new ObjectMapper();
				Map<String, Object> map = objectMapper.convertValue(obj, Map.class);
				Object val = map.get("id");
				ids.add(val);
			}
		}
		else {
			ids.add(valueObject);
		}
		return ids;
	}
	
	@SuppressWarnings({ "unchecked" })
	private static void globalAttributesFilterOptions(Map<String, Object> searchQuery, ArrayList<Condition> conditions,
			DSLContext dslContext, Set<BigInteger> clientIds) {

		List<Map<Object, Object>> globalAttributesObject = (List<Map<Object, Object>>) searchQuery.getOrDefault("globalAttributes",
				Collections.emptyList());
		
		Set<String> globalAttributeNames = new HashSet<>();
		globalAttributesObject.stream().forEach(e -> {
			e.values().stream().forEach(i -> {
				if (i instanceof Map) {
					Map<Object, Object> j = (Map<Object, Object>) i;
					globalAttributeNames.add((String) j.get(GLOBAL_ATTRIBUTE_NAME));
				}
			});
		});
		
	
		Map<String, String> map = dslContext.select(LU_GLOBAL_FILTERABLE_ATTRIBUTES.ATTRIBUTE_NAME, LU_GLOBAL_FILTERABLE_ATTRIBUTES.ITEM_COLUMN_NAME)
				.from(LU_GLOBAL_FILTERABLE_ATTRIBUTES)
				.where(LU_GLOBAL_FILTERABLE_ATTRIBUTES.ATTRIBUTE_NAME.in(globalAttributeNames))
				.fetchMap(LU_GLOBAL_FILTERABLE_ATTRIBUTES.ATTRIBUTE_NAME, LU_GLOBAL_FILTERABLE_ATTRIBUTES.ITEM_COLUMN_NAME);
	

		Set<Condition> orConditions = new HashSet<>();
		if(!map.isEmpty())
		{
		globalAttributesObject.parallelStream().filter(Objects::nonNull).filter(e -> e.containsKey("globalAttributeId")).forEach(e -> {
			orConditions.add(processEachGlobalAttribute(map.get(((Map<String, String>) e.get("globalAttributeId")).get(GLOBAL_ATTRIBUTE_NAME)),
					e.get("globalAttributeValue") == null ? null : e.get("globalAttributeValue").toString()));
		});

		if (!orConditions.isEmpty())
			conditions.add(DSL.and(orConditions));
		}
	}
	
	@SuppressWarnings("unchecked")
	private static Condition processEachGlobalAttribute(String globalAttributeItemColumnName, String globalAttributeValue)
	{
		final Field<String> GLOBAL_ATTRIBUTES_VALUES = GLOBAL_FILTERABLE_ATTRIBUTES_MAP.get(globalAttributeItemColumnName);
		Condition tempCondition = null;
		if (null != GLOBAL_ATTRIBUTES_VALUES) {
			if (globalAttributeItemColumnName.equals("CUSTOM_KEYWORDS")) {
				if (globalAttributeValue == null || globalAttributeValue.isEmpty()
						|| NO_VALUE_DISPLAY.equals(globalAttributeValue)) {
					tempCondition = DSL.condition("a.Keywords is not null and a.keywords not LIKE '%\"keywordType\": \"SKW\"%'");
					return tempCondition;
				} else if (WITH_VALUE_DISPLAY.equals(globalAttributeValue))
					tempCondition = DSL.condition("a.Keywords is not null and a.keywords LIKE '%\"keywordType\": \"SKW\"%'");
				return tempCondition;
			} else if(globalAttributeItemColumnName.equals("PART_NUMBER_KEYWORDS")) {
				if (globalAttributeValue == null || globalAttributeValue.isEmpty()
						|| NO_VALUE_DISPLAY.equals(globalAttributeValue)) {
					tempCondition = DSL.condition("a.Keywords is not null and a.keywords not LIKE '%\"keywordType\": \"PKW\"%'");
					return tempCondition;
				} else if (WITH_VALUE_DISPLAY.equals(globalAttributeValue))
					tempCondition = DSL.condition("a.Keywords is not null and a.keywords LIKE '%\"keywordType\": \"PKW\"%'");
				return tempCondition;			
			} else if(globalAttributeItemColumnName.equals("KEYWORDS")) {
				if (globalAttributeValue == null || globalAttributeValue.isEmpty()
						|| NO_VALUE_DISPLAY.equals(globalAttributeValue)) {
					tempCondition = DSL.condition("a.Keywords is not null and a.keywords not LIKE '%\"keywordType\": \"CKW\"%'");
					return tempCondition;
				} else if (WITH_VALUE_DISPLAY.equals(globalAttributeValue))
					tempCondition = DSL.condition("a.Keywords is not null and a.keywords LIKE '%\"keywordType\": \"CKW\"%'");
				return tempCondition;	
			}
			
			if (globalAttributeValue == null || globalAttributeValue.isEmpty()
					|| NO_VALUE_DISPLAY.equals(globalAttributeValue))
				return GLOBAL_ATTRIBUTES_VALUES.isNull();
			else if (WITH_VALUE_DISPLAY.equals(globalAttributeValue))
				return GLOBAL_ATTRIBUTES_VALUES.isNotNull();
			else {
				return GLOBAL_ATTRIBUTES_VALUES.in(globalAttributeValue);
			}
		}
		return null;
	}

	private ItemListFilter() {
	}
}
