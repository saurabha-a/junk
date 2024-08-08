package com.unilog.cx1.pim.commons.util;

import static com.unilog.iam.jooq.tables.IamClient.IAM_CLIENT;
import static com.unilog.iam.jooq.tables.SiPublisherClient.SI_PUBLISHER_CLIENT;
import static com.unilog.iam.jooq.tables.SiPublisherSubscriber.SI_PUBLISHER_SUBSCRIBER;
import static com.unilog.prime.commons.util.BooleanUtil.convertToBoolean;
import static com.unilog.prime.jooq.tables.SiSubsetQuery.SI_SUBSET_QUERY;
import static com.unilog.prime.jooq.tables.SiSubsetSources.SI_SUBSET_SOURCES;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.types.ULong;
import org.springframework.http.HttpStatus;

import com.unilog.cx1.pim.commons.model.Tables;
import com.unilog.prime.commons.exception.PrimeException;
import com.unilog.prime.commons.util.QueryUtil;
import com.unilog.prime.jooq.enums.SiSubsetDefinitionSubsetStatus;
import com.unilog.prime.jooq.enums.SiSubsetDefinitionSubsetType;
import com.unilog.prime.jooq.tables.SiSubsetDefinition;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;
import com.unilog.prime.commons.util.StringUtil;

public class FilteredQueryUtil {

	public static List<String> getExportAndCountQuery(ULong clientId, Map<String, Object> searchQuery, ULong subsetId,
			DSLContext dslContext, String globalFilterTableName) {
		List<String> queryList = new ArrayList<>();
		Tables tables = getTables(subsetId, false, clientId, dslContext);
		String exportQuery = dslContext
				.select(SI_SUBSET_QUERY.ITEM_HISTORY).from(SI_SUBSET_QUERY).where(SI_SUBSET_QUERY.SUBSET_ID.eq(subsetId)
						.and(SI_SUBSET_QUERY.TABLE_TYPE.eq("ITEM")).and(SI_SUBSET_QUERY.VIEW_TYPE.eq("EXPORT")))
				.fetchOneInto(String.class);

		if (convertToBoolean(searchQuery.get("changedItems"))) {
			var table = DSL.table(DSL.name(tables.getDatabaseName(), tables.getShadowITEMTable())).as("i");
			queryList.add(
					QueryUtil.replaceWithQuery(exportQuery, dslContext.select(DSL.field("i.ID")).from(table).getSQL()));
			queryList.add(dslContext.selectCount().from(table).getSQL());
		} else {
			List<Condition> conditions = ItemListFilter.conditionsFrom(subsetId, searchQuery, tables, dslContext,
					getAllowedClientIds(clientId, dslContext), clientId.toBigInteger());
			if(globalFilterTableName != null) 
			    conditions.add(DSL.condition(String.format("a.ID IN (SELECT ft.ITEM_ID FROM prime.%s ft)", globalFilterTableName)));
			String conditionQuery = !conditions.isEmpty() ? DSL.and(conditions).toString() : "";
			exportQuery = QueryUtil.getConstructedQuery(exportQuery, conditionQuery, null, 0, null, null);
			exportQuery = replaceWithCategoryTargetId(searchQuery, exportQuery, "a.");

			String countQuery = dslContext.select(SI_SUBSET_QUERY.QUERY_COUNT).from(SI_SUBSET_QUERY)
					.where(SI_SUBSET_QUERY.SUBSET_ID.eq(subsetId).and(SI_SUBSET_QUERY.TABLE_TYPE.eq("PRECOMPUTE"))
							.and(SI_SUBSET_QUERY.VIEW_TYPE.eq("MERGE")))
					.fetchOneInto(String.class);
			conditionQuery = conditionQuery.replace("a.", "a1.");
			countQuery = QueryUtil.getConstructedQuery(countQuery, conditionQuery, null, 0, null, null);
			countQuery = replaceWithCategoryTargetId(searchQuery, countQuery, "a1.");
			queryList.add(exportQuery);
			queryList.add(countQuery);
		}
		return queryList;
	}
	
	private  static String replaceWithCategoryTargetId(Map<String, Object> searchQuery, String query, String str) {
		 final String openingQuotes = "'";
		  final String closingQuotes = "', ";
		
		if (searchQuery.containsKey("categories")) {
			String[] string = query.split("prime.si_subset_rules_subscriber_taxonomy ww");
			List<Object> values = ItemListFilter.getValues(searchQuery, "categories");
			
			IntStream.range(1, string.length).forEach(i -> {

					values.stream().forEach(category ->{
					StringBuilder string1 = new StringBuilder();
					string1.append(openingQuotes).append(category).append(closingQuotes); 
						
						
					String replacestring1=string1.toString()+str + "CATEGORY_LIST";   
					String replacestring2=string1.toString() + "ww.CATEGORY_TARGET_ID";
					
					String replacestring3=string1.toString()+str + "CATEGORY_ID in";
					String replacestring4=string1.toString() + "ww.CATEGORY_TARGET_ID in";
	
					string[i] = string[i].replaceFirst(replacestring1,replacestring2);
					string[i] = string[i].replaceFirst(replacestring3,replacestring4);
				});
				
			});
			query = StringUtil.convertToStringWithDelimiter(string, "prime.si_subset_rules_subscriber_taxonomy ww");
		}
		return query;
	}

	private static Set<BigInteger> getAllowedClientIds(ULong clientId, DSLContext dslContext) {
		Set<BigInteger> set = new HashSet<>();
		set.add(clientId.toBigInteger());
		String clientTypeCode = dslContext.select(IAM_CLIENT.CLIENT_TYPE_CODE).from(IAM_CLIENT)
				.where(IAM_CLIENT.ID.eq(clientId)).limit(1).fetchOneInto(String.class);
		if ("P".equals(clientTypeCode))
			return set;

		set.add(dslContext.select(SI_PUBLISHER_CLIENT.CLIENT_ID).from(SI_PUBLISHER_CLIENT)
				.join(SI_PUBLISHER_SUBSCRIBER).on(SI_PUBLISHER_SUBSCRIBER.PUBLISHER_ID.eq(SI_PUBLISHER_CLIENT.ID))
				.where(SI_PUBLISHER_SUBSCRIBER.CLIENT_ID.eq(clientId)).limit(1).fetchOneInto(BigInteger.class));
		return set;
	}

	public static Tables getTables(ULong subsetId, boolean forSave, ULong clientId, DSLContext dslContext) {

		SiSubsetDefinitionRecord result = dslContext.selectFrom(SiSubsetDefinition.SI_SUBSET_DEFINITION)
				.where(SiSubsetDefinition.SI_SUBSET_DEFINITION.ID.eq(subsetId)
						.and(SiSubsetDefinition.SI_SUBSET_DEFINITION.CLIENT_ID.eq(clientId)
								.or(SiSubsetDefinition.SI_SUBSET_DEFINITION.CLIENT_CREATED_ID.eq(clientId))))
				.fetchOne();
		if (result == null) {
			result = dslContext.selectFrom(SiSubsetDefinition.SI_SUBSET_DEFINITION)
					.where(SiSubsetDefinition.SI_SUBSET_DEFINITION.ID
							.in(dslContext.select(SI_SUBSET_SOURCES.SUBSET_ID).from(SI_SUBSET_SOURCES)
									.leftJoin(SI_PUBLISHER_CLIENT)
									.on(SI_SUBSET_SOURCES.PUBLISHER_ID
											.eq(DSL.cast(SI_PUBLISHER_CLIENT.ID, SQLDataType.BIGINT)))
									.where(SI_PUBLISHER_CLIENT.CLIENT_ID.eq(clientId)))
							.and(SiSubsetDefinition.SI_SUBSET_DEFINITION.ID.eq(subsetId)))
					.fetchOne();
		}
		return getTables(result, forSave);
	}

	private static Tables getTables(SiSubsetDefinitionRecord result, boolean forSave) {

		if (forSave && result
				.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.SUBSET_STATUS) != SiSubsetDefinitionSubsetStatus.V)
			throw new PrimeException(HttpStatus.BAD_REQUEST, "Subset is not in valid state.");

		Tables tables = new Tables();
		tables.setDatabaseName(result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.SCHEMA_NAME));
		SiSubsetDefinitionSubsetType subsetType = result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.SUBSET_TYPE);

		tables.setMemberShipCount(result.getMembershipCount());
		tables.setSubsetType(result.getSubsetType());
		tables.setTaxonomyId(result.getTaxonomyId());
		tables.setAssociationTable(result.getAssocdataTableName());
		tables.setPrivateMembersTableName(result.getPrivateMembersTableName());
		tables.setMasterListTableName(result.getMasterListTableName());
		tables.setItemMembersTableName(result.getItemMembersTableName());
		tables.setZeroMembershipAllowedSubset(Byte.valueOf((byte) 1).equals(result.getZeroMembershipAllowed()));
		if (subsetType == SiSubsetDefinitionSubsetType.E) {
			tables.setShadowASSETTable(result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.ASSET_TABLE_NAME));
			tables.setShadowATTRIBUTETable(
					result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.ATTRIBUTE_TABLE_NAME));
			tables.setShadowCATEGORYTable(result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.CATEGORY_TABLE_NAME));
			tables.setShadowPARTNUMBERTable(
					result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.PARTNUMBER_TABLE_NAME));
			tables.setShadowITEMTable(result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.ITEM_TABLE_NAME));
			tables.setShadowPRICETable(result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.PRICE_TABLE_NAME));
			tables.setShadowKEYWORDTable(result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.KEYWORD_TABLE_NAME));
			tables.setShadowPRODUCTTable(result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.PRODUCT_TABLE_NAME));
			tables.setShadowX_FIELDTable(result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.X_FIELD_TABLE_NAME));
			tables.setShadowLINKTable(result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.LINK_TABLE_NAME));
		} else {
			tables.setShadowASSETTable(
					result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.ASSET_SHADOW_TABLE_NAME));
			tables.setShadowATTRIBUTETable(
					result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.ATTRIBUTE_SHADOW_TABLE_NAME));
			tables.setShadowCATEGORYTable(
					result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.CATEGORY_SHADOW_TABLE_NAME));
			tables.setShadowPARTNUMBERTable(
					result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.PARTNUMBER_SHADOW_TABLE_NAME));
			tables.setShadowITEMTable(result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.ITEM_SHADOW_TABLE_NAME));
			tables.setShadowPRICETable(
					result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.PRICE_SHADOW_TABLE_NAME));
			tables.setShadowKEYWORDTable(
					result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.KEYWORD_SHADOW_TABLE_NAME));
			tables.setShadowPRODUCTTable(
					result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.PRODUCT_SHADOW_TABLE_NAME));
			tables.setShadowX_FIELDTable(
					result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.X_FIELD_SHADOW_TABLE_NAME));
			tables.setShadowLINKTable(result.getValue(SiSubsetDefinition.SI_SUBSET_DEFINITION.LINK_TABLE_NAME ) );
		}
		tables.setParentId(result.getParentId());
		tables.setDefinitionRecord(result);
		return tables;
	}
}
