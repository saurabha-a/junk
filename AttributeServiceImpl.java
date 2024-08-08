package com.unilog.cx1.pim.commons.service.impl;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Maps.newTreeMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.CAN_CREATE_ATTRIBUTE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ATTRIBUTE_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ATTRIBUTE_NAME;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ATTRIBUTE_UOM;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ATTRIBUTE_UOM_COLUMN;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ATTRIBUTE_VALUE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ATTRIBUTE_VALUE_COLUMN;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ITEM_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER_HASH;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PUBLISHER_ID;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryTableType.ATTRIBUTE;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryViewType.BASE;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryViewType.MERGE;
import static com.unilog.prime.commons.util.BooleanUtil.convertToBoolean;
import static com.unilog.prime.commons.util.IDUtil.createHash;
import static com.unilog.prime.jooq.tables.SiItemAttributeValue.SI_ITEM_ATTRIBUTE_VALUE;
import static com.unilog.prime.jooq.tables.TmbClientAttribute.TMB_CLIENT_ATTRIBUTE;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.wrap;
import static org.jooq.impl.DSL.and;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.or;
import static org.springframework.transaction.annotation.Isolation.READ_COMMITTED;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.types.ULong;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.unilog.cx1.pim.commons.enumeration.AttributeDataType;
import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;
import com.unilog.cx1.pim.commons.service.IAttributeService;
import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.commons.util.EnumUtil;
import com.unilog.prime.jooq.Tables;
import com.unilog.prime.jooq.tables.records.SiItemAttributeValueRecord;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;
import com.unilog.prime.jooq.tables.records.TmbClientAttributeRecord;

@Service(AttributeServiceImpl.BEAN_ID)
public class AttributeServiceImpl extends AbstractImportService implements IAttributeService {

	public static final String BEAN_ID = "etlAttributeService";
	
	public static final Integer MAX_ATTRIBUTES = 51;

	@Override
	@Transactional(isolation = READ_COMMITTED)
	public void doImport(Import anImport, PimDataObject qdo) {
		Map<?, SiItemAttributeValueRecord> allExistingAttributes = getAllParentRecords(anImport, qdo);
		Tuple<List<Tuple<Map<String, Object>, Map<String, Object>>>, Condition> tuple = getTuplesToInsert(anImport, qdo,
				allExistingAttributes);
		Set<String> notNullColumns = newHashSet(ATTRIBUTE_ID);
		if (anImport.subset.isExternal()) {
			notNullColumns.add(ATTRIBUTE_VALUE);
		}
		super.doImportInItemSubTable(anImport, qdo, tuple, anImport.subset.getAttributeInsertTable(),
				anImport.subset.getAttributeInsertColumns(), of(ATTRIBUTE_UOM, ATTRIBUTE_VALUE), of(ATTRIBUTE_ID),
				notNullColumns);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private Tuple<List<Tuple<Map<String, Object>, Map<String, Object>>>, Condition> getTuplesToInsert(Import anImport,
			PimDataObject qdo, Map<?, SiItemAttributeValueRecord> allExistingAttributes) {
		List<Tuple> tuples = Lists.newArrayList();
		List<Condition> conditions = Lists.newArrayList();

		String itemId = qdo.getValue(ITEM_ID);
		for (int i = 1; i < MAX_ATTRIBUTES; i++) {
			String atrName = qdo.getValue(ATTRIBUTE_NAME + "_" + i);
			if (isNotBlank(atrName)) {
				ULong atrId = getAttributeKey(atrName, itemId, anImport, qdo);
				String atrVal = qdo.getValue(ATTRIBUTE_VALUE + "_" + i);
				this.validateAttributeValue(atrId, atrVal, atrName);
				String uomVal = qdo.getValue(ATTRIBUTE_UOM + "_" + i);
				String id = createHash(itemId, atrId.longValue());
				Record existingRecord = getExistingRecord(anImport, id, allExistingAttributes);
				Map<String, Object> oldR = existingRecord == null ? newTreeMap() : existingRecord.intoMap();
				Map<String, Object> newR = getNewImportRec(qdo, atrVal, uomVal, atrId, id, existingRecord, anImport);
				tuples.add(new Tuple(newR, oldR));
				Condition conditionToDeleteOldRecord = getConditionToDeleteOldRecord(atrName, atrVal, uomVal, atrId, id,
						oldR, qdo, anImport);
				if (conditionToDeleteOldRecord != null)
					conditions.add(conditionToDeleteOldRecord);
			}
		}
		Condition finalCondition = null;
		if (!conditions.isEmpty())
			finalCondition = or(conditions);
		return new Tuple(tuples, finalCondition);
	}

	private void validateAttributeValue(ULong atrId, String atrVal, String atrName) {
		if (atrVal == null)
			return;

		String dataType = this.dslContext.select(TMB_CLIENT_ATTRIBUTE.DATA_TYPE).from(TMB_CLIENT_ATTRIBUTE)
				.where(TMB_CLIENT_ATTRIBUTE.ID.eq(atrId)).fetchOneInto(String.class);
		String[] split = atrVal.split("\\|");
		if (!atrVal.contains("|"))
			split[0] = atrVal;
		for (String a : split) {
			if (EnumUtil.equals(AttributeDataType.N, dataType) && !StringUtils.isNumeric(a))
				throw new IllegalArgumentException(String.format("Data type of %s is Numeric.", atrName));
			else if (EnumUtil.equals(AttributeDataType.B, dataType)
					&& !(a.equalsIgnoreCase("true") || a.equalsIgnoreCase("false") || a.equalsIgnoreCase("yes")
							|| a.equalsIgnoreCase("no") || a.equals("0") || a.equals("1")))
				throw new IllegalArgumentException(String.format(
						"Data type of %s is Boolean, Only either of (true/false/yes/no/0/1) are accepted.", atrName));
		}

	}

	private Condition getConditionToDeleteOldRecord(String atrName, String atrVal, String uomVal, ULong atrId,
			String id, Map<String, Object> oldR, PimDataObject qdo, Import anImport) {
		if (isNotBlank(atrName) && isBlank(atrVal) && isNotBlank(id) && anImport.canHonorNullValue()) {
			return field(ID).eq(id);
		}
		return null;
	}

	private Record getExistingRecord(Import anImport, String id,
			Map<?, SiItemAttributeValueRecord> allExistingAttributes) {
		if (allExistingAttributes == null)
			return null;
		return allExistingAttributes.get(id);
	}

	private Map<String, Object> getNewImportRec(PimDataObject qdo, String atrVal, String uomVal, ULong atrId, String id,
			Record existingRecord, Import anImport) {
		Map<String, Object> recMap = Maps.newHashMap();
		if (existingRecord == null && isBlank(atrVal) && isBlank(uomVal))
			return recMap;
		recMap.put(ID, id);
		recMap.put(ATTRIBUTE_ID, atrId);
		recMap.put(ATTRIBUTE_VALUE_COLUMN, atrVal);
		recMap.put(ATTRIBUTE_UOM_COLUMN, uomVal);
		recMap.put(ITEM_ID, qdo.getValue(ITEM_ID));
		recMap.put(PART_NUMBER_HASH, qdo.getValue(PART_NUMBER_HASH));
		return recMap;
	}

	private Map<?, SiItemAttributeValueRecord> getAllParentRecords(Import anImport, PimDataObject qdo) {
		String itemId = qdo.getValue(ITEM_ID);
		if (anImport.subset.isExternal()) {
			ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
			return dslContext.selectFrom(SI_ITEM_ATTRIBUTE_VALUE).where(SI_ITEM_ATTRIBUTE_VALUE.ITEM_ID.eq(itemId)
					.and(SI_ITEM_ATTRIBUTE_VALUE.PUBLISHER_ID.eq(publisherId))).fetch().intoMap(ID);
		} else {
			SiSubsetDefinitionRecord catalogSubset = itemService.getSubsetDefinition(qdo.getExecutionId(),
					anImport.subset.definition.getParentId());
			Map<?, SiItemAttributeValueRecord> catalogRecords = dslContext
					.fetch(subsetService.getQuery(qdo.getExecutionId(), catalogSubset, ATTRIBUTE, MERGE), itemId)
					.intoMap(ID, SiItemAttributeValueRecord.class);
			if (anImport.subset.isContentWorkspace()) {
				if (catalogRecords == null || catalogRecords.isEmpty()) {
					ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
					Validate.notNull(publisherId,
							"Something is wrong, you need to be publisher to execute the operation");
					return dslContext.selectFrom(SI_ITEM_ATTRIBUTE_VALUE).where(SI_ITEM_ATTRIBUTE_VALUE.ITEM_ID
							.eq(itemId).and(SI_ITEM_ATTRIBUTE_VALUE.PUBLISHER_ID.eq(publisherId))).fetch().intoMap(ID);
				}
			}
			return catalogRecords;
		}
	}

	@Override
	public Map<?, SiItemAttributeValueRecord> getAllExistingAttributes(ULong publisherId, ULong executionId,
			String itemId, SiSubsetDefinitionRecord subsetDefinitionRecord) {
		if (subsetDefinitionRecord.getZeroMembershipAllowed().equals(Byte.valueOf("1"))) {
			SiSubsetDefinitionRecord catalogSubset = itemService.getSubsetDefinition(executionId,
					subsetDefinitionRecord.getParentId());
			Map<?, SiItemAttributeValueRecord> catalogRecords = dslContext
					.fetch(subsetService.getQuery(executionId, catalogSubset, ATTRIBUTE, MERGE), itemId)
					.intoMap(ID, SiItemAttributeValueRecord.class);
			if (catalogRecords == null || catalogRecords.isEmpty()) {
				Validate.notNull(publisherId, "Something is wrong, you need to be publisher to execute the operation");
				return dslContext.selectFrom(SI_ITEM_ATTRIBUTE_VALUE).where(SI_ITEM_ATTRIBUTE_VALUE.ITEM_ID.eq(itemId)
						.and(SI_ITEM_ATTRIBUTE_VALUE.PUBLISHER_ID.eq(publisherId))).fetch().intoMap(ID);
			}
			return catalogRecords;
		}
		return dslContext.fetch(subsetService.getQuery(executionId, subsetDefinitionRecord, ATTRIBUTE, MERGE), itemId)
				.intoMap(ID, SiItemAttributeValueRecord.class);
	}

	@Override
	@Cacheable(value = "ClientAttribute", key = "{#attributeName, #clientId, #executionId}", unless = "#result == null")
	public ULong getClientAttribute(String attributeName, String attributeCode, ULong clientId, ULong executionId) {
		Condition clientCondition = TMB_CLIENT_ATTRIBUTE.CLIENT_ID
				.in(itemService.getMyAllowedClientIds(clientId, executionId));
		Condition nameCondition = TMB_CLIENT_ATTRIBUTE.ATTRIBUTE_CODE.eq(attributeCode)
				.or(TMB_CLIENT_ATTRIBUTE.ATTRIBUTE_NAME.eq(attributeName));
		TmbClientAttributeRecord record = dslContext.selectFrom(Tables.TMB_CLIENT_ATTRIBUTE)
				.where(and(clientCondition, nameCondition)).fetchOne();
		if (record != null)
			return record.getId();
		return null;
	}

	@Override
	@Transactional(isolation = Isolation.READ_COMMITTED)
	public ULong createClientAttribute(String attributeName, String attributeCode, ULong clientId, ULong executionId) {
		dslContext
				.insertInto(Tables.TMB_CLIENT_ATTRIBUTE, Tables.TMB_CLIENT_ATTRIBUTE.ATTRIBUTE_NAME,
						Tables.TMB_CLIENT_ATTRIBUTE.ATTRIBUTE_CODE, Tables.TMB_CLIENT_ATTRIBUTE.CLIENT_ID,
						Tables.TMB_CLIENT_ATTRIBUTE.CREATED_BY)
				.values(attributeName, attributeCode, clientId, clientId).onDuplicateKeyIgnore().execute();
		return attributeService.getClientAttribute(attributeName, attributeCode, clientId, executionId);
	}

	private ULong getAttributeKey(String attributeName, String itemId, Import anImport, PimDataObject pdo) {
		if (anImport.subset.isExternal()) {
			return getOrCreateClientAttribute(attributeName, attributeName, pdo);
		} else {
			List<ULong> attributeIds = dslContext
					.fetch(subsetService.getQuery(pdo.getExecutionId(), anImport.subset.definition, ATTRIBUTE, BASE),
							itemId)
					.getValues("ATTRIBUTE_ID", ULong.class);
			Map<String, ULong> existingAttributes = dslContext.selectFrom(TMB_CLIENT_ATTRIBUTE)
					.where(TMB_CLIENT_ATTRIBUTE.ID.in(attributeIds)).fetch()
					.intoMap(TMB_CLIENT_ATTRIBUTE.ATTRIBUTE_NAME, TMB_CLIENT_ATTRIBUTE.ID);

			if (existingAttributes.containsKey(attributeName)) {
				return existingAttributes.get(attributeName);
			}

			TmbClientAttributeRecord attributeRecord = dslContext.selectFrom(TMB_CLIENT_ATTRIBUTE)
					.where(TMB_CLIENT_ATTRIBUTE.ATTRIBUTE_NAME.eq(attributeName)
							.and(TMB_CLIENT_ATTRIBUTE.CLIENT_ID
									.in(itemService.getMyAllowedClientIds(pdo.getClientId(), pdo.getExecutionId()))))
					.fetchAny();
			if (attributeRecord != null) {
				return attributeRecord.getId();
			} else {
				return getOrCreateClientAttribute(attributeName, attributeName, pdo);
			}
		}
	}

	private ULong getOrCreateClientAttribute(String attributeName, String attributeCode, PimDataObject pdo) {
		ULong atrId = attributeService.getClientAttribute(attributeName, attributeCode, pdo.getClientId(),
				pdo.getExecutionId());
		if (atrId != null)
			return atrId;
		else if (convertToBoolean(pdo.getData().get(CAN_CREATE_ATTRIBUTE))) {
			return attributeService.createClientAttribute(attributeName, attributeCode, pdo.getClientId(),
					pdo.getExecutionId());
		} else {
			String errorMsg = wrap(attributeName, "'")
					+ " attribute does not exist, unable to create attribute due to lack of authority";
			throw new IllegalArgumentException(errorMsg);
		}
	}

	@Override
	public List<TmbClientAttributeRecord> getAttributesRecords(List<ULong> attributesId) {
		return dslContext.selectFrom(TMB_CLIENT_ATTRIBUTE).where(TMB_CLIENT_ATTRIBUTE.ID.in(attributesId)).fetch();
	}
}
