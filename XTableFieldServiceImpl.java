package com.unilog.cx1.pim.commons.service.impl;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newTreeMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.BRAND_OWNER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.BRAND_OWNER_CODE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.GREEN_INDICATOR;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.GTIN_BOX;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.GTIN_CASE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.GTIN_ITEM;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.GTIN_PALLET;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.INNER_PACKS_IN_CASE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ITEM_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MOBILE_DESC;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MSDS_FILE_NAME;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MULTI_BOX;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER_HASH;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PRODUCT_INACTIVE_INDICATOR;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PRODUCT_LIFE_CYCLE_DATE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PROP_65;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PUBLISHER_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.RELATED_SKUS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.REPLACED_BY;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.SAP_MATERIAL_NUMBER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.UNITS_IN_CASE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.UNITS_IN_INNER_PACK;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.X_FIELD_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.X_FIELD_ROW_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.X_FIELD_VALUE;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryTableType.X_FIELD;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryViewType.MERGE;
import static com.unilog.iam.jooq.tables.IamClient.IAM_CLIENT;
import static com.unilog.iam.jooq.tables.SiPublisherClient.SI_PUBLISHER_CLIENT;
import static com.unilog.iam.jooq.tables.SiPublisherSubscriber.SI_PUBLISHER_SUBSCRIBER;
import static com.unilog.prime.commons.util.IDUtil.createHash;
import static com.unilog.prime.commons.util.IntegerUtil.isInteger;
import static com.unilog.prime.commons.util.StringUtil.safeValueOf;
import static com.unilog.prime.jooq.tables.SiClientXField.SI_CLIENT_X_FIELD;
import static com.unilog.prime.jooq.tables.SiItemXFieldValue.SI_ITEM_X_FIELD_VALUE;
import static com.unilog.prime.jooq.tables.SiSubsetDefinition.SI_SUBSET_DEFINITION;
import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.ObjectUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.substringAfter;
import static org.apache.commons.lang3.Validate.isTrue;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.or;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.math.NumberUtils;
import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;
import com.unilog.cx1.pim.commons.service.IXTableFieldService;
import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.commons.util.BooleanUtil;
import com.unilog.prime.commons.util.IntegerUtil;
import com.unilog.prime.jooq.Tables;
import com.unilog.prime.jooq.tables.GlobalAttributeFilterableDefinition;
import com.unilog.prime.jooq.tables.GlobalAttributeFilterableValues;
import com.unilog.prime.jooq.tables.SiClientXField;
import com.unilog.prime.jooq.tables.SiItemXFieldValue;
import com.unilog.prime.jooq.tables.records.GlobalAttributeFilterableDefinitionRecord;
import com.unilog.prime.jooq.tables.records.GlobalAttributeFilterableValuesRecord;
import com.unilog.prime.jooq.tables.records.SiClientXFieldRecord;
import com.unilog.prime.jooq.tables.records.SiItemXFieldValueRecord;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;

@Service(XTableFieldServiceImpl.BEAN_ID)
public class XTableFieldServiceImpl extends AbstractImportService implements IXTableFieldService {

    public static final String BEAN_ID = "xTableFieldService";
    public static final String EX_FIELD_SEPARATOR = "EX_";
	public static final List<String> X_FIELDS = ImmutableList.of(MULTI_BOX, PROP_65,
			PRODUCT_INACTIVE_INDICATOR, MOBILE_DESC, GTIN_ITEM, GTIN_BOX, GTIN_CASE, GTIN_PALLET, INNER_PACKS_IN_CASE,
			UNITS_IN_CASE, UNITS_IN_INNER_PACK, PRODUCT_LIFE_CYCLE_DATE, MSDS_FILE_NAME, BRAND_OWNER_CODE, BRAND_OWNER,
			REPLACED_BY, GREEN_INDICATOR, RELATED_SKUS, SAP_MATERIAL_NUMBER);

    @Override
    @Transactional
    public void doImport(Import anImport, PimDataObject qdo) {
        Tuple<List<Tuple<Map<String, Object>, Map<String, Object>>>, Condition> tuple = getTuplesToInsert(anImport, qdo);
        Set<String> notNullColumns = newHashSet(X_FIELD_ID, X_FIELD_ROW_ID);
        super.doImportInItemSubTable(anImport, qdo, tuple, anImport.subset.getXFieldTableInsertTable(),
                anImport.subset.getXFieldTableInsertColumns(), List.of(X_FIELD_VALUE), of(X_FIELD_ID, X_FIELD_ROW_ID), notNullColumns);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	private Tuple<List<Tuple<Map<String, Object>, Map<String, Object>>>, Condition> getTuplesToInsert(Import anImport, PimDataObject qdo) {
        List<Tuple> tuples = Lists.newArrayList();
        List<Condition> conditions = Lists.newArrayList();
        String itemId = qdo.getValue(ITEM_ID);
        
        Set<String> xFieldAttributeNames = new HashSet<String>();
        
        for(Map.Entry<String, Object> entryObj : qdo.getData().entrySet())
        {
        	if(StringUtils.startsWith(entryObj.getKey(), "EX_"))
        	{
        		xFieldAttributeNames.add(entryObj.getKey());
        	}
        }
                 
        if (isEmpty(xFieldAttributeNames))
            return new Tuple(tuples, null);

        Map<?, SiItemXFieldValueRecord> allExistingRecords = getParentRecords(anImport, qdo);
        for (String tempAtrName : xFieldAttributeNames) {
            String atrName = substringAfter(tempAtrName, EX_FIELD_SEPARATOR).trim();
            Record xField = getXField(atrName, qdo.getClientId(), qdo.getExecutionId());
            Map<String, Object> xFieldMap = xField == null ? newTreeMap() : xField.intoMap();
            String atrVal = qdo.getValue(tempAtrName);
			List<String> headers = X_FIELDS;
			if (qdo.getSourceFormat() != null && qdo.getSourceFormat().getHeader() != null)
				headers = qdo.getSourceFormat().getHeader();
			
//			atrVal = validateValidValue(atrName, atrVal, qdo); //Commenting since are not picking xfields right now
			
            validate(atrName, atrVal, xFieldMap, qdo.getClientId(), headers);
            String id = createHash(itemId, xField.get(ID));
            Record existingRecord = getExistingRecord(id, allExistingRecords);
            Map<String, Object> oldR = existingRecord == null ? newTreeMap() : existingRecord.intoMap();
            Map<String, Object> newR = getNewImportRec(qdo, id, atrVal, xFieldMap, anImport);
            tuples.add(new Tuple(newR, oldR));
            Condition conditionToDeleteOldRecord = getConditionToDeleteOldRecord(atrName, atrVal, xField.get(ID, ULong.class), id, oldR, qdo, anImport);
            if (conditionToDeleteOldRecord != null)
                conditions.add(conditionToDeleteOldRecord);
        }

        Condition finalCondition = null;
        if (!conditions.isEmpty())
            finalCondition = or(conditions);
        return new Tuple(tuples, finalCondition);
    }

    /**
     * Method to support valid value check for x fields
     */
    @SuppressWarnings("unchecked")
	private String validateValidValue(String atrName, String atrVal, PimDataObject qdo) {
    	
		ULong publisherClientId = this.getPubClientIdViaClientId(qdo.getClientId()); 
    	
		SiClientXFieldRecord rec = this.dslContext.selectFrom(SiClientXField.SI_CLIENT_X_FIELD)
	    		.where(SiClientXField.SI_CLIENT_X_FIELD.CLIENT_ID.eq(publisherClientId))
	    		.and(SiClientXField.SI_CLIENT_X_FIELD.FIELD_NAME.eq(atrName)).fetchOne();
    	
		if(rec==null) return atrVal;
		
		GlobalAttributeFilterableDefinitionRecord def = this.dslContext.selectFrom(GlobalAttributeFilterableDefinition.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION)
					.where(GlobalAttributeFilterableDefinition.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION.CLIENT_ID.eq(publisherClientId))
					.and(GlobalAttributeFilterableDefinition.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION.IS_XFIELD.eq(Byte.valueOf("1")))
					.and(GlobalAttributeFilterableDefinition.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION.GLOBAL_OR_XFIELD_ATTRIBUTE_ID.eq(rec.getId()))
					.fetchOne();
		
		if(def == null) {
			return atrVal;
		}
		
    	if(isNotBlank(atrVal)) {
			GlobalAttributeFilterableValuesRecord valueRec = this.dslContext.selectFrom(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES)
					.where(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION_ID.eq(def.getId()))
					.and(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.VALID_VALUE.eq(atrVal))
					.fetchOne();

			if(valueRec==null) {
				List<String> valsName = this.dslContext.select(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.VALID_VALUE)
						.from(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES)
						.where(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION_ID.eq(def.getId()))
						.fetchInto(String.class);
				
				if(valsName.isEmpty()) return atrVal;

				throw new IllegalArgumentException("The value entered for "+ atrName +" is not an acceptable value. The valid values are " + valsName + ".");
			}
    	} else if(def.getHasDefault().intValue()==1) {
    		atrVal = this.dslContext.select(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.VALID_VALUE)
					.from(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES)
					.where(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION_ID.eq(def.getId()))
					.and(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.IS_DEFAULT.eq(Byte.valueOf("1")))
					.fetchOneInto(String.class);
    		
    		qdo.getData().put("EX_".concat(atrName), atrVal);
    	}
    	
    	return atrVal;
    }

    @SuppressWarnings("unchecked")
	private void validate(String atrName, String atrVal, Map<String, Object> xFieldMap, ULong clientId,
			List<String> headers) {
		isTrue(!xFieldMap.isEmpty(),
				"X-Field = '" + atrName + "' definition record not found for user = " + clientId);
		Map<String, Object> xFieldValidation = (Map<String, Object>) xFieldMap.get("FIELD_VALIDATION");

		if (isNotBlank(atrVal)) {
			String type = safeValueOf(xFieldMap.get("FIELD_TYPE"));
			if (type.equals("INT")) {
				if (!isInteger(atrVal))
					throw new IllegalArgumentException(atrName + " value should be integer");
			} else if (type.equals("DECIMAL")) {
				if (!NumberUtils.isParsable(atrVal))
					throw new IllegalArgumentException(atrName + " value should be decimal");
			}
		}

		if (xFieldValidation != null && !xFieldValidation.isEmpty()) {
			if (BooleanUtil.convertToBoolean(xFieldValidation.get("mandatory"))) {
				if (!headers.contains(atrName))
					throw new IllegalArgumentException(atrName + " is mandatory field, not present in import headers");
				if (isBlank(atrVal))
					throw new IllegalArgumentException(atrName + " is mandatory field, value can not be blank");
			}
			if (isNotEmpty(xFieldValidation.get("maxLength"))) {
				int max = IntegerUtil.safeValueOf(xFieldValidation.get("maxLength"));
				if (isNotBlank(atrVal) && atrVal.length() > max)
					throw new IllegalArgumentException(atrName + " can't be more than " + max + " characters");
			}
		}
	}

	@Override
	@Cacheable(value = "XField", key = "{#atrName, #clientId, #executionId}", unless = "#result == null")
	public Record getXField(String atrName, ULong clientId, ULong executionId) {
		return this.dslContext.select(SI_CLIENT_X_FIELD.fields()).from(SI_CLIENT_X_FIELD)
				.where(SI_CLIENT_X_FIELD.CLIENT_ID.in(this.getAllowedClientIdsThruClientId(clientId))
						.and(SI_CLIENT_X_FIELD.FIELD_NAME.eq(atrName)))
				.orderBy(SI_CLIENT_X_FIELD.CLIENT_ID.desc()).limit(1).fetchOne();
	}

    private Condition getConditionToDeleteOldRecord(String atrName, String atrVal, ULong xFieldId,
                                                    String id, Map<String, Object> oldR, PimDataObject qdo, Import anImport) {
        List<Condition> list = newArrayList();
        if (isNotBlank(atrName) && isBlank(atrVal) && isNotBlank(id) && anImport.canHonorNullValue()) {
            list.add(field(ID).eq(id));
        }
        if (oldR != null && atrVal != null &&
                ObjectUtils.equals(oldR.get(X_FIELD_ID), xFieldId) && anImport.subset.isWorkspace())
            list.add(field(ID).eq(oldR.get(ID)));
        return list.isEmpty() ? null : DSL.or(list);
    }

    private SiItemXFieldValueRecord getExistingRecord(String id, Map<?, SiItemXFieldValueRecord> allExistingRecords) {
        if (allExistingRecords == null)
            return null;
        return allExistingRecords.get(id);
    }

    private Map<String, Object> getNewImportRec(PimDataObject qdo, String id, String atrVal,
                                                Map<String, Object> xFieldMap, Import anImport) {
        Map<String, Object> recMap = Maps.newHashMap();
        recMap.put(ID, id);
        recMap.put(X_FIELD_ID, xFieldMap.get(ID));
        recMap.put(X_FIELD_VALUE, atrVal);
        recMap.put(X_FIELD_ROW_ID, null);
        recMap.put(ITEM_ID, qdo.getValue(ITEM_ID));
        recMap.put(PART_NUMBER_HASH, qdo.getValue(PART_NUMBER_HASH));
        return recMap;
    }

    private Map<?, SiItemXFieldValueRecord> getParentRecords(Import anImport, PimDataObject qdo) {
        String itemId = qdo.getValue(ITEM_ID);
        if (anImport.subset.isExternal()) {
            ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
            return dslContext.selectFrom(Tables.SI_ITEM_X_FIELD_VALUE).where(SiItemXFieldValue.SI_ITEM_X_FIELD_VALUE.ITEM_ID
                    .eq(itemId).and(SiItemXFieldValue.SI_ITEM_X_FIELD_VALUE.PUBLISHER_ID.eq(publisherId))).fetch().intoMap(ID);
        } else {
            SiSubsetDefinitionRecord catalogSubset = this.getSubsetDefinition(qdo.getExecutionId(), anImport.subset.definition.getParentId());
            Map<?, SiItemXFieldValueRecord> catalogRecords = dslContext.fetch(subsetService.getQuery(qdo.getExecutionId(), catalogSubset, X_FIELD, MERGE), itemId).intoMap(ID, SiItemXFieldValueRecord.class);
            if (anImport.subset.isContentWorkspace()) {
                if (catalogRecords == null || catalogRecords.isEmpty()) {
                    ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
                    Validate.notNull(publisherId, "Something is wrong, you need to be publisher to execute the operation");
                    return dslContext.selectFrom(Tables.SI_ITEM_X_FIELD_VALUE).where(SiItemXFieldValue.SI_ITEM_X_FIELD_VALUE.ITEM_ID
                            .eq(itemId).and(SiItemXFieldValue.SI_ITEM_X_FIELD_VALUE.PUBLISHER_ID.eq(publisherId))).fetch().intoMap(ID);
                }
            }
            return catalogRecords;
        }
    }

    @Override
    public Map<?, SiItemXFieldValueRecord> getAllExistingRecords(ULong publisherId, ULong executionId, String itemId, SiSubsetDefinitionRecord subsetDefinitionRecord) {
        if (subsetDefinitionRecord.getZeroMembershipAllowed().equals(Byte.valueOf("1"))) {
            SiSubsetDefinitionRecord catalogSubset = this.getSubsetDefinition(executionId, subsetDefinitionRecord.getParentId());
            Map<?, SiItemXFieldValueRecord> catalogRecords = dslContext.fetch(subsetService.getQuery(executionId, catalogSubset, X_FIELD, MERGE), itemId).intoMap(ID, SiItemXFieldValueRecord.class);
            if (catalogRecords == null || catalogRecords.isEmpty()) {
                return dslContext.selectFrom(Tables.SI_ITEM_X_FIELD_VALUE).where(SI_ITEM_X_FIELD_VALUE.ITEM_ID
                        .eq(itemId).and(SI_ITEM_X_FIELD_VALUE.PUBLISHER_ID.eq(publisherId))).fetch().intoMap(ID);
            }
            return catalogRecords;
        }
        return dslContext.fetch(subsetService.getQuery(executionId, subsetDefinitionRecord, X_FIELD, MERGE), itemId).intoMap(ID, SiItemXFieldValueRecord.class);
    }

    @Override
    public List<SiClientXFieldRecord> getPublisherXFieldRecords(List<ULong> ids) {
        return dslContext.selectFrom(SI_CLIENT_X_FIELD).where(SI_CLIENT_X_FIELD.ID.in(ids)).fetch();
    }
    
	private Set<ULong> getAllowedClientIdsThruClientId(ULong clientId) {
		String clientTypeCode = this.dslContext.select(IAM_CLIENT.CLIENT_TYPE_CODE).from(IAM_CLIENT)
				.where(IAM_CLIENT.ID.eq(clientId)).limit(1).fetchOneInto(String.class);

		Set<ULong> set = new HashSet<>();
		if (!"P".equals(clientTypeCode)) {
			ULong publisherClientId = this.dslContext.select(SI_PUBLISHER_CLIENT.CLIENT_ID).from(SI_PUBLISHER_CLIENT)
					.join(SI_PUBLISHER_SUBSCRIBER).on(SI_PUBLISHER_SUBSCRIBER.PUBLISHER_ID.eq(SI_PUBLISHER_CLIENT.ID))
					.where(SI_PUBLISHER_SUBSCRIBER.CLIENT_ID.eq(clientId)).limit(1).fetchOneInto(ULong.class);
			set.add(publisherClientId);
		}

		set.add(clientId);
		return set;
	}
	
	private ULong getPubClientIdViaClientId(ULong clientId) {
		String clientTypeCode = this.dslContext.select(IAM_CLIENT.CLIENT_TYPE_CODE).from(IAM_CLIENT)
				.where(IAM_CLIENT.ID.eq(clientId)).limit(1).fetchOneInto(String.class);

		if (!"P".equals(clientTypeCode)) {
			ULong publisherClientId = this.dslContext.select(SI_PUBLISHER_CLIENT.CLIENT_ID).from(SI_PUBLISHER_CLIENT)
					.join(SI_PUBLISHER_SUBSCRIBER).on(SI_PUBLISHER_SUBSCRIBER.PUBLISHER_ID.eq(SI_PUBLISHER_CLIENT.ID))
					.where(SI_PUBLISHER_SUBSCRIBER.CLIENT_ID.eq(clientId)).limit(1).fetchOneInto(ULong.class);
			
			return publisherClientId;
		}

		return clientId;
	}
	
	private SiSubsetDefinitionRecord getSubsetDefinition(ULong executionId, ULong subsetId) {

        return this.dslContext.selectFrom(SI_SUBSET_DEFINITION).where(SI_SUBSET_DEFINITION.ID.eq(subsetId)).fetchOne();
    }

}
