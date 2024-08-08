package com.unilog.cx1.pim.commons.service.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;
import com.unilog.cx1.pim.commons.service.ICategoryService;
import com.unilog.prime.commons.exception.PrimeException;
import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.commons.util.BooleanUtil;
import com.unilog.prime.commons.util.StringUtil;
import com.unilog.prime.jooq.enums.SiSubsetDefinitionSubsetType;
import com.unilog.prime.jooq.tables.records.SiItemCategoryRecord;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.jooq.Condition;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Timestamp;
import java.util.*;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newTreeMap;
import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.CAN_CREATE_MULTIPLE_CATEGORY;
import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.FORCE_DELETE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.*;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryTableType.CATEGORY;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryViewType.MERGE;
import static com.unilog.cx1.pim.commons.enumeration.UpdateType.OVER_WRITE_ALL_VALUES;
import static com.unilog.cx1.pim.commons.enumeration.UpdateType.OVER_WRITE_WITH_ALL_AND_BLANK_VALUE;
import static com.unilog.prime.commons.util.IDUtil.createHash;
import static com.unilog.prime.commons.util.StringUtil.safeValueOf;
import static com.unilog.prime.commons.util.TextUtil.anyToDBFormat;
import static com.unilog.prime.jooq.tables.SiItemCategory.SI_ITEM_CATEGORY;
import static com.unilog.prime.jooq.tables.SiPublisherTaxonomy.SI_PUBLISHER_TAXONOMY;
import static com.unilog.prime.jooq.tables.TmbClientCategory.TMB_CLIENT_CATEGORY;
import static com.unilog.prime.jooq.tables.TmbClientTaxonomyTreeNode.TMB_CLIENT_TAXONOMY_TREE_NODE;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.jooq.impl.DSL.and;
import static org.jooq.impl.DSL.field;
import static org.springframework.transaction.annotation.Isolation.READ_COMMITTED;
import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.IS_MANUFACTURER_CATEGORY_MAPPING;

@Service(CategoryServiceImpl.BEAN_ID)
public class CategoryServiceImpl extends AbstractImportService implements ICategoryService {

    public static final String BEAN_ID = "etlCategoryService";
    
    @Override
    @Transactional(isolation = READ_COMMITTED)
    public void doImport(Import anImport, PimDataObject qdo) {
        if (qdo.contains(CATEGORY_CODE) || qdo.contains(CATEGORY_CODES + 1)) {
            Map<?, SiItemCategoryRecord> existingRecord = getAllParentRecords(anImport, qdo);
            Tuple<List<Tuple<Map<String, Object>, Map<String, Object>>>, Condition> tuple = getTuple(anImport, qdo,
                    existingRecord);
            List<Tuple<Map<String, Object>, Map<String, Object>>> recs = tuple.getFirstValue();
            Condition conditionToDeleteOldRecord = tuple.getSecondValue();
            super.doImportInItemSubTable(anImport, qdo, new Tuple(recs, conditionToDeleteOldRecord),
                    anImport.subset.getCategoryInsertTable(), anImport.subset.getCategoryInsertColumns(), of(),
                    of(CATEGORY_ID), Set.of(CATEGORY_ID, DEFAULT_CATEGORY));
        }
    }

    private Tuple<List<Tuple<Map<String, Object>, Map<String, Object>>>, Condition> getTuple(Import anImport,
                                                                                             PimDataObject qdo, Map<?, SiItemCategoryRecord> existingRecord) {
        List<Tuple<Map<String, Object>, Map<String, Object>>> tuples = Lists.newArrayList();
        List<Condition> conditions = Lists.newArrayList();
        Set<ULong> newCategorySet = new HashSet<>();
        boolean flag = true;
        int count = 0;
        boolean isPrimaryDefine = false;

        while (flag) {
            String catCodeKey = CATEGORY_CODE + (count > 0 ? "_" + count : "");
            String catNameKey = CATEGORY_NAME + (count > 0 ? "_" + count : "");
            String categoryCode = qdo.getValue(catCodeKey);
            String categoryName = qdo.getValue(catNameKey);
            if ((!qdo.contains(catCodeKey) || Boolean.TRUE.equals(!Boolean.valueOf(qdo.getValue(CAN_CREATE_MULTIPLE_CATEGORY))))
            		&& count > 1)
                break;
            
			/*
			 * if(Boolean.TRUE.equals(!Boolean.valueOf(qdo.getValue(
			 * CAN_CREATE_MULTIPLE_CATEGORY))) && anImport.isRetain() &&
			 * anImport.getSubset().isExternal() && !StringUtils.isBlank(categoryCode)) {
			 * ULong oldCategoryId = this.dslContext.select(SI_ITEM_CATEGORY.CATEGORY_ID)
			 * .from(SI_ITEM_CATEGORY)
			 * .where(SI_ITEM_CATEGORY.ITEM_ID.eq(qdo.getValue(ITEM_ID)))
			 * .fetchOneInto(ULong.class); if ( oldCategoryId != null ) { throw new
			 * PrimeException(HttpStatus.BAD_REQUEST,
			 * "An item cannot be assigned to multiple categories. Please ensure this item is only assigned to one category."
			 * );
			 * 
			 * } }
			 * 
			 * if(Boolean.TRUE.equals(!Boolean.valueOf(qdo.getValue(
			 * CAN_CREATE_MULTIPLE_CATEGORY))) && anImport.isRetain() &&
			 * (anImport.getSubset().isWorkspace() ||
			 * anImport.getSubset().isContentWorkspace()) &&
			 * !StringUtils.isBlank(categoryCode)) { Table<?> newtable =
			 * DSL.table(anImport.subset.definition.getSchemaName() + "." +
			 * anImport.subset.definition.getCategoryMergedTableName()); ULong oldCategoryId
			 * = this.dslContext.select(field(CATEGORY_ID)) .from(newtable)
			 * .where(field(ITEM_ID).eq(qdo.getValue(ITEM_ID))) .fetchOneInto(ULong.class);
			 * if ( oldCategoryId != null ) { throw new
			 * PrimeException(HttpStatus.BAD_REQUEST,
			 * "An item cannot be assigned to multiple categories. Please ensure this item is only assigned to one category."
			 * ); }
			 * 
			 * }
			 */
            
            if (StringUtils.isBlank(categoryCode)) {
                count++;
                continue;
            }
 
            preProcess(qdo, catCodeKey, categoryCode, catNameKey, categoryName);
            ULong categoryKey = categoryService.getCategoryKey(anImport.subset.definition, categoryCode, categoryName,
                    qdo.getClientId(), qdo.getExecutionId());
            if (categoryKey == null)
                throw new PrimeException(HttpStatus.BAD_REQUEST, "The provided CATEGORY_NAME either does not exist in CX1 PIM or it is not a leaf node category (end category).");
            SiItemCategoryRecord oldRecord = existingRecord.get(categoryKey);
            Map<String, Object> oldR = oldRecord == null ? newTreeMap() : oldRecord.intoMap();
            boolean isFirstCatToBeAssigned = existingRecord.isEmpty();
            //if importing the already existing default category again for multiple category enabled user 
            if(!isFirstCatToBeAssigned && oldRecord != null && oldR.get(DEFAULT_CATEGORY).equals((byte)1)) 
            	isFirstCatToBeAssigned = true;
            Map<String, Object> newR = getNewImportRec(qdo, oldR, anImport, categoryKey, count, isFirstCatToBeAssigned);
			if ((newR.get(DEFAULT_CATEGORY).equals(1) && !anImport.isRetain()) 
					|| (newR.get(DEFAULT_CATEGORY).equals(1) && Boolean.TRUE.equals(!Boolean.valueOf(qdo.getValue(IS_MANUFACTURER_CATEGORY_MAPPING)))))
				isPrimaryDefine = true;
            tuples.add(new Tuple<>(newR, oldR));
            newCategorySet.add(categoryKey);

            validateImport(qdo, newR, count);
            Condition conditionToDeleteOldRecord = getConditionToDeleteOldRecord(newR, oldR, qdo, anImport,
                    categoryCode, categoryName);
            if (conditionToDeleteOldRecord != null)
                conditions.add(conditionToDeleteOldRecord);
            count++;
        }

        if (Boolean.valueOf(qdo.getValue(CAN_CREATE_MULTIPLE_CATEGORY)))
            cleanUp(anImport, qdo, tuples, existingRecord, newCategorySet, isPrimaryDefine);
        deleteOldRecords(anImport, qdo, tuples, conditions, existingRecord, newCategorySet);
        
        if(Boolean.TRUE.equals((anImport.updateType.equals(OVER_WRITE_WITH_ALL_AND_BLANK_VALUE) 
        		  || anImport.updateType.equals(OVER_WRITE_ALL_VALUES)) 
        		&& qdo.contains(CAN_CREATE_MULTIPLE_CATEGORY) 
        		&& Boolean.valueOf(qdo.getValue(CAN_CREATE_MULTIPLE_CATEGORY))
        		&& qdo.contains(CATEGORY_CODE)) && qdo.getValue(CATEGORY_CODE) == null) {
        	modifyRecordsTupleList(tuples);
        }

        if (anImport.updateType.equals(OVER_WRITE_WITH_ALL_AND_BLANK_VALUE)
                || anImport.updateType.equals(OVER_WRITE_ALL_VALUES)) {
            List<Condition> coniditionList = newArrayList();
            coniditionList.add(field(ITEM_ID).eq(qdo.getValue(ITEM_ID)));
            coniditionList.add(field(CATEGORY_ID).notIn(newCategorySet));
            coniditionList.add(field(CATEGORY_ID).notEqual(-1));
            if (anImport.subset.isExternal())
                coniditionList.add(field(PUBLISHER_ID).eq(qdo.getValue(PUBLISHER_ID)));
            conditions.add(DSL.and(coniditionList));
        } else if (qdo.contains(CAN_CREATE_MULTIPLE_CATEGORY)
                && Boolean.valueOf(qdo.getValue(CAN_CREATE_MULTIPLE_CATEGORY)) && anImport.isRetain()) {
            conditions = Lists.newArrayList();
        } else if (Boolean.TRUE.equals(Boolean.valueOf(qdo.getValue(CAN_CREATE_MULTIPLE_CATEGORY)))
				&& Boolean.TRUE.equals(Boolean.valueOf(qdo.getValue(IS_MANUFACTURER_CATEGORY_MAPPING)))) {
        	
            conditions = Lists.newArrayList();
        }

        Condition finalCondition = null;
        if (!conditions.isEmpty())
            finalCondition = DSL.or(conditions);
        return new Tuple(tuples, finalCondition);

    }
    
    private void modifyRecordsTupleList(List<Tuple<Map<String, Object>, Map<String, Object>>> tupleList) {
    	tupleList.sort((e1, e2) -> Timestamp.valueOf(e1.getFirstValue().get(UPDATED_AT).toString())
				.compareTo(Timestamp.valueOf(e2.getFirstValue().get(UPDATED_AT).toString())));
    	Collections.reverse(tupleList); 	
    	boolean flag = true;
    	List<Integer> indicesToBeRemoved = new ArrayList<>();
    	for(int i=0; i< tupleList.size(); i++) {
    		Map<String,Object> newRecord = tupleList.get(i).getFirstValue();
    		Map<String,Object> oldRecord = tupleList.get(i).getSecondValue();
    		if(newRecord.get(CATEGORY_ID).toString().equals("-1") 
    				&& newRecord.get(DEFAULT_CATEGORY).toString().equals("1")) {
    			newRecord.put(DEFAULT_CATEGORY, "0");
    		}
    		else if(newRecord.get(CATEGORY_ID).toString().equals("-1") 
    				&& newRecord.get(DEFAULT_CATEGORY).toString().equals("0") 
    				&& Boolean.TRUE.equals(flag)) {
    			newRecord.put(CATEGORY_ID, oldRecord.get(CATEGORY_ID));
    			newRecord.put(DEFAULT_CATEGORY, "1");
    			flag = false;
    		} 
    		else if(newRecord.get(CATEGORY_ID).toString().equals("-1") 
    				&& newRecord.get(DEFAULT_CATEGORY).toString().equals("0") 
    				&& Boolean.FALSE.equals(flag)) {
    			indicesToBeRemoved.add(i);
    		}
    	}
    	for(int i=0; i< indicesToBeRemoved.size(); i++) {
    		Tuple<Map<String,Object>, Map<String,Object>> object = tupleList.get(indicesToBeRemoved.get(i));
    		tupleList.remove(object);
    	}
    }

    private void cleanUp(Import anImport, PimDataObject qdo, List<Tuple<Map<String, Object>, Map<String, Object>>> tuples,
                         Map<?, SiItemCategoryRecord> existingRecord, Set<ULong> newCategorySet, boolean isPrimaryDefine) {

        if (!isPrimaryDefine)
            return;

        if (anImport.subset.isWorkspace())
            dslContext.update(anImport.subset.getCategoryInsertTable()).set(field(DEFAULT_CATEGORY), Byte.valueOf("0"))
                    .where(and(field(ITEM_ID).eq(qdo.getValue(ITEM_ID)), field(DEFAULT_CATEGORY).eq(Byte.valueOf("1"))))
                    .execute();

        Collection<SiItemCategoryRecord> set = existingRecord.values();
        for (SiItemCategoryRecord oldR : set) {
            if (oldR.getDefaultCategory().equals(Byte.valueOf("0")) || newCategorySet.contains(oldR.getCategoryId()))
                continue;

            Map<String, Object> newR = oldR.intoMap();
            newR.put(DEFAULT_CATEGORY, 0);
            anImport.cleanUp(newR, oldR.intoMap(), tuples);
        }
    }

    private void deleteOldRecords(Import anImport, PimDataObject qdo,
                                  List<Tuple<Map<String, Object>, Map<String, Object>>> tuples, List<Condition> conditions,
                                  Map<?, SiItemCategoryRecord> existingRecord, Set<ULong> newCategorySet) {

        if (newCategorySet.isEmpty() && qdo.contains(CAN_CREATE_MULTIPLE_CATEGORY)
                && !Boolean.valueOf(qdo.getValue(CAN_CREATE_MULTIPLE_CATEGORY)) && anImport.isRetain())
            return;
        

        if (newCategorySet.isEmpty() && Boolean.TRUE.equals(!Boolean.valueOf(qdo.getValue(CAN_CREATE_MULTIPLE_CATEGORY)))
				&& Boolean.TRUE.equals(Boolean.valueOf(qdo.getValue(IS_MANUFACTURER_CATEGORY_MAPPING))))
            return;

        Collection<SiItemCategoryRecord> set = existingRecord.values();
        for (SiItemCategoryRecord oldR : set) {
			if ((oldR == null || newCategorySet.contains(oldR.getCategoryId()))
					|| (qdo.contains(CAN_CREATE_MULTIPLE_CATEGORY)
							&& Boolean.valueOf(qdo.getValue(CAN_CREATE_MULTIPLE_CATEGORY)) && anImport.isRetain())
					|| (Boolean.TRUE.equals(Boolean.valueOf(qdo.getValue(CAN_CREATE_MULTIPLE_CATEGORY)))
							&& Boolean.TRUE.equals(Boolean.valueOf(qdo.getValue(IS_MANUFACTURER_CATEGORY_MAPPING)))))
				continue;

            Map<String, Object> newR = oldR.intoMap();
            Condition conditionToDeleteOldRecord = getConditionToDeleteOldRecord(newR, oldR.intoMap(), qdo, anImport,
                    null, null);
            if (conditionToDeleteOldRecord != null)
                conditions.add(conditionToDeleteOldRecord);

            newR.put(CATEGORY_ID, null);

            anImport.deleteOldRecords(newR, oldR.intoMap(), Collections.emptyMap(), tuples, Collections.emptyList(),
                    List.of(CATEGORY_ID, DEFAULT_CATEGORY));

        }
    }
    
    private Condition getConditionToDeleteOldRecord(Map<String, Object> newR, Map<String, Object> oldR,
                                                    PimDataObject qdo, Import anImport, String categoryCode, String categoryName) {
        ULong categoryId = (ULong) newR.get(CATEGORY_ID);
        List<Condition> list = newArrayList();
        if (isBlank(categoryCode) && isBlank(categoryName) && anImport.canHonorNullValue()) {
            list.add(field(ITEM_ID).eq(qdo.getValue(ITEM_ID)));
            if (anImport.subset.isExternal())
                list.add(field(PUBLISHER_ID).eq(qdo.getValue(PUBLISHER_ID)));
        }
        if (anImport.subset.isWorkspace() || (qdo.contains(CAN_CREATE_MULTIPLE_CATEGORY)
                && !Boolean.valueOf(qdo.getValue(CAN_CREATE_MULTIPLE_CATEGORY)) && anImport.isRetain()
                && anImport.subset.isExternal())) {
            list.add(field(ITEM_ID).eq(qdo.getValue(ITEM_ID)));
            list.add(field(CATEGORY_ID).notEqual(categoryId));
            if (anImport.subset.isExternal())
                list.add(field(PUBLISHER_ID).eq(qdo.getValue(PUBLISHER_ID)));
            qdo.getData().put(FORCE_DELETE, true);
        }
        if (ObjectUtils.equals(oldR.get(CATEGORY_ID), newR.get(CATEGORY_ID)) && anImport.subset.isWorkspace()) {
            list.add(field(ITEM_ID).eq(qdo.getValue(ITEM_ID)));
        }
        return list.isEmpty() ? null : DSL.and(list);
    }

    private void preProcess(PimDataObject qdo, String catCodeKey, String categoryCode, String catNameKey,
                            String categoryName) {
        Map<String, Object> data = qdo.getData();
        data.put(catCodeKey, anyToDBFormat(safeValueOf(categoryCode)));
        data.put(catNameKey, anyToDBFormat(safeValueOf(categoryName)));
    }

    private Map<?, SiItemCategoryRecord> getAllParentRecords(Import anImport, PimDataObject qdo) {
        String itemId = qdo.getData().get(ITEM_ID).toString();
        if (anImport.subset.isExternal()) {
            ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
            return dslContext.selectFrom(SI_ITEM_CATEGORY)
                    .where(SI_ITEM_CATEGORY.ITEM_ID.eq(itemId).and(SI_ITEM_CATEGORY.PUBLISHER_ID.eq(publisherId)))
                    .fetchMap(CATEGORY_ID, SiItemCategoryRecord.class);
        } else {
            SiSubsetDefinitionRecord catalogSubset = itemService.getSubsetDefinition(qdo.getExecutionId(),
                    anImport.subset.definition.getParentId());
            Map<?, SiItemCategoryRecord> catalogRecord = dslContext
                    .fetch(subsetService.getQuery(qdo.getExecutionId(), catalogSubset, CATEGORY, MERGE), itemId)
                    .intoMap(CATEGORY_ID, SiItemCategoryRecord.class);
            if (catalogRecord.isEmpty() && anImport.subset.isContentWorkspace()) {
                ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
                Validate.notNull(publisherId, "Something is wrong, you need to be publisher to execute the operation");
                return dslContext.selectFrom(SI_ITEM_CATEGORY)
                        .where(SI_ITEM_CATEGORY.ITEM_ID.eq(itemId).and(SI_ITEM_CATEGORY.PUBLISHER_ID.eq(publisherId)))
                        .fetchMap(CATEGORY_ID, SiItemCategoryRecord.class);
            }
            
            if(catalogRecord.isEmpty()) {
            	SiSubsetDefinitionRecord workspaceSubset = itemService.getSubsetDefinition(qdo.getExecutionId(),
                        anImport.subset.definition.getId());
                catalogRecord = dslContext
                        .fetch(subsetService.getQuery(qdo.getExecutionId(), workspaceSubset, CATEGORY, MERGE), itemId)
                        .intoMap(CATEGORY_ID, SiItemCategoryRecord.class);
            }
            return catalogRecord;
        }
    }

    private Map<String, Object> getNewImportRec(PimDataObject qdo, Map<String, Object> oldR, Import anImport,
                                                ULong categoryKey, int count, boolean isFirstCatToBeAssigned) {
        String categoryCode = qdo.getValue(CATEGORY_CODE + (count > 0 ? "_" + count : ""));
        String catDefaultKey = DEFAULT_CATEGORY + (count > 0 ? "_" + count : "");
        Map<String, Object> newRecMap = Maps.newHashMap();
        newRecMap.put(CATEGORY_ID, categoryKey);
        newRecMap.put(ITEM_ID, qdo.getValue(ITEM_ID));
        newRecMap.put(PART_NUMBER_HASH, qdo.getValue(PART_NUMBER_HASH));
        if (isBlank(categoryCode) && oldR != null) {
            newRecMap.put(ID, oldR.get(ID));
        } else {
            newRecMap.put(ID, createHash(qdo.getValue(ITEM_ID), categoryKey));
        }

        if (qdo.contains(catDefaultKey)) {
            boolean vIn = BooleanUtil.convertToBoolean(qdo.getValue(catDefaultKey));
            byte vOut = (byte) (vIn ? 1 : 0);
            newRecMap.put(DEFAULT_CATEGORY, vOut);
        } else {
        	if(Boolean.TRUE.equals(Boolean.valueOf(qdo.getValue(CAN_CREATE_MULTIPLE_CATEGORY))) 
        			&& Boolean.TRUE.equals(!Boolean.valueOf(isFirstCatToBeAssigned))) {
        		if(Boolean.TRUE.equals(Boolean.valueOf(qdo.getValue(IS_MANUFACTURER_CATEGORY_MAPPING)))){
        			newRecMap.put(DEFAULT_CATEGORY, (count  <= 1  ? 0 : 1)); 
        		}else {
        			newRecMap.put(DEFAULT_CATEGORY, (count  <= 1 && !anImport.isRetain()) ? 1 : 0); 
        		}
        		       		
        	} else {
        		newRecMap.put(DEFAULT_CATEGORY, (count  <= 1 ? 1 : 0));
        	}
        		
        }

        return newRecMap;
    }

    private void validateImport(PimDataObject qdo, Map<String, Object> newR, int count) {
        Map<String, Object> data = qdo.getData();
        String categoryCode = StringUtil.safeValueOf(data.get(CATEGORY_CODE + (count > 0 ? "_" + count : "")));
        String categoryName = StringUtil.safeValueOf(data.get(CATEGORY_NAME + (count > 0 ? "_" + count : "")));
        if (isNotBlank(categoryCode) || isNotBlank(categoryName))
            if (newR.get(CATEGORY_ID) == null)
                throw new IllegalArgumentException(
                        "Either Category records doesn't exists or category is not the leaf node, CategoryCode : "
                                + categoryCode + " ClientId : "
                                + itemService.getMyAllowedClientIds(qdo.getClientId(), qdo.getExecutionId()));
    }

    @Override
    @Cacheable(value = "CategoryKey", key = "{#categoryCode, #categoryName, #clientId, #executionId}", unless = "#result == null")
    public ULong getCategoryKey(SiSubsetDefinitionRecord definition, String categoryCode, String categoryName,
                                ULong clientId, ULong executionId) {
        if (!isBlank(categoryName) || !isBlank(categoryCode)) {
            Condition clientCondition = TMB_CLIENT_CATEGORY.CLIENT_ID
                    .in(itemService.getMyAllowedClientIds(clientId, executionId));
            Condition nameCondition = TMB_CLIENT_CATEGORY.CATEGORY_CODE.eq(categoryCode);
            Condition leafNodeCondition = TMB_CLIENT_TAXONOMY_TREE_NODE.IS_LEAF_NODE.eq(Byte.valueOf("1"));
            Condition taxonomyId = TMB_CLIENT_TAXONOMY_TREE_NODE.TAXONOMY_ID.eq(definition.getTaxonomyId());
            if (definition.getSubsetType().equals(SiSubsetDefinitionSubsetType.E)) {
                ULong publisherId = itemService.getPublisherIdForClientId(executionId, clientId);
                ULong taxId = dslContext.select(SI_PUBLISHER_TAXONOMY.TAXONOMY_ID).from(SI_PUBLISHER_TAXONOMY)
                        .where(SI_PUBLISHER_TAXONOMY.PUBLISHER_ID.eq(publisherId)
                                .and(SI_PUBLISHER_TAXONOMY.DEFAULT_TAXONOMY.eq(Byte.valueOf("1"))))
                        .limit(1).fetchOneInto(ULong.class);
                taxonomyId = TMB_CLIENT_TAXONOMY_TREE_NODE.TAXONOMY_ID.eq(taxId);
            }
            return dslContext.select(TMB_CLIENT_CATEGORY.ID).from(TMB_CLIENT_TAXONOMY_TREE_NODE)
                    .innerJoin(TMB_CLIENT_CATEGORY)
                    .on(TMB_CLIENT_TAXONOMY_TREE_NODE.CATEGORY_ID.eq(TMB_CLIENT_CATEGORY.ID))
                    .where(DSL.and(clientCondition, nameCondition, leafNodeCondition, taxonomyId)).limit(1)
                    .fetchOneInto(ULong.class);
        }
        return null;
    }
}
