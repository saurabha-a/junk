package com.unilog.cx1.pim.commons.service.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.unilog.cx1.pim.commons.constants.ItemHeaderConstants;
import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;
import com.unilog.cx1.pim.commons.service.IItemService;
import com.unilog.cx1.pim.commons.service.IKeywordService;
import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.jooq.tables.records.SiItemKeywordRecord;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.Validate;
import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newTreeMap;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.*;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryTableType.KEYWORD;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryViewType.MERGE;
import static com.unilog.prime.commons.util.IDUtil.createHash;
import static com.unilog.prime.jooq.Tables.SI_ITEM_KEYWORD;
import static com.unilog.prime.jooq.Tables.SI_LU_KEYWORD_TYPE;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.or;
import static org.springframework.transaction.annotation.Isolation.READ_COMMITTED;

@Service(KeywordServiceImpl.BEAN_ID)
public class KeywordServiceImpl extends AbstractImportService implements IKeywordService {

    public static final String BEAN_ID = "etlKeywordService";

    @Autowired
    private IItemService itemService;

    @Override
    @Cacheable(value = "KeywordTypes", key = "#root.methodName")
    public Map<String, String> getKeywordTypeMap() {
        return dslContext.selectFrom(SI_LU_KEYWORD_TYPE)
                .fetchMap(SI_LU_KEYWORD_TYPE.KEYWORD_TYPE, SI_LU_KEYWORD_TYPE.CODE);
    }

    @Override
    @Transactional(isolation = READ_COMMITTED)
    public void doImport(Import anImport, PimDataObject qdo) {
        Map<?, SiItemKeywordRecord> existingRecord = getParentRecords(anImport, qdo);
        Tuple<List<Tuple<Map<String, Object>, Map<String, Object>>>, Condition> tuplesToInsert = getTuplesToInsert(anImport, qdo, existingRecord);
        super.doImportInItemSubTable(anImport, qdo, tuplesToInsert, anImport.subset.getKeywordInsertTable(),
                anImport.subset.getKeywordInsertColumns(), newArrayList(ItemHeaderConstants.KEYWORD), newArrayList());
    }

    private Tuple<List<Tuple<Map<String, Object>, Map<String, Object>>>, Condition> getTuplesToInsert(Import anImport, PimDataObject qdo, Map<?, SiItemKeywordRecord> allExistingRecords) {
        List<Tuple> tuples = Lists.newArrayList();
        List<Condition> conditions = Lists.newArrayList();

        if (qdo.contains(KEYWORDS)) {
            String keywordVal = qdo.getValue(KEYWORDS);
            String keywordType = keywordService.getKeywordTypeMap().get("Custom keywords");
            createTuple(anImport, qdo, allExistingRecords, tuples, conditions, keywordVal, keywordType);
        }
        if (qdo.contains(PART_NUMBER_KEYWORDS)) {
            String keywordVal = qdo.getValue(PART_NUMBER_KEYWORDS);
            String keywordType = keywordService.getKeywordTypeMap().get("Partnumber keywords");
            createTuple(anImport, qdo, allExistingRecords, tuples, conditions, keywordVal, keywordType);
        }
        if (qdo.contains(CUSTOM_KEYWORDS)) {
            String keywordVal = qdo.getValue(CUSTOM_KEYWORDS);
            String keywordType = keywordService.getKeywordTypeMap().get("Subscriber Custom Keywords");
            createTuple(anImport, qdo, allExistingRecords, tuples, conditions, keywordVal, keywordType);
        }

        Condition finalCondition = null;
        if (!conditions.isEmpty())
            finalCondition = or(conditions);
        return new Tuple(tuples, finalCondition);
    }

    private void createTuple(Import anImport, PimDataObject qdo, Map<?, SiItemKeywordRecord> allExistingRecords, List<Tuple> tuples, List<Condition> conditions, String keywordVal, String keywordType) {
        SiItemKeywordRecord existingRecord = allExistingRecords.get(keywordType);
        Map<String, Object> oldR = existingRecord == null ? newTreeMap() : existingRecord.intoMap();
        Map<String, Object> newR = getNewImportRec(qdo, existingRecord, anImport, keywordVal, keywordType);
        tuples.add(new Tuple<>(newR, oldR));
        Condition conditionToDeleteOldRecord = getConditionToDeleteOldRecord(newR, oldR, qdo, anImport, keywordVal, keywordType);
        if (conditionToDeleteOldRecord != null)
            conditions.add(conditionToDeleteOldRecord);
    }

    private Condition getConditionToDeleteOldRecord(Map<String, Object> newR, Map<String, Object> oldR,
                                                    PimDataObject qdo, Import anImport, String keywordVal, String keywordType) {
        List<Condition> list = newArrayList();
        if (isBlank(keywordVal) && anImport.canHonorNullValue()) {
            list.add(field(ITEM_ID).eq(qdo.getData().get(ITEM_ID))
                    .and(field(KEYWORD_TYPE).eq(keywordType)).and(field(ItemHeaderConstants.KEYWORD).notEqual("")));
        }
        if (oldR != null && keywordVal != null &&
                ObjectUtils.equals(oldR.get(ItemHeaderConstants.KEYWORD), keywordVal) && anImport.subset.isWorkspace())
            list.add(field(ID).eq(oldR.get(ID)));
        return list.isEmpty() ? null : DSL.or(list);
    }

    private Map<?, SiItemKeywordRecord> getParentRecords(Import anImport, PimDataObject qdo) {
        String itemId = qdo.getData().get(ITEM_ID).toString();
        if (anImport.subset.isExternal()) {
            ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
            return dslContext.selectFrom(SI_ITEM_KEYWORD).where(SI_ITEM_KEYWORD.ITEM_ID
                    .eq(itemId).and(SI_ITEM_KEYWORD.PUBLISHER_ID.eq(publisherId))).fetch().intoMap(KEYWORD_TYPE);
        } else {
            SiSubsetDefinitionRecord catalogSubset = itemService.getSubsetDefinition(qdo.getExecutionId(), anImport.subset.definition.getParentId());
            Map<?, SiItemKeywordRecord> catalogRecord = dslContext.fetch(subsetService.getQuery(qdo.getExecutionId(), catalogSubset, KEYWORD, MERGE), itemId)
                    .intoMap(KEYWORD_TYPE, SiItemKeywordRecord.class);
            if (anImport.subset.isContentWorkspace()) {
                if (catalogRecord == null || catalogRecord.isEmpty()) {
                    ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
                    Validate.notNull(publisherId, "Something is wrong, you need to be publisher to execute the operation");
                    return dslContext.selectFrom(SI_ITEM_KEYWORD).where(SI_ITEM_KEYWORD.ITEM_ID
                            .eq(itemId).and(SI_ITEM_KEYWORD.PUBLISHER_ID.eq(publisherId))).fetch().intoMap(KEYWORD_TYPE);
                }
            }
            return catalogRecord;
        }
    }

    private Map<String, Object> getNewImportRec(PimDataObject qdo, Record existingRecord, Import anImport, String
            keywordVal, String keywordType) {
        Map<String, Object> newRecMap = Maps.newHashMap();
        String itemId = qdo.getValue(ITEM_ID);
        String id = createHash(itemId, keywordType);
        newRecMap.put(ID, id);
        newRecMap.put(ITEM_ID, itemId);
        newRecMap.put(PART_NUMBER_HASH, qdo.getValue(PART_NUMBER_HASH));
        newRecMap.put(KEYWORD_TYPE, keywordType);
        newRecMap.put(ItemHeaderConstants.KEYWORD, keywordVal);
        return newRecMap;
    }
}

