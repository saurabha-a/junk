package com.unilog.cx1.pim.commons.service.impl;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newTreeMap;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ALTERNATE_PART_NUMBER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.DISCONTINUED_MPN;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ITEM_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PARTNUMBER_TYPE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER_HASH;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PUBLISHER_ID;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryTableType.PARTNUMBER;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryViewType.MERGE;
import static com.unilog.prime.commons.util.IDUtil.createHash;
import static com.unilog.prime.commons.util.StringUtil.safeValueOf;
import static com.unilog.prime.jooq.tables.SiItemPartnumber.SI_ITEM_PARTNUMBER;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.jooq.impl.DSL.field;
import static org.springframework.transaction.annotation.Isolation.READ_COMMITTED;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.Validate;
import org.jooq.Condition;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;
import com.unilog.cx1.pim.commons.service.IPartnumberService;
import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.jooq.Tables;
import com.unilog.prime.jooq.tables.records.SiItemPartnumberRecord;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;

@Service(PartnumberServiceImpl.BEAN_ID)
public class PartnumberServiceImpl extends AbstractImportService implements IPartnumberService {

    public static final String BEAN_ID = "etlPartnumberService";
    public static final String ALT_PARTNUMBER_1_NAME = "Alternate Part Number 1";
    public static final String ALT_PARTNUMBER_2_NAME = "Alternate Part Number 2";
    public static final String DISCONTINUED_MPN_NAME = "Discontinued Mpn";

    @Autowired
    private ItemServiceImpl itemService;

    @Override
    @Transactional(isolation = READ_COMMITTED)
    public void doImport(Import anImport, PimDataObject qdo) {
        Map<?, SiItemPartnumberRecord> allExistingPartnumber = getParentRecords(anImport, qdo);
        if (allExistingPartnumber == null)
            allExistingPartnumber = Maps.newHashMap();
        Tuple<List<Tuple<Map<String, Object>, Map<String, Object>>>, Condition> tuple = getTuplesToInsert(anImport, qdo, allExistingPartnumber);
        List<Tuple<Map<String, Object>, Map<String, Object>>> recs = tuple.getFirstValue();
        Condition conditionToDeleteOldRecord = tuple.getSecondValue();
        super.doImportInItemSubTable(anImport, qdo, new Tuple(recs, conditionToDeleteOldRecord), anImport.subset.getPartNumberInsertTable(),
                anImport.subset.getPartnumberInsertColumns(), newArrayList(PART_NUMBER), newArrayList());
    }

    private Tuple<List<Tuple<Map<String, Object>, Map<String, Object>>>, Condition> getTuplesToInsert(Import anImport, PimDataObject qdo, Map<?, SiItemPartnumberRecord> allExistingPartnumbers) {
        List<Tuple> tuples = Lists.newArrayList();
        List<Condition> conditions = Lists.newArrayList();
        String itemId = qdo.getValue(ITEM_ID);

        String pnType;
        String id;
        Condition conditionToDeleteOldRecord;
        if (qdo.contains(ALTERNATE_PART_NUMBER + 1)) {
            pnType = partnumberService.getPartnumberTypes().get(ALT_PARTNUMBER_1_NAME);
            id = createHash(itemId, pnType);
            Tuple<Map<String, Object>, Map<String, Object>> apn1Tuple = getATuple(anImport, qdo, ALTERNATE_PART_NUMBER + 1, pnType, id, allExistingPartnumbers.get(id));
            tuples.add(apn1Tuple);
            conditionToDeleteOldRecord = getConditionToDeleteOldRecord(apn1Tuple.getFirstValue(),
                    apn1Tuple.getSecondValue(), qdo, anImport, pnType);
            if (conditionToDeleteOldRecord != null)
                conditions.add(conditionToDeleteOldRecord);
        }

        if (qdo.contains(ALTERNATE_PART_NUMBER + 2)) {
            pnType = partnumberService.getPartnumberTypes().get(ALT_PARTNUMBER_2_NAME);
            id = createHash(itemId, pnType);
            Tuple<Map<String, Object>, Map<String, Object>> apn2Tuple = getATuple(anImport, qdo, ALTERNATE_PART_NUMBER + 2, pnType, id, allExistingPartnumbers.get(id));
            tuples.add(apn2Tuple);
            conditionToDeleteOldRecord = getConditionToDeleteOldRecord(apn2Tuple.getFirstValue(),
                    apn2Tuple.getSecondValue(), qdo, anImport, pnType);
            if (conditionToDeleteOldRecord != null)
                conditions.add(conditionToDeleteOldRecord);
        }

        if (qdo.contains(DISCONTINUED_MPN)) {
            pnType = partnumberService.getPartnumberTypes().get(DISCONTINUED_MPN_NAME);
            id = createHash(itemId, pnType);
            Tuple<Map<String, Object>, Map<String, Object>> dpnTuple = getATuple(anImport, qdo, DISCONTINUED_MPN, pnType, id, allExistingPartnumbers.get(id));
            tuples.add(dpnTuple);
            conditionToDeleteOldRecord = getConditionToDeleteOldRecord(dpnTuple.getFirstValue(),
                    dpnTuple.getSecondValue(), qdo, anImport, pnType);
        if (conditionToDeleteOldRecord != null)
                conditions.add(conditionToDeleteOldRecord);
        }

        Condition finalCondition = null;
        if (!conditions.isEmpty())
            finalCondition = DSL.or(conditions);
        return new Tuple(tuples, finalCondition);
    }

    private Tuple getATuple(Import anImport, PimDataObject qdo, String pn,
                            String pnType, String id, SiItemPartnumberRecord existingRecord) {
        Map<String, Object> oldR = existingRecord == null ? newTreeMap() : existingRecord.intoMap();
        Map<String, Object> newR = getNewImportRec(qdo.getValue(pn), pnType, qdo, id, anImport, oldR);
        Tuple<Map<String, Object>, Map<String, Object>> recTuple = new Tuple(newR, oldR);
        return recTuple;
    }

    private Condition getConditionToDeleteOldRecord(Map<String, Object> newR, Map<String, Object> oldR, PimDataObject qdo,
                                                    Import anImport, String pnType) {
        String pn = safeValueOf(newR.get(PART_NUMBER));
        List<Condition> list = newArrayList();
        if (isNotBlank(pnType)) {
            if (isBlank(pn) && anImport.canHonorNullValue()) {
                Condition c1 = field(ITEM_ID).eq(qdo.getValue(ITEM_ID));
                c1 = c1.and(field(PARTNUMBER_TYPE).eq(pnType));
                c1 = c1.and(field(PART_NUMBER).notEqual(""));
                if (anImport.subset.isExternal())
                    c1 = c1.and(field(PUBLISHER_ID).eq(qdo.getValue(PUBLISHER_ID)));
                list.add(c1);
            }
            if (oldR != null && pn != null &&
                    ObjectUtils.equals(oldR.get(PART_NUMBER), pn) && anImport.subset.isWorkspace())
                list.add(field(ID).eq(oldR.get(ID)));

        }
        return list.isEmpty() ? null : DSL.or(list);
    }

    private Map<String, Object> getNewImportRec(String pn, String pnType, PimDataObject qdo, String id,
                                                Import anImport, Map<String, Object> oldR) {
        String itemId = qdo.getValue(ITEM_ID);
        Map<String, Object> recMap = Maps.newHashMap();
        recMap.put(ID, id);
        recMap.put(ITEM_ID, itemId);
        recMap.put(PART_NUMBER_HASH, qdo.getValue(PART_NUMBER_HASH));
        recMap.put(PARTNUMBER_TYPE, pnType);
		if (anImport.isRetain()) {
			pn = StringUtils.equals((String) oldR.get(PART_NUMBER), pn) ? null : pn;
		}
		recMap.put(PART_NUMBER, pn);
		return recMap;
    }

    private Map<?, SiItemPartnumberRecord> getParentRecords(Import anImport, PimDataObject qdo) {
        String itemId = qdo.getValue(ITEM_ID);
        if (anImport.subset.isExternal()) {
            ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
            return dslContext.selectFrom(SI_ITEM_PARTNUMBER).where(SI_ITEM_PARTNUMBER.ITEM_ID
                    .eq(itemId).and(SI_ITEM_PARTNUMBER.PUBLISHER_ID.eq(publisherId))).fetch().intoMap(ID);
        } else {
            SiSubsetDefinitionRecord catalogSubset = itemService.getSubsetDefinition(qdo.getExecutionId(), anImport.subset.definition.getParentId());
            Map<?, SiItemPartnumberRecord> catalogRecords = dslContext.fetch(subsetService.getQuery(qdo.getExecutionId(), catalogSubset, PARTNUMBER, MERGE), itemId).intoMap(ID, SiItemPartnumberRecord.class);
            if (anImport.subset.isContentWorkspace()) {
                if (catalogRecords == null || catalogRecords.isEmpty()) {
                    ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
                    Validate.notNull(publisherId, "Something is wrong, you need to be publisher to execute the operation");
                    return dslContext.selectFrom(SI_ITEM_PARTNUMBER).where(SI_ITEM_PARTNUMBER.ITEM_ID
                            .eq(itemId).and(SI_ITEM_PARTNUMBER.PUBLISHER_ID.eq(publisherId))).fetch().intoMap(ID);
                }
            }
            return catalogRecords;
        }
    }

    @Override
    @Cacheable(value = "PartnumberTypes", key = "#root.methodName")
    public Map<String, String> getPartnumberTypes() {
        Map<String, String> codes = dslContext.selectFrom(Tables.SI_LU_PARTNUMBER_TYPE)
                .fetchMap(Tables.SI_LU_PARTNUMBER_TYPE.PARTNUMBER_TYPE, Tables.SI_LU_PARTNUMBER_TYPE.CODE);
        return codes;
    }
}
