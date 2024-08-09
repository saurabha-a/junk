package com.unilog.cx1.pim.commons.model.importtype;

import static com.google.common.collect.Maps.newHashMap;
import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.CHANGES_IN_ITEM_SUBTABLES;
import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.ITEM_IMPORT_REQUIRED_SERVICES_NAMES;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.CREATED_AT;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ITEM_ID;
import static com.unilog.cx1.pim.commons.util.JooqUtil.getItemAssetDeleteCondition;
import static com.unilog.cx1.pim.commons.util.JooqUtil.getItemAttributeDeleteCondition;
import static com.unilog.cx1.pim.commons.util.JooqUtil.getItemCategoryDeleteCondition;
import static com.unilog.cx1.pim.commons.util.JooqUtil.getItemDeleteCondition;
import static com.unilog.cx1.pim.commons.util.JooqUtil.getItemKeywordDeleteCondition;
import static com.unilog.cx1.pim.commons.util.JooqUtil.getItemPartnumberDeleteCondition;
import static com.unilog.cx1.pim.commons.util.JooqUtil.getItemProductDeleteCondition;
import static com.unilog.cx1.pim.commons.util.JooqUtil.getItemXFieldDeleteCondition;
import static com.unilog.prime.commons.util.StringUtil.safeValueOf;
import static com.unilog.prime.jooq.tables.SiClientItem.SI_CLIENT_ITEM;
import static com.unilog.prime.jooq.tables.SiItemAsset.SI_ITEM_ASSET;
import static com.unilog.prime.jooq.tables.SiItemAttributeValue.SI_ITEM_ATTRIBUTE_VALUE;
import static com.unilog.prime.jooq.tables.SiItemCategory.SI_ITEM_CATEGORY;
import static com.unilog.prime.jooq.tables.SiItemKeyword.SI_ITEM_KEYWORD;
import static com.unilog.prime.jooq.tables.SiItemPartnumber.SI_ITEM_PARTNUMBER;
import static com.unilog.prime.jooq.tables.SiItemProduct.SI_ITEM_PRODUCT;
import static com.unilog.prime.jooq.tables.SiItemProperties.SI_ITEM_PROPERTIES;
import static com.unilog.prime.jooq.tables.SiItemXFieldValue.SI_ITEM_X_FIELD_VALUE;
import static org.jooq.impl.DSL.falseCondition;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;

import com.unilog.cx1.pim.commons.enumeration.UpdateType;
import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.recordtype.RecordType;
import com.unilog.cx1.pim.commons.model.subset.Subset;
import com.unilog.cx1.pim.commons.util.UserUtil;
import com.unilog.prime.commons.model.Tuple;

public class Replace extends Import {
	public Replace(Subset subset, RecordType recordType, DSLContext dslContext, boolean isPartialImport,
			UserUtil userUtil, UpdateType updateType) {
		super(subset, recordType, dslContext, isPartialImport, userUtil, updateType);
	}

    @java.lang.Override
    public void filterRecordValues(Map<String, Object> newR, Map<String, Object> oldR, List<String> stringTypeDeletableFields, List<String> numericTypeDeletableFields, boolean isItemSubtable) {
        newR.putIfAbsent(CREATED_AT, oldR.get(CREATED_AT));
        oldR.clear();
        super.filterRecordValues(newR, newHashMap(), stringTypeDeletableFields, numericTypeDeletableFields, isItemSubtable);
    }

    @java.lang.Override
    public void filterByImportType(Map<String, Object> newR, Map<String, Object> oldR, Map<String, Object> shadowR,
                                   final List<String> stringTypeDeletableFields, final List<String> numericTypeDeletableFields) {
        filterNullOrBlankValues(newR, oldR, shadowR);
    }

    @java.lang.Override
    public void prePersist(List<Tuple<Map<String, Object>, Map<String, Object>>> recs, Table<Record> table, Condition conditionToDeleteOldRecord, PimDataObject qdo, DSLContext dslContext) {
        super.prePersist(recs, table, conditionToDeleteOldRecord, qdo, dslContext);
        deleteOldRec(table, qdo);
    }

    private void deleteOldRec(Table<Record> table, PimDataObject qdo) {
        String delimitedRequiredServices = safeValueOf(qdo.getData().get(ITEM_IMPORT_REQUIRED_SERVICES_NAMES));
        List<String> requiredServices = Arrays.asList(delimitedRequiredServices.split(","));
        String itemId = safeValueOf(qdo.getData().get(ITEM_ID));
        Condition condition = falseCondition();
        if (table.equals(SI_CLIENT_ITEM)) {
            condition = getItemDeleteCondition(requiredServices, itemId);
        } else if (table.equals(SI_ITEM_ASSET)) {
            condition = getItemAssetDeleteCondition(requiredServices, itemId);
        } else if (table.equals(SI_ITEM_CATEGORY)) {
            condition = getItemCategoryDeleteCondition(requiredServices, itemId);
        } else if (table.equals(SI_ITEM_PRODUCT)) {
            condition = getItemProductDeleteCondition(requiredServices, itemId);
        } else if (table.equals(SI_ITEM_ATTRIBUTE_VALUE)) {
            condition = getItemAttributeDeleteCondition(requiredServices, itemId);
        } else if (table.equals(SI_ITEM_PARTNUMBER)) {
            condition = getItemPartnumberDeleteCondition(requiredServices, itemId);
        }else if (table.equals(SI_ITEM_KEYWORD)) {
            condition = getItemKeywordDeleteCondition(requiredServices, itemId);
        } else if (table.equals(SI_ITEM_PROPERTIES)) {
            condition = null;
        } else if (table.equals(SI_ITEM_X_FIELD_VALUE)) {
            condition = getItemXFieldDeleteCondition(requiredServices, itemId);
        } else {
            throw new IllegalArgumentException("Not sure how to REPLACE records in table : " + table);
        }
        if(condition != null) {
            int count = dslContext.deleteFrom(table).where(condition).execute();
            if (count > 0)
                qdo.getData().put(CHANGES_IN_ITEM_SUBTABLES, true);
        }
    }

    public boolean isReplace() {
        return true;
    }
    
	@java.lang.Override
	public void deleteOldRecords(Map<String, Object> newR, Map<String, Object> oldR, Map<String, Object> shadowR,
			List<Tuple<Map<String, Object>, Map<String, Object>>> tuples, List<String> stringTypeDeletableFields,
			List<String> numericTypeDeletableFields) {

	}
}
