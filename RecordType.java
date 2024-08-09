package com.unilog.cx1.pim.commons.model.recordtype;

import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.CHANGES_IN_ITEM_SUBTABLES;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ATTRIBUTE_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.CATEGORY_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.KEYWORD;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PRODUCT_ID;
import static com.unilog.cx1.pim.commons.model.importtype.Import.IMMUTABLE_TABLE_FIELDS;
import static com.unilog.prime.commons.util.StringUtil.safeValueOf;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.jooq.impl.DSL.falseCondition;

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.unilog.cx1.pim.commons.model.importtype.Import;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Table;

import com.google.common.collect.ImmutableSet;
import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.prime.commons.model.Tuple;

public abstract class RecordType {

    public static final Set<String> ITEM_SUBTABLE_NULLABLE_COLUMNS = ImmutableSet.of(CATEGORY_ID, ATTRIBUTE_ID, PRODUCT_ID, KEYWORD, PART_NUMBER);
    protected Import anImport;

    public abstract void deleteValue(Map<String, Object> newR, Map<String, Object> oldR,
                                     Map<String, Object> shadowR, List<String> stringTypeDeletableFields, List<String> numericTypeDeletableFields);

    public void prePersist(List<Tuple<Map<String, Object>, Map<String, Object>>> recs, Table<Record> table,
                           Condition conditionToDeleteOldRecord, PimDataObject pdo, DSLContext dslContext) {
        if (conditionToDeleteOldRecord == null || conditionToDeleteOldRecord.equals(falseCondition()))
            return;
        int count = dslContext.deleteFrom(table).where(conditionToDeleteOldRecord).execute();
        if (count > 0)
            pdo.getData().put(CHANGES_IN_ITEM_SUBTABLES, true);
    }

    public boolean canHonorNullValue() {
        return false;
    }

    public void removeNullValue(Map<String, Object> newR, List<String> stringTypeDeletableFields, List<String> numericTypeDeletableFields) {
        if (newR != null) {
            newR.entrySet().removeIf(entry -> {
                boolean isSubTableNullableColumn = entry.getValue() == null && ITEM_SUBTABLE_NULLABLE_COLUMNS.contains(entry.getKey());
                boolean isColumnDeletable = !(stringTypeDeletableFields.contains(entry.getKey())
                        || numericTypeDeletableFields.contains(entry.getKey())
                        || IMMUTABLE_TABLE_FIELDS.contains(entry.getKey()));
                boolean isBlank = isBlank(safeValueOf(entry.getValue()));
                return (isSubTableNullableColumn || isColumnDeletable ) && isBlank;
            });
        }
    }

    public void setImport(Import anImport) {
        this.anImport = anImport;
    }

    public abstract boolean isPrivate();

    public boolean isPublisherNonSubscribedRecord() {
        return false;
    }

    public boolean isCatalogNonSubscribedRecord() {
        return false;
    }
}
