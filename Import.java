package com.unilog.cx1.pim.commons.model.importtype;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.unilog.cx1.pim.commons.constants.Cx1PimConstants;
import com.unilog.cx1.pim.commons.enumeration.UpdateType;
import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.recordtype.RecordType;
import com.unilog.cx1.pim.commons.model.subset.Subset;
import com.unilog.cx1.pim.commons.util.UserUtil;
import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;
import lombok.Data;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang3.StringUtils;
import org.jooq.Record;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Sets.newHashSet;
import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.KEEP_BLANK_VALUE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.*;
import static com.unilog.cx1.pim.commons.enumeration.UpdateType.*;
import static com.unilog.prime.commons.util.BooleanUtil.convertToBoolean;
import static com.unilog.prime.commons.util.StringUtil.safeValueOf;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.commons.collections.CollectionUtils.isSubCollection;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.jooq.impl.DSL.field;
import static org.jooq.util.mysql.MySQLDSL.values;

@Data
public abstract class Import {

    protected final Logger logger;
    public final Subset subset;
    public final RecordType recordType;
    public final UpdateType updateType;
    public final DSLContext dslContext;
    public static final Set<String> IMMUTABLE_TABLE_FIELDS = ImmutableSet.of(ID, PART_NUMBER_HASH, PART_NUMBER, CLIENT_ID,
            PART_NUMBER_PREFIX, PART_NUMBER_SUFFIX, CREATED_BY, PUBLISHER_ID, CREATED_AT, ITEM_ID, KEYWORD_TYPE, ASSET_TYPE,
            PARTNUMBER_TYPE, RECORD_STATUS, LOCATION_HASH);
    private boolean isPartialImport;
    private boolean updateThroughApi;
    private UserUtil userUtil;

    public Import(Subset subset, RecordType recordType, DSLContext dslContext, Boolean isPartialImport,
                  UserUtil userUtil, UpdateType updateType) {
        this.subset = subset;
        this.recordType = recordType;
        this.dslContext = dslContext;
        this.isPartialImport = isPartialImport;
        this.updateType = updateType;
        recordType.setImport(this);
        this.userUtil = userUtil;
        logger = LoggerFactory.getLogger(this.getClass());
    }

    public Import(Subset subset, RecordType recordType, DSLContext dslContext, Boolean isPartialImport,
                  boolean updateThroughApi, UserUtil userUtil, UpdateType updateType) {
        this(subset, recordType, dslContext, isPartialImport, userUtil, updateType);
        this.updateThroughApi = updateThroughApi;
    }

    public boolean canHonorNullValue() {
        return false;
    }

    public static final Import getImport(UpdateType updateType, RecordType recordType,
                                         SiSubsetDefinitionRecord definition, DSLContext dslContextA, boolean isPartialImport, UserUtil userUtil) {
        Subset subset = Subset.getSubset(definition);
        Import importObj = null;
        if (updateType.equals(REPLACE_OLD_DATA))
            importObj = new Replace(subset, recordType, dslContextA, isPartialImport, userUtil, updateType);
        else if (updateType.equals(OVER_WRITE_WITH_ALL_AND_BLANK_VALUE) || updateType.equals(OVER_WRITE_ALL_VALUES))
            importObj = new Override(subset, recordType, dslContextA, isPartialImport, userUtil, updateType);
        else if (updateType.equals(RETAIN_OLD_DATA))
            importObj = new Retain(subset, recordType, dslContextA, isPartialImport, userUtil, updateType);
        recordType.setImport(importObj);
        return importObj;
    }

    public static final Import getImport(UpdateType updateType, RecordType recordType,
                                         SiSubsetDefinitionRecord definition, DSLContext dslContextA,
                                         boolean isPartialImport, boolean isUpdateThroughApi, UserUtil userUtil) {
        Import anImport = getImport(updateType, recordType, definition, dslContextA, isPartialImport, userUtil);
        anImport.updateThroughApi = isUpdateThroughApi;
        return anImport;
    }

    public static final void filterNullOrBlankValues(Map<String, Object> newR, Map<String, Object> oldR, Map<String, Object> shadowR) {
        if (newR != null) {
            newR.entrySet().removeIf(entry -> !convertToBoolean(newR.get(KEEP_BLANK_VALUE))
                    && isBlank(safeValueOf(entry.getValue())) && shadowR.get(entry.getKey()) == null);
        }
    }

    public abstract void filterByImportType(final Map<String, Object> newR, final Map<String, Object> oldR, final Map<String, Object> shadowR,
                                            final List<String> stringTypeDeletableFields, final List<String> numericTypeDeletableFields);

    public void filterRecordValues(final Map<String, Object> newR, final Map<String, Object> oldR,
                                   final List<String> stringTypeDeletableFields, final List<String> numericTypeDeletableFields, boolean isItemSubTable) {
        if (newR == null || newR.isEmpty())
            return;
        Map<String, Object> shadowR = Map.of();
        filterEqualValues(newR, oldR, shadowR, isItemSubTable, recordType.isPrivate());
        filterByImportType(newR, oldR, shadowR, stringTypeDeletableFields, numericTypeDeletableFields);
    }

    @SuppressWarnings("unused")
    private boolean checkNumberEquality(String z1, String z2) {
        try {
            return new BigDecimal(z1).compareTo(new BigDecimal(z2)) == 0;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public void persistToDb(List<Tuple<Map<String, Object>, Map<String, Object>>> recs,
                            Condition conditionToDeleteOldRecord, Table<Record> table, PimDataObject qdo) {
        if (recs != null && !recs.isEmpty()) {
            Tuple<Map<String, Object>, Map<String, Object>> firstRec = recs.get(0);
            Set<String> tableColumns = firstRec.getFirstValue().keySet();
            persistToDb(recs, conditionToDeleteOldRecord, table, tableColumns, qdo, false);
        }
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void persistToDb(List<Tuple<Map<String, Object>, Map<String, Object>>> recs,
                            Condition conditionToDeleteOldRecord, Table<Record> table,
                            Set<String> tableColumns, PimDataObject qdo, boolean isItemSubTable) {
        persistToDb(recs, conditionToDeleteOldRecord, table, tableColumns, qdo, isItemSubTable, new HashSet());
    }

    public void persistToDb(List<Tuple<Map<String, Object>, Map<String, Object>>> recs,
                            Condition conditionToDeleteOldRecord, Table<Record> table,
                            Set<String> tableColumns, PimDataObject qdo, boolean isItemSubTable, Set<String> notNullColumns) {
        prePersist(recs, table, conditionToDeleteOldRecord, qdo, dslContext);
        Set<String> ignorableFields = newHashSet(IMMUTABLE_TABLE_FIELDS);
        ignorableFields.removeIf(r -> isItemSubTable && r.equals(PART_NUMBER));
        recs.removeIf(r -> isSubCollection(r.getFirstValue().keySet(), ignorableFields));
        if (isNotEmpty(recs)) {
            if (isNotEmpty(tableColumns)) {
                List<Field<Object>> insertFields = tableColumns.stream().map(DSL::field).collect(Collectors.toList());
                List<String> subsetSpecificItemColumns = subset.getSubsetSpecificItemInsertColumns();
                recs.forEach(r -> subsetSpecificItemColumns.forEach(s -> {
                    Map<String, Object> firstValue = r.getFirstValue();
                    if (isNotBlank(safeValueOf(firstValue.get(s)))) {
                        return;
                    }
                    Validate.notNull(qdo.getData().get(s), s + " value is required");
                    firstValue.put(s, qdo.getData().get(s));
                }));
                subsetSpecificItemColumns.forEach(r -> insertFields.add(field(r)));

                notNullColumns.forEach(s -> {
                    recs.stream().forEach(t -> {
                        if (t.getSecondValue().containsKey(s) && t.getFirstValue().get(s) == null) {
                            t.getFirstValue().put(s, t.getSecondValue().get(s));
                        }
                    });
                });

                InsertValuesStepN<Record> insert = this.dslContext.insertInto(table).columns(insertFields);
                recs.stream().forEach(rec -> insert.values(insertFields.stream().map(Field::getName)
                        .map(r -> {
                            Object o = rec.getFirstValue().get(r);
                            if (o == null && r.equalsIgnoreCase(RECORD_STATUS))
                                return "C";
                            else return o;
                        }).collect(Collectors.toList())));

                Map<Field<Object>, Field<Object>> maps = newHashMap();
                insertFields.stream().filter(col -> !ignorableFields.contains(col.getName())).forEach(col -> maps.put(col, values(col)));

                int count = insert.onDuplicateKeyUpdate().set(maps).execute();
                if (count > 0)
                    qdo.getData().put(Cx1PimConstants.CHANGES_IN_ITEM_SUBTABLES, true);
            }
        } else {
            logger.info("No value changes in table : {}", table);
        }
    }

    public void persistToGlobalTable(List<Tuple<Map<String, Object>, Map<String, Object>>> recs,
                                     Condition conditionToDeleteOldRecord, Table<Record> table, PimDataObject qdo) {
        prePersist(recs, table, conditionToDeleteOldRecord, qdo, dslContext);
        if (recs != null && !recs.isEmpty() && !recs.get(0).getFirstValue().isEmpty()) {
            List<Field<Object>> insertFields = recs.get(0).getFirstValue().keySet().stream().map(DSL::field).collect(Collectors.toList());

            InsertValuesStepN<Record> insert = this.dslContext.insertInto(table).columns(insertFields);
            recs.stream().filter(rec -> !isSubCollection(rec.getFirstValue().keySet(), IMMUTABLE_TABLE_FIELDS)).forEach(rec -> insert
                    .values(insertFields.stream().map(Field::getName).map(r -> rec.getFirstValue().get(r)).collect(Collectors.toList())));
            Map<Field<Object>, Field<Object>> maps = newHashMap();
            insertFields.stream().filter(col -> !IMMUTABLE_TABLE_FIELDS.contains(col.getName())).forEach(col -> maps.put(col, values(col)));
            int count = insert.onDuplicateKeyUpdate().set(maps).execute();
            if (count > 0)
                qdo.getData().put(Cx1PimConstants.CHANGES_IN_ITEM_GLOBAL_TABLE, true);
        }
    }

    public void prePersist(List<Tuple<Map<String, Object>, Map<String, Object>>> recs, Table<Record> table,
                           Condition conditionToDeleteOldRecord, PimDataObject qdo, DSLContext dslContext) {

    }

    public final void filterRecordValues(final Map<String, Object> newR, final Map<String, Object> oldR) {
        filterRecordValues(newR, oldR, ImmutableList.of(), ImmutableList.of(), false);
    }

    public final void filterEqualValues(Map<String, Object> newR, Map<String, Object> oldR, Map<String, Object> shadowR,
                                        boolean isItemSubTable, boolean ignoreNullEqualValue) {
        if (oldR == null || newR == null)
            return;
        Map<String, Object> newRU = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        newRU.putAll(newR);
        Map<String, Object> oldRU = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        oldRU.putAll(oldR);

        Set<String> immutableFields = newHashSet(IMMUTABLE_TABLE_FIELDS);
        if (isItemSubTable)
            immutableFields.remove(PART_NUMBER);

        newRU.entrySet().removeIf(k -> {
            String key = k.getKey();
            if (!oldRU.containsKey(key) || immutableFields.contains(key))
                return false;
            if (ignoreNullEqualValue && oldRU.get(key) == null && newRU.get(key) == null)
                return false;
            String oldFieldVal = safeValueOf(oldRU.get(key), "").trim();
            String newFieldVal = safeValueOf(newRU.get(key), "").trim();
            if (StringUtils.equals(oldFieldVal, newFieldVal)) {
                if (shadowR.get(key) != null) {
                    newRU.put(key, null);
                    return false;
                }
                return true;
            }
            return false;
        });
        newR.clear();
        newR.putAll(newRU);
    }

    @SuppressWarnings("unused")
    private boolean isFloat(String oldFieldVal) {
        return oldFieldVal.matches("[-+]?[0-9]*\\.[0-9]+");
    }

    public void filterRecordValues(List<Tuple<Map<String, Object>, Map<String, Object>>> recs,
                                   List<String> stringTypeDeletableFields, List<String> numericTypeDeletableFields) {
        if (isNotEmpty(recs)) {
            recs.forEach(r -> {
                Map<String, Object> newR = r.getFirstValue();
                Map<String, Object> oldR = r.getSecondValue();
                filterRecordValues(newR, oldR, stringTypeDeletableFields, numericTypeDeletableFields, canHonorNullValue());
            });
            recs.removeIf(r -> r.getFirstValue().isEmpty());
        }
    }

    public boolean isPartialImport() {
        return isPartialImport;
    }

    public boolean isUpdateThroughApi() {
        return updateThroughApi;
    }

    public boolean isImportByPublisher() {
        return userUtil.isPublisher();
    }

    public UserUtil getUserUtil() {
        return userUtil;
    }

    public boolean isReplace() {
        return false;
    }

    public void filterRecordValues(Map<String, Object> newR, Map<String, Object> parentR, Map<String, Object> shadowR, List<String> stringTypeDeletableFields,
                                   List<String> numericTypeDeletableFields, boolean isItemSubTable) {
        if (newR == null || newR.isEmpty())
            return;
        filterEqualValues(newR, parentR, shadowR, isItemSubTable, recordType.isPrivate());
        filterByImportType(newR, parentR, shadowR, stringTypeDeletableFields, numericTypeDeletableFields);
    }

    public boolean isRetain() {
        return false;
    }

    public void deleteOldRecords(Map<String, Object> newR, Map<String, Object> oldR, Map<String, Object> shadowR,
                                 List<Tuple<Map<String, Object>, Map<String, Object>>> tuples, List<String> stringTypeDeletableFields,
                                 List<String> numericTypeDeletableFields) {

    }

    public void cleanUp(Map<String, Object> newR, Map<String, Object> oldR,
                        List<Tuple<Map<String, Object>, Map<String, Object>>> tuples) {

    }

}
