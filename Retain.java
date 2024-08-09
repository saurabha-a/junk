package com.unilog.cx1.pim.commons.model.importtype;

import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.FORCE_DELETE;
import static com.unilog.prime.commons.util.BooleanUtil.convertToBoolean;
import static com.unilog.prime.jooq.tables.SiItemAsset.SI_ITEM_ASSET;

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

public class Retain extends Import {
	public Retain(Subset subset, RecordType recordType, DSLContext dslContext, boolean isPartialImport,
			UserUtil userUtil, UpdateType updateType) {
		super(subset, recordType, dslContext, isPartialImport, userUtil, updateType);
	}

    @java.lang.Override
    public void filterByImportType(Map<String, Object> newR, Map<String, Object> oldR, Map<String, Object> shadowR,
                                   final List<String> stringTypeDeletableFields, final List<String> numericTypeDeletableFields) {
        filterNullOrBlankValues(newR, oldR, shadowR);
    }

    @java.lang.Override
    public void prePersist(List<Tuple<Map<String, Object>, Map<String, Object>>> recs, Table<Record> table,
                           Condition conditionToDeleteOldRecord, PimDataObject qdo, DSLContext dslContext) {
        // only assets records can be deleted in RETAIN type import
        if(subset.isExternal() && table.equals(SI_ITEM_ASSET)) {
            super.prePersist(recs, table, conditionToDeleteOldRecord, qdo, dslContext);
            recordType.prePersist(recs, table, conditionToDeleteOldRecord, qdo, dslContext);
        }
        if(convertToBoolean(qdo.getData().get(FORCE_DELETE))) {
            try {
                recordType.prePersist(recs, table, conditionToDeleteOldRecord, qdo, dslContext);
            }
            finally {
                qdo.getData().remove(FORCE_DELETE);
            }
        }
    }
    
    @java.lang.Override
    public boolean isRetain() {
    	return true;
    }
    
    @java.lang.Override
    public void cleanUp(Map<String, Object> newR, Map<String, Object> oldR, List<Tuple<Map<String, Object>, Map<String, Object>>> tuples) {
    	tuples.add(new Tuple<>(newR, oldR));
    }
    
    @java.lang.Override
	public void deleteOldRecords(Map<String, Object> newR, Map<String, Object> oldR, Map<String, Object> shadowR,
			List<Tuple<Map<String, Object>, Map<String, Object>>> tuples, List<String> stringTypeDeletableFields,
			List<String> numericTypeDeletableFields) {

		recordType.deleteValue(newR, oldR, shadowR, stringTypeDeletableFields, numericTypeDeletableFields);
		tuples.add(new Tuple<>(newR, oldR));
	}
}
