package com.unilog.cx1.pim.commons.model.importtype;

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

public class Override extends Import {
	public Override(Subset subset, RecordType recordType, DSLContext dslContext, boolean isPartialImport,
			UserUtil userUtil, UpdateType updateType) {
		super(subset, recordType, dslContext, isPartialImport, userUtil, updateType);
	}

	public Override(Subset subset, RecordType recordType, DSLContext dslContext, boolean isPartialImport,
			boolean updateThroughApi, UserUtil userUtil, UpdateType updateType) {
		super(subset, recordType, dslContext, isPartialImport, updateThroughApi, userUtil, updateType);
	}

    public boolean canHonorNullValue() {
        return true;
    }

    @java.lang.Override
    public void filterByImportType(Map<String, Object> newR, Map<String, Object> oldR, Map<String, Object> shadowR,
                                   final List<String> stringTypeDeletableFields, final List<String> numericTypeDeletableFields) {
        if(oldR.isEmpty() && !recordType.isPrivate()) {
            filterNullOrBlankValues(newR, oldR, shadowR);
        }
        recordType.deleteValue(newR, oldR, shadowR, stringTypeDeletableFields, numericTypeDeletableFields);
    }

    @java.lang.Override
    public void prePersist(List<Tuple<Map<String, Object>, Map<String, Object>>> recs, Table<Record> table,
                           Condition conditionToDeleteOldRecord, PimDataObject qdo, DSLContext dslContext) {
        super.prePersist(recs, table, conditionToDeleteOldRecord, qdo, dslContext);
        recordType.prePersist(recs, table, conditionToDeleteOldRecord, qdo, dslContext);
    }
    
	@java.lang.Override
	public void deleteOldRecords(Map<String, Object> newR, Map<String, Object> oldR, Map<String, Object> shadowR,
			List<Tuple<Map<String, Object>, Map<String, Object>>> tuples, List<String> stringTypeDeletableFields,
			List<String> numericTypeDeletableFields) {

		recordType.deleteValue(newR, oldR, shadowR, stringTypeDeletableFields, numericTypeDeletableFields);
		tuples.add(new Tuple<>(newR, oldR));
	}
}
