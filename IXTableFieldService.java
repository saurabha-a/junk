package com.unilog.cx1.pim.commons.service;

import java.util.List;
import java.util.Map;

import org.jooq.Record;
import org.jooq.types.ULong;

import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;
import com.unilog.prime.jooq.tables.records.SiClientXFieldRecord;
import com.unilog.prime.jooq.tables.records.SiItemXFieldValueRecord;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;

public interface IXTableFieldService {
    void doImport(Import anImport, PimDataObject qdo);
    Record getXField(String atrName, ULong clientId, ULong executionId);

    Map<?, SiItemXFieldValueRecord> getAllExistingRecords(ULong publisherId, ULong executionId, String itemId, SiSubsetDefinitionRecord subsetDefinitionRecord);

    List<SiClientXFieldRecord> getPublisherXFieldRecords(List<ULong> ids);
}
