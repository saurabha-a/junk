package com.unilog.cx1.pim.commons.service;

import com.unilog.prime.jooq.tables.records.SiItemAttributeValueRecord;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;
import com.unilog.prime.jooq.tables.records.TmbClientAttributeRecord;
import org.jooq.types.ULong;

import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;

import java.util.List;
import java.util.Map;

public interface IAttributeService {

    void doImport(Import anImport, PimDataObject qdo);

    Map<?, SiItemAttributeValueRecord> getAllExistingAttributes(ULong publisherId, ULong executionId, String itemId, SiSubsetDefinitionRecord subsetDefinitionRecord);

    ULong getClientAttribute(String attributeName, String attributeCode, ULong clientId, ULong executionId);

    ULong createClientAttribute(String attributeName, String attributeCode, ULong clientId, ULong executionId);

    List<TmbClientAttributeRecord> getAttributesRecords(List<ULong> attributesId);
}
