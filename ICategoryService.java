package com.unilog.cx1.pim.commons.service;

import org.jooq.types.ULong;

import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;

public interface ICategoryService {

    void doImport(Import anImport, PimDataObject qdo);

	ULong getCategoryKey(SiSubsetDefinitionRecord definition, String categoryCode, String categoryName,
                         ULong clientId, ULong executionId);
}
