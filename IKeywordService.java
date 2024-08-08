package com.unilog.cx1.pim.commons.service;

import java.util.Map;

import org.springframework.transaction.annotation.Transactional;

import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;

public interface IKeywordService {
    Map<String, String> getKeywordTypeMap();

    @Transactional
    void doImport(Import anImport, PimDataObject qdo);
}
