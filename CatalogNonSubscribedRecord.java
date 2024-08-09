package com.unilog.cx1.pim.commons.model.recordtype;


import org.apache.commons.lang3.NotImplementedException;

import java.util.List;
import java.util.Map;

public class CatalogNonSubscribedRecord extends InheritedRecord {

    @Override
    public void deleteValue(Map<String, Object> newR, Map<String, Object> oldR,
                            Map<String, Object> shadowR, List<String> stringTypeDeletableFields, List<String> numericTypeDeletableFields) {
        throw new NotImplementedException("Method is not implemented");
    }

    public boolean isCatalogNonSubscribedRecord() {
        return true;
    }
}
