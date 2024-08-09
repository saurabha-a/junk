package com.unilog.cx1.pim.commons.model.recordtype;

import java.util.List;
import java.util.Map;

public abstract class InheritedRecord extends RecordType {

    public abstract void deleteValue(Map<String, Object> newR, Map<String, Object> oldR,
                                     Map<String, Object> shadowR, List<String> stringTypeDeletableFields, List<String> numericTypeDeletableFields);

    @Override
    public boolean canHonorNullValue() {
        return true;
    }

    @Override
    public boolean isPrivate() {
        return false;
    }
}
