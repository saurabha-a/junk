package com.unilog.cx1.pim.commons.model.recordtype;


import java.util.List;
import java.util.Map;

public class PublisherRecord extends RecordType {

    @Override
    public void deleteValue(Map<String, Object> newR, Map<String, Object> oldR,
                            Map<String, Object> shadowR, List<String> stringTypeDeletableFields, List<String> numericTypeDeletableFields) {
        removeNullValue(newR, stringTypeDeletableFields, numericTypeDeletableFields);
    }

    @Override
    public boolean isPrivate() {
        return false;
    }
}
