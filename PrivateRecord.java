package com.unilog.cx1.pim.commons.model.recordtype;

import java.util.List;
import java.util.Map;

import static com.unilog.prime.commons.util.CommonUtil.canDeleteField;
import static com.unilog.prime.commons.util.StringUtil.safeValueOf;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import com.google.common.collect.Lists;
public class PrivateRecord extends RecordType {

	@Override
	public void deleteValue(Map<String, Object> newR, Map<String, Object> oldR, Map<String, Object> shadowR,
			List<String> stringTypeDeletableFields, List<String> numericTypeDeletableFields) {
		if (oldR.isEmpty()) {
			newR.entrySet().removeIf(entry -> entry.getValue() == null);
		}else {
			  List<String> numericFieldsToBeRemoved = Lists.newArrayList();
		        newR.forEach((k, v) -> {
		            String oldVal = safeValueOf(oldR.get(k));
		            if (isNotBlank(oldVal) && shadowR.get(k) == null) {
		                if (canDeleteField(v)) {
		                    if (stringTypeDeletableFields.contains(k.toUpperCase()))
		                        newR.put(k, "");
		                    else if (numericTypeDeletableFields.contains(k.toUpperCase())) {
		                        if (isNotBlank(oldVal))
		                            newR.put(k, -1);
		                        else
		                            numericFieldsToBeRemoved.add(k);
		                    }
		                }
		            } else if(isBlank(oldVal) && isBlank(safeValueOf(v))) {
		                newR.put(k, null);
		            }
		        });
		        numericFieldsToBeRemoved.stream().forEach(newR::remove);
		        newR.entrySet().removeIf(k -> k.getValue() == null && shadowR.get(k.getKey()) == null);
		    
		}
	}

	@Override
	public boolean isPrivate() {
		return true;
	}
}
