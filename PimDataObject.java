package com.unilog.cx1.pim.commons.model;

import static com.unilog.prime.commons.util.StringUtil.safeValueOf;

import java.io.Serializable;
import java.util.Map;

import org.jooq.types.ULong;

import com.unilog.prime.commons.model.SourceFormat;

import lombok.Data;
import lombok.experimental.Accessors;

@Data
@Accessors(chain = true)
public class PimDataObject implements Serializable {

	private static final long serialVersionUID = -1197772135930774320L;

	protected ULong clientId;
	protected ULong userId;
	protected Map<String, Object> sourceData; // NOSONAR
	protected Map<String, Object> data; // NOSONAR
	protected ULong executionId;
	protected SourceFormat sourceFormat;
	
	//adding this to get mapped headers in the log files
	protected SourceFormat inputSourceFormat;
	
	public String getValue(String name) {
		return safeValueOf(data.get(name));
	}

	public boolean contains(String key) {
		if(data == null)
			return false;
		return data.containsKey(key);
	}
}
