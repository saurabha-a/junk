package com.unilog.prime.commons.model;

import java.io.Serializable;
import java.util.Map;

public class RowDataObject implements Serializable {

	private static final long serialVersionUID = -1197772135930774320L;

	private Map<String, Object> data; // NOSONAR
	private SourceFormat sourceFormat;
	
	public RowDataObject() {
		this(null,null);
	}

	public RowDataObject(SourceFormat sourceFormat, Map<String, Object> data) {
		this.sourceFormat = sourceFormat;
		this.data = data;
	}

	public Map<String, Object> getData() {
		return data;
	}

	public void setData(Map<String, Object> data) {
		this.data = data;
	}

	public SourceFormat getSourceFormat() {
		return sourceFormat;
	}

	public void setSourceFormat(SourceFormat sourceFormat) {
		this.sourceFormat = sourceFormat;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((data == null) ? 0 : data.hashCode());
		result = prime * result + ((sourceFormat == null) ? 0 : sourceFormat.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		RowDataObject other = (RowDataObject) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		if (sourceFormat == null) {
			if (other.sourceFormat != null)
				return false;
		} else if (!sourceFormat.equals(other.sourceFormat))
			return false;
		return true;
	}

}
