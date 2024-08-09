package com.unilog.prime.commons.model;

import java.io.Serializable;
import java.util.List;

import com.unilog.prime.commons.enumeration.FormatType;

import lombok.Data;
import lombok.experimental.Accessors;

@Accessors(chain = true)
public @Data class SourceFormat implements Serializable {

	private static final long serialVersionUID = -3548182072543496363L;
	private FormatType format;
	private List<String> header;
	private List<PTPHeader> ptpHeaders;
	private XMLHeader xmlFormat;

	public SourceFormat() {
		this(null, null);
	}
	
	public SourceFormat(FormatType formatType, List<String> header) {
		this.format = formatType;
		this.header = header;
	}
}
