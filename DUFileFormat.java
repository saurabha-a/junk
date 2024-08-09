package com.unilog.prime.downloadupload;

public enum DUFileFormat {

	XLS("application/vnd.ms-excel"), XML("text/xml"),
	XLSX("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"), CSV("text/csv"),
	TSV("text/tab-separated-values"), OBJECT("application/octet-stream");

	private String mimeType;

	private DUFileFormat(String mimeType) {

		this.mimeType = mimeType;
	}

	public String getMimeType() {
		return this.mimeType;
	}
}
