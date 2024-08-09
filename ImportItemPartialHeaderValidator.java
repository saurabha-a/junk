package com.unilog.prime.etl2.validator;

import java.util.List;
import java.util.Map;

public class ImportItemPartialHeaderValidator extends ImportItemReplacePartialHeaderValidator {

	public ImportItemPartialHeaderValidator(List<String> xFieldNameList) {
		super(xFieldNameList);
	}

	@Override
    protected void validateType(List<String> importHeader, Map<String, Object> headers, List<String> missingHeaders) {
        matchAtLeastOneField(importHeader, headers);
        matchAllField(importHeader, headers, missingHeaders);
        matchGroupOfFields(importHeader, headers, missingHeaders);
    }
}
