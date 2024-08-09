package com.unilog.prime.etl2.validator;

import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

@SuppressWarnings("unchecked")
public class ImportItemReplacePartialHeaderValidator {
	private static final String FIELD_NAMES = "FIELD_NAMES";
	private static final String SERVICES = "SERVICES";
	protected List<String> xFieldNameList;

	protected TreeSet<String> servicesRequired = Sets.newTreeSet();

	
	public ImportItemReplacePartialHeaderValidator(List<String> xFieldNameList) {
		this.xFieldNameList = xFieldNameList;
	}

	public List<String> getServicesRequired() {
		return Lists.newArrayList(servicesRequired);
	}

	protected void validateType(List<String> importHeader, Map<String, Object> headers, List<String> missingHeaders) {
		matchAtLeastOneField(importHeader, headers);
		matchItemTableField(importHeader, headers, missingHeaders);
		matchAllField(importHeader, headers, missingHeaders);
		matchGroupOfFields(importHeader, headers, missingHeaders);
	}

	public void matchAtLeastOneField(List<String> importHeader, List<String> tableHeader, List<String> serviceNames) {
		List<String> intersection = new ArrayList<>(CollectionUtils.intersection(importHeader, tableHeader));
		if (!intersection.isEmpty() && tableHeader.containsAll(intersection)) {
			servicesRequired.addAll(serviceNames);
			importHeader.removeAll(intersection);
		}
	}

	protected void matchAtLeastOneField(List<String> importHeader, Map<String, Object> headers) {
		List<Object> singleMatchList = (List<Object>) headers.get("singleMatchList");
		for (Object singleMatchObj : singleMatchList) {
			Map<String, Object> feildNServiceMap = (Map<String, Object>) singleMatchObj;
			matchAtLeastOneField(importHeader, (List<String>) feildNServiceMap.get(FIELD_NAMES),
					(List<String>) feildNServiceMap.get(SERVICES));
		}
	}

	private void matchItemTableField(List<String> importHeader, Map<String, Object> headers, List<String> missingHeaders) {
		Map<String, Object> feildNServiceMap = (Map<String, Object>) headers.get("itemTableMatchList");
		subsetFieldsMatch(importHeader, (List<String>) feildNServiceMap.get(FIELD_NAMES),
				(List<String>) feildNServiceMap.get(SERVICES), missingHeaders);
	}

	public final void validate(List<String> importHeader, Map<String, Object> headers, List<String> missingHeaders, 
			List<String> invalidHeaders) {
		if(CollectionUtils.isEmpty(importHeader)) return;
		List<String> copyOfImportHeader = Lists.newArrayList(importHeader);
		if(!importHeader.get(0).equals(PART_NUMBER)) {
			invalidHeaders.add(PART_NUMBER);
			copyOfImportHeader.remove(PART_NUMBER);
		} else
		    copyOfImportHeader.remove(0);
		validateType(copyOfImportHeader, headers, missingHeaders);
		validateXFields(copyOfImportHeader);
		invalidHeaders.addAll(copyOfImportHeader);
	}

	private void validateXFields(List<String> copyOfImportHeader) {
		List<String> list = new ArrayList<String>(copyOfImportHeader);
		for (String xField : list) {
			if (xField.startsWith("EX_") && xFieldNameList.contains(xField.replaceFirst("EX_", ""))) {
				servicesRequired.add("xField");
				copyOfImportHeader.remove(xField);
			}
		}
	}

	protected void matchGroupOfFields(List<String> importHeader, Map<String, Object> headers, List<String> missingHeaders) {
		List<Object> groupMatchList = (List<Object>) headers.get("groupMatchList");
		for (Object groupMatchObj : groupMatchList) {
			Map<String, Object> feildNServiceMap = (Map<String, Object>) groupMatchObj;
			matchGroupOfFields(importHeader, (List<String>) feildNServiceMap.get(FIELD_NAMES),
					(List<String>) feildNServiceMap.get("FIELD_NAMES_SUBSET"),
					(List<String>) feildNServiceMap.get(SERVICES), missingHeaders);
		}
	}

	protected void matchGroupOfFields(List<String> importHeader, List<String> tableHeader,
			List<String> requiredGroupField, List<String> serviceNames, List<String> missingHeaders) {
		List<String> intersection = ListUtils.intersection(importHeader, tableHeader);
		if (!intersection.isEmpty() && tableHeader.containsAll(intersection)) {

			int nextGroupCount = 1;
			int checkedFieldCount = 0;
			List<String> expectedFields = new ArrayList<>();
			List<String> validFields = new ArrayList<>();
			int i=0;
			int counter =1;
			while (i < intersection.size()) {	
				String field = intersection.get(i);
				int finalNextInt = nextGroupCount;
				List<String> expectedGrp = requiredGroupField.stream().map(f -> f + finalNextInt)
						.collect(Collectors.toList());
				expectedFields.removeAll(expectedGrp);
				expectedFields.addAll(expectedGrp);
				if (expectedGrp.contains(field)) {
					checkedFieldCount++;
					if (checkedFieldCount == requiredGroupField.size()) {
						checkedFieldCount = 0;
					    nextGroupCount++;
					}
					validFields.add(field);
					i++;
				} else if (counter > 200) {
					break;
				} else {
			        checkedFieldCount++;
			        counter++;
					if (checkedFieldCount == requiredGroupField.size()) {
						checkedFieldCount = 0;
					    nextGroupCount++;
					}
			    }
			}
			missingHeaders.addAll(CollectionUtils.subtract(expectedFields,validFields));
			servicesRequired.addAll(serviceNames);
			importHeader.removeAll(intersection);
		}
	}

	protected void matchAllField(List<String> importHeader, Map<String, Object> headers, List<String> missingHeaders) {	
		List<Object> allMatchFieldList = (List<Object>) headers.get("allMatchList");
		for (Object allMatchObj : allMatchFieldList) {
			Map<String, Object> feildNServiceMap = (Map<String, Object>) allMatchObj;
			subsetFieldsMatch(importHeader, (List<String>) feildNServiceMap.get(FIELD_NAMES),
					(List<String>) feildNServiceMap.get(SERVICES), missingHeaders);
		}
	}

	public void subsetFieldsMatch(List<String> importHeader, List<String> tableHeader, List<String> serviceNames, 
			List<String> missingHeaders) {
		if (importHeader.containsAll(tableHeader)) {
			servicesRequired.addAll(serviceNames);
			importHeader.removeAll(tableHeader);
		} else if (!CollectionUtils.intersection(importHeader, tableHeader).isEmpty()) {
			missingHeaders.addAll(CollectionUtils.subtract(tableHeader, importHeader));
			importHeader.removeAll(tableHeader);
		}
	}

	public void validateMfrCategorization(List<String> copyOfImportHeader, List<String> missingHeaders) {
		if(!copyOfImportHeader.contains("EX_MFG_CATEGORIZATION"))
			missingHeaders.add("EX_MFG_CATEGORIZATION");
		if(!copyOfImportHeader.contains("MANUFACTURER_NAME"))
			missingHeaders.add("MANUFACTURER_NAME");
		if(!copyOfImportHeader.contains("BRAND_NAME"))
			missingHeaders.add("BRAND_NAME");
	}
}
