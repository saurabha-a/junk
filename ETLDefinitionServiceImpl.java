package com.unilog.prime.etl2.service.impl;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.jooq.types.ULong;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.commons.util.StringUtil;
import com.unilog.prime.etl2.service.IETLDefinitionService;
import com.unilog.prime.etl2.service.IMiscJobExecutionService;
import com.unilog.prime.misc.jooq.enums.MiscEtlEtlType;
import com.unilog.prime.misc.jooq.tables.records.MiscEtlRecord;

@Service(ETLDefinitionServiceImpl.BEAN_ID)
public class ETLDefinitionServiceImpl implements IETLDefinitionService {

	public static final String BEAN_ID = "ETLDefinitionService";
	
	@Autowired
	protected IMiscJobExecutionService executionService;
	
	@Override
	public Map<String, String> getMappedDBHeaders(ULong executionId) {

		Map<String, Object> etlDefinition = new LinkedHashMap<>();
		MiscEtlRecord record = this.executionService.getETLOthRecord(executionId);
		if (record != null) {

			Tuple<MiscEtlEtlType, Object> definition = this.executionService.getLogic(record);
			if (definition != null && definition.getFirstValue() == MiscEtlEtlType.D)
				etlDefinition.put("publisher", definition.getSecondValue());
		}

		record = this.executionService.getETLRecord(executionId);
		if (record != null) {

			Tuple<MiscEtlEtlType, Object> definition = this.executionService.getLogic(record);
			if (definition != null && definition.getFirstValue() == MiscEtlEtlType.D)
				etlDefinition.put("custom", definition.getSecondValue());
		}

		return etlDefinition.isEmpty() ? null : processEtlDefinition(etlDefinition);
	}
	
	@SuppressWarnings("unchecked")
	protected Map<String, String> processEtlDefinition(Map<String, Object> etlDefinition) {
		Map<String, String> finalEtlDefinition = null;
		for (Map.Entry<String, Object> etlTypeEntry : etlDefinition.entrySet()) {
			Map<String, Object> etlMap = (Map<String, Object>) etlTypeEntry.getValue();
			Map<String, String> tempFinalEtlDefinition = new LinkedHashMap<>();
			for (Map.Entry<String, Object> templateSourceEntry : etlMap.entrySet()) {
				Map<String, Object> valueMap = (Map<String, Object>) templateSourceEntry.getValue();
				tempFinalEtlDefinition.put(templateSourceEntry.getKey(), StringUtil.safeValueOf(valueMap.get("src")));
			}

			if (finalEtlDefinition != null) {
				Map<String, String> tempMap = new LinkedHashMap<>();
				for (Entry<String, String> tempFinalEtlDefinitionEntry : tempFinalEtlDefinition.entrySet()) {
					if (finalEtlDefinition.containsKey(tempFinalEtlDefinitionEntry.getValue())) {
						tempMap.put(tempFinalEtlDefinitionEntry.getKey(),
								finalEtlDefinition.get(tempFinalEtlDefinitionEntry.getValue()));
						finalEtlDefinition.remove(tempFinalEtlDefinitionEntry.getValue());
					}
				}
				finalEtlDefinition.putAll(tempMap);
			} else {
				finalEtlDefinition = tempFinalEtlDefinition;
			}
		}

		return finalEtlDefinition;
	}
}
