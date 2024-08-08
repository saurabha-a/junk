package com.unilog.cx1.pim.commons.service;

import java.text.ParseException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jooq.types.ULong;
import org.jooq.Record;
import org.jooq.Result;

import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;
import com.unilog.prime.jooq.tables.records.TmbCategoryFieldRecord;
import com.unilog.prime.jooq.tables.records.TmbClientCategoryRecord;
import com.unilog.prime.jooq.tables.records.TmbFormulaRecord;
import com.unilog.prime.jooq.tables.records.TmbLuFieldNamesRecord;

/**
 * @author Abhinav Saurabh
 *
 */
public interface IFormulaService {
	
	public List<String> parseFormula(String line) throws ParseException;
	public ULong getClientAgainstFormula(ULong id);
	public ULong getClientAgainstCategoryField(ULong id);
	public TmbFormulaRecord getFormulaById(ULong id);
	public TmbCategoryFieldRecord getCategoryFieldById(ULong id);
	public TmbClientCategoryRecord getLeafCategoryRecord(ULong categoryId, ULong clientId);
	public List<ULong> getLeafNodeIds(ULong clientId);
	public List<ULong> getCategoryFieldIds(ULong clientId);
	public boolean isLeafNode(ULong categoryId, ULong clientId);
	public void incrementOrDecrementFormulaCount(ULong id,  boolean increment);
	public List<ULong> getCategoryFieldCategoryFieldIds(ULong clientId);
	public TmbLuFieldNamesRecord getFieldNamesById(ULong id);
	public TmbLuFieldNamesRecord getFieldNamesByFieldName(String name);
	public Record getFormulaByUniqueIndex(ULong categoryFieldId, ULong fieldId);
	public Result<TmbLuFieldNamesRecord> getFieldNamesByFieldNamePartial(String name);
	public String calculateFieldValueFromFormula(ULong clientId,String itemId,List<String> parsedFields,Set<String> valueFields);
	public Map<String,String> getInvertedBaseFieldsMap();
	public Tuple<Map<String, Object>, List<String>> getDataNHeaders(ULong clientID, Map<String, Object> dataKeys);
	public void buildComputedFormulaImportMap(ULong clientId, Map<String,Object> data, ULong subsetId);
	public Tuple<String,String> getEncodedNDecodedFormulaPair(ULong clientId, List<String> parsedFields) throws ParseException;
	public List<String> getBaseFieldsList();
	public void refreshFormulaForCategoryField(ULong clientId, ULong categoryId);
	public Map<String,Object> fetchItemAssociatedFieldsWithFormula(ULong categoryId);
	public void buildDataInRequiredFormat(Map<String,Object> data);
	public void buildDataInRequiredFormatForRetrieve(Map<String,Object> data,List<String> errors);
	public void validateFieldsForRetrieve(ULong clientId, Object fieldsObject, List<String> errors);
	public Map<String,Object> calculateAttributes(ULong clientId,final Map<String,Object> data);
	public Tuple<Map<String,Object>, List<String>> calculateAttributes(ULong clientId,SiSubsetDefinitionRecord definition,final Map<String,Object> data,boolean isOverwrite);
	
}
