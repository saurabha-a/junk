package com.unilog.cx1.pim.commons.service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.Record4;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.types.ULong;

import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;
import com.unilog.cx1.pim.commons.model.recordtype.RecordType;
import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.jooq.enums.SiItemUpdateDetailsStatus;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;

public interface IItemService {

	ULong getPublisherIdForClientId(ULong executionId, ULong clientId);

	ULong getPublisherClientIdThruClientId(ULong clientId);

	void doImport(Import anImport, PimDataObject qdo, boolean isItemImportWithReplace,boolean isGenericImport);

	SiSubsetDefinitionRecord getSubsetDefinition(ULong executionId, ULong subsetId);

	String getPreferredManufacturer(String manufacturer, ULong clientId);
	
	String getPfrdSupplierExcItem(String partnumber);

	Timestamp createDummyShadowRecord(SiSubsetDefinitionRecord definition, PimDataObject qdo);

    List<Map<String, Object>> getItemPrice(String partNumber, ULong clientId, ULong executionId);

    void updateItemCreatedAt(SiSubsetDefinitionRecord definition, Map<String, Object> data);

    void doItemPropertiesImport(Import anImport, PimDataObject qdo, Tuple<Map<String, Object>, Condition> tuple);

    String getAdPartnumber(String nItemId);

    void insertIdwAdPartnumberMapping(Object nitemId, Object partnumber);

    Record getItemRecord(SiSubsetDefinitionRecord definition, String partNumber, ULong executionId, ULong publisherId);

    boolean isItemExistsInExternal(ULong executionId, String partNumber, ULong publisherId);

    Timestamp updateExternalSubsetItemTimeStamp(String itemId, ULong publisherId);

    RecordType getRecordType(SiSubsetDefinitionRecord definition, PimDataObject qdo);

    Set<ULong> getMyAllowedClientIds(ULong clientId, ULong executionId);
    
	void deleteItem(String clientName, String partNumber, ULong externalSubsetId);

	void updateIdwPartNumberMapWithRetainId(String partNumber, String retainPartNumber,
			List<String> externalSystemItemIdList);

	List<String> getExternalIds(String partNumber);
	
	void insertOrUpdateImportDetails(ULong subsetId, ULong publisherId, String itemId, ULong updatedBy, ULong executionId,
			Timestamp updatedTime, SiItemUpdateDetailsStatus siItemUpdateDetailsStatus);

	String getClientName(ULong clientId);

	List<String> getItems(String partNumber, ULong publisherId);

	int getCountFromIdwPartNumberMap(String partNumber);

	void removeRetainId(List<String> externalSystemItemIdList);

	Stream<Record4<ULong, String, String, String>> getBatchSubsetDetails(ULong clientId);

	boolean isItemExistsInBatchSubset(String itemId, Table<Record> e);
	
	boolean isLinkedItemExistsInBatchSubset(String itemId, Table<Record> e);

	Result<Record> getExternalSubsetItem(String mfrName, String mfrPartNumber, ULong publisherId);

	void createOrUpdateBulkDummyShadowRecord(ULong subsetId, List<Map<String, String>> dataList, ULong createdBy);
	
	String getClientTypeCode(ULong clientId);

	String getDefaultManufacturerStatus(ULong publisherId);
	
	String getByIdWithHeaderImagePath(ULong publisherclientId);
	
	String imageNameFromFilePath(String filePath);
	
	String getDefaultEnrichedIndicatorStatus(ULong publisherId);
	
	Record getItemRecordWhenNull(SiSubsetDefinitionRecord definition, String partNumber, ULong executionId, ULong publisherId);

	Record checkItems(String partNumber, ULong subsetId, ULong publisherId, SiSubsetDefinitionRecord definition);

}
