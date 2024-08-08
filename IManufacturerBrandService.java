
package com.unilog.cx1.pim.commons.service;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.jooq.Condition;
import org.jooq.Record;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.types.ULong;

import com.unilog.cx1.pim.commons.enumeration.UpdateType;
import com.unilog.prime.jooq.tables.records.IdwManufacturerMapRecord;
import com.unilog.prime.jooq.tables.records.TmbBrandRecord;
import com.unilog.prime.jooq.tables.records.TmbManufacturerRecord;
import org.springframework.cache.annotation.Cacheable;

public interface IManufacturerBrandService {

	public TmbManufacturerRecord getManufacturerIdByName(ULong executionId, Set<ULong> clientIds, String name);

	public TmbManufacturerRecord getManufacturerIdByCode(ULong executionId, Set<ULong> clientIds, String mfrCode);

	String generateManufacturerCode(ULong executionId, ULong clientId);

	String generateBrandCode(ULong clientId);

	public ULong createManufacturer(ULong executionId, ULong userId, ULong clientId, Set<ULong> set, String manName,
			String manLogo, String manCode,String manExternalId, String manUrl, String manDesc);

	public void updateManufacturer(ULong executionId, UpdateType updateType, ULong userId, ULong clientId,
			Set<ULong> clientIds, ULong manId, String manLogo, String manCode, String manExternalId, String manUrl,String manDesc);

	public TmbBrandRecord getBrandIdByName(ULong executionId, Set<ULong> clientIds, ULong manId, String brandName,
			ULong clientId, ULong publisherId, String clientTypeCode);

	public void updateBrand(ULong executionId, UpdateType updateType, ULong clientId, ULong userId, ULong brandId,
			String brandDesc, String brandImage, // NOSONAR
			String brandURL, String brandCode,String brandExternalId);

	public Map<Object, Object> idwManufacturerCurrency();

	@Cacheable(value = "Brand", key = "{#executionId, #brandId}", unless = "#result == null")
	TmbBrandRecord getBrandId(ULong executionId, ULong brandId);

	public ULong createBrand(ULong executionId, ULong userId, ULong clientId, ULong manId, String brandName,
							 String brandDesc, String brandImage, String brandURL, // NOSONAR
							 String brandCode,String brandExternalId);

	public Map<String, Object> getPrimeMfrAndBrand(String idwPubId, String idwBrandName, String idwMfrName,
			ULong executionId);

	public void createIdwMfrAndBrandMappingRecord(String idwPubId, String idwBrandName, String idwMfrName, String mfrId,
			String brandId);

	List<ULong> getAllowedPublisherIds(ULong executionId, ULong clientId);

	public void renameBrand(String manufacturerName, String brandName, String newBrandName, ULong brandId);

	public void renameManufacturer(String manufacturerName, String newManufacturerName, ULong manufacturerId,
			ULong clientId);

	public String getClientCode(ULong clientId);

	public List<ULong> getBrandIdList(ULong manufacturerId);

	public List<ULong> getManufacturerIdList(String manufacturerName, ULong clientId);

	public List<ULong> restrictedManufacturerIdList(ULong manufacturerId);

	public List<ULong> exludedManufacturerIdList(ULong manufacturerId);

	public Result<IdwManufacturerMapRecord> getIdwManufacturerMapRecord(Condition condition);

	public void deleteManufacturer(ULong manufacturerId);

	public List<ULong> getBrandIdListForManufacturer(String manufacturerName, ULong clientId, String brandName);

	public int isRestrictedBrand(ULong brandId, String manufacturerName, String brandName);

	public int isExludedBrand(ULong brandId, String manufacturerName, String brandName);

	public Stream<Record3<ULong, String, String>> getBatchSubsetDetails();

	public boolean isBrandModified(ULong brandId, Table<Record> e);

	public List<String> getItemIdList(ULong brandId, ULong publisherId);

	public void deleteBrand(ULong brandId);

	public void checkForBrandCodeExists(ULong clientId, String brandCode);

	public ULong getClientIdForPublisherId(ULong publisherId);

	void checkForManufacturerNameExists(ULong clientId, String mfrName);

	ULong getPublisherClientIdThruClientId(ULong clientId);

	void checkForManufacturerCodeExists(String manufacturerCode, Set<ULong> clientIds);

	void checkForBrandNameExists(ULong clientId, String brandName,ULong manufacturerId);

	public String getClientTypeCode(ULong clientId);

	Stream<Record3<ULong, String, String>> getSubsetsListForSubscriber(ULong brandId, ULong clientId);

	ULong getPublisherIdThruClientId(ULong clientId);

	TmbBrandRecord getBrandIdByCode(ULong executionId, Set<ULong> clientIds, ULong manId, String brandCode);

	void udpateManufacturerName(ULong executionId, UpdateType updateType, ULong userId, ULong clientId,
			Set<ULong> clientIds, ULong manId, String manLogo, String manName, String manExternalId, String manUrl,String manDesc);
	
	 void validateImage(String imageAddress);
	
	 public Record2<String, String> getManufacturerCategoryMapping(ULong clientId,
			 String mfrName, String brandName, String manufacturerCategory);
	 
	public void deleteManufacturerCategoryMapping(Condition condition);

	 
}
