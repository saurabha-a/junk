package com.unilog.cx1.pim.commons.service.impl;

import static com.unilog.iam.jooq.tables.IamClient.IAM_CLIENT;
import static com.unilog.iam.jooq.tables.SiPublisherClient.SI_PUBLISHER_CLIENT;
import static com.unilog.iam.jooq.tables.SiPublisherSubscriber.SI_PUBLISHER_SUBSCRIBER;
import static com.unilog.prime.commons.util.TextUtil.anyToDBFormat;
import static com.unilog.prime.jooq.tables.IdwManufacturerMap.IDW_MANUFACTURER_MAP;
import static com.unilog.prime.jooq.tables.SiClientItem.SI_CLIENT_ITEM;
import static com.unilog.prime.jooq.tables.SiSubsetDefinition.SI_SUBSET_DEFINITION;
import static com.unilog.prime.jooq.tables.TmbBrand.TMB_BRAND;
import static com.unilog.prime.jooq.tables.TmbBrandExceptions.TMB_BRAND_EXCEPTIONS;
import static com.unilog.prime.jooq.tables.TmbBrandExclusionList.TMB_BRAND_EXCLUSION_LIST;
import static com.unilog.prime.jooq.tables.TmbManufacturer.TMB_MANUFACTURER;
import static com.unilog.prime.jooq.tables.TmbManufacturerCategoryMapping.TMB_MANUFACTURER_CATEGORY_MAPPING;
import static com.unilog.prime.jooq.tables.TmbManufacturerExceptions.TMB_MANUFACTURER_EXCEPTIONS;
import static com.unilog.prime.jooq.tables.TmbManufacturerExclusionList.TMB_MANUFACTURER_EXCLUSION_LIST;
import static java.lang.String.valueOf;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.Validate.isTrue;
import static org.apache.commons.lang3.math.NumberUtils.isCreatable;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.table;
import static org.jooq.tools.StringUtils.isBlank;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.Validate;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.SelectConditionStep;
import org.jooq.SelectOrderByStep;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.unilog.cx1.pim.commons.constants.PimException;
import com.unilog.cx1.pim.commons.enumeration.UpdateType;
import com.unilog.cx1.pim.commons.service.IManufacturerBrandService;
import com.unilog.prime.commons.enumeration.PrimeResponseCode;
import com.unilog.prime.commons.exception.PrimeException;
import com.unilog.prime.commons.exception.ResourceAlreadyExistException;
import com.unilog.prime.jooq.tables.IdwManufacturerMap;
import com.unilog.prime.jooq.tables.SiClientItem;
import com.unilog.prime.jooq.tables.SiSubsetDefinition;
import com.unilog.prime.jooq.tables.TmbBrand;
import com.unilog.prime.jooq.tables.TmbBrandExceptions;
import com.unilog.prime.jooq.tables.TmbBrandExclusionList;
import com.unilog.prime.jooq.tables.TmbManufacturer;
import com.unilog.prime.jooq.tables.TmbManufacturerCategoryMapping;
import com.unilog.prime.jooq.tables.TmbManufacturerExceptions;
import com.unilog.prime.jooq.tables.TmbManufacturerExclusionList;
import com.unilog.prime.jooq.tables.records.IdwManufacturerMapRecord;
import com.unilog.prime.jooq.tables.records.TmbBrandRecord;
import com.unilog.prime.jooq.tables.records.TmbManufacturerRecord;

@Service(ManufacturerBrandService.BEAN_ID)
public class ManufacturerBrandService implements IManufacturerBrandService {

	private static final String MANUFACTURER_FORBIDDEN_MESSAGE = "The provided MANUFACTURER_NAME does not exist for your Publisher.";
	private static final Logger logger = LoggerFactory.getLogger(ManufacturerBrandService.class);
	public static final String BEAN_ID = "etlManufacturerBrandService";
	
	private static final ArrayList<String> INVALID_FILE_CHARACTERS = new ArrayList<String>(
			Arrays.asList("/", "\\", "<", ">", ":", "\"", "|", "?", "*"));
	
	private static final ArrayList<String> FILE_EXTENSIONS = new ArrayList<String>(
			Arrays.asList(".jpg", ".png", ".tiff", ".gif", ".bmp", ".svg", ".jpeg"));
	

	public static final String NEW_BRAND_NAME = "NEW_BRAND_NAME";

	@Autowired
	private DSLContext dslContext;

	@Override
	@Cacheable(value = "Manufacturer", key = "{#executionId, #name}", unless = "#result == null")
	public TmbManufacturerRecord getManufacturerIdByName(ULong executionId, Set<ULong> clientIds, String name) {
		return this.dslContext.selectFrom(TMB_MANUFACTURER)
				.where(TMB_MANUFACTURER.MANUFACTURER_NAME.eq(name).and(TMB_MANUFACTURER.CLIENT_ID.in(clientIds)))
				.fetchOne();
	}

	@Override
	@Cacheable(value = "Manufacturer", key = "{#executionId, #mfrCode}", unless = "#result == null")
	public TmbManufacturerRecord getManufacturerIdByCode(ULong executionId, Set<ULong> clientIds, String mfrCode) {
		return this.dslContext.selectFrom(TMB_MANUFACTURER)
				.where(TMB_MANUFACTURER.MANUFACTURER_CODE.eq(mfrCode).and(TMB_MANUFACTURER.CLIENT_ID.in(clientIds)))
				.fetchOne();
	}

	@Override
	public String generateManufacturerCode(ULong executionId, ULong clientId) {
		Record1<Long> maxRecord = dslContext.select(DSL.max(DSL.cast(TMB_MANUFACTURER.MANUFACTURER_CODE, Long.class)))
				.from(TMB_MANUFACTURER).where(TMB_MANUFACTURER.CLIENT_ID.eq(clientId)).fetchOne();
		if (maxRecord != null && maxRecord.value1() != null)
			return valueOf(maxRecord.into(Long.class) + 1);
		else
			return valueOf(1);
	}

	@Override
	public String generateBrandCode(ULong clientId) {
		Record1<Long> maxRecord = dslContext.select(DSL.max(DSL.cast(TMB_BRAND.BRAND_CODE, Long.class))).from(TMB_BRAND)
				.innerJoin(TMB_MANUFACTURER).on(TMB_BRAND.MANUFACTURER_ID.eq(TMB_MANUFACTURER.ID))
				.where(TMB_MANUFACTURER.CLIENT_ID.eq(clientId)).fetchOne();
		if (maxRecord != null && maxRecord.value1() != null)
			return valueOf(maxRecord.into(Long.class) + 1);
		else
			return valueOf(1);
	}

	@Override
	@Transactional(isolation = Isolation.READ_COMMITTED)
	public ULong createManufacturer(ULong executionId, ULong userId, ULong clientId, Set<ULong> clientIds,
			String manName, String manLogo, String manCode, String manExternalId, String manUrl,String manDesc) {
		Validate.notNull(manCode, "Manufacturer code can't be null");
		TmbManufacturerRecord record = this.dslContext.newRecord(TMB_MANUFACTURER);
		record.setCreatedBy(userId);
		record.setManufacturerName(manName);
		record.setImage(manLogo);
		record.setUpdatedBy(userId);
		record.setExternalManufacturerId(manExternalId);
		record.setManufacturerUrl(manUrl);
		record.setManufacturerDescription(manDesc);
		try {
			record.setClientId(clientId);
		} catch (PimException ex) {
			logger.debug("Unable to create manufacturer.", ex);
			throw new PimException(
					"Unable to create manufacturer '" + manName + "' due to lack of publisher privilege: ");
		}

		checkForManufacturerCodeExists(manCode, clientIds);

		record.setManufacturerCode(manCode);
		record.store();

		return record.getId();
	}

	@Override
	public void updateManufacturer(ULong executionId, UpdateType updateType, ULong userId, ULong clientId,
			Set<ULong> clientIds, ULong manId, String manLogo, String manCode, String manExternalId, String manUrl,String manDesc) {

		TmbManufacturerRecord record = null;
		try {
			record = this.dslContext.selectFrom(TMB_MANUFACTURER)
					.where(TMB_MANUFACTURER.ID.eq(manId).and(TMB_MANUFACTURER.CLIENT_ID.eq(clientId))).fetchOne();
		} catch (PimException ex) {
			logger.debug("Unable to update manufacturer.", ex);
			throw new PimException(
					"Unable to update manufacturer with id '" + manId + "' due to lack of publisher privilege: ");
		}
		if (record == null)
			throw new PimException(MANUFACTURER_FORBIDDEN_MESSAGE);

		checkForManufacturerCodeExists(manCode, clientIds);

		if (updateType == UpdateType.OVER_WRITE_ALL_VALUES_IGNORE_NULL) {
			if (manLogo != null)
				record.setImage(manLogo);
			if (manCode != null)
				record.setManufacturerCode(manCode);
			if (manExternalId != null)
				record.setExternalManufacturerId(manExternalId);
			if (manUrl != null)
				record.setManufacturerUrl(manUrl);
			if (manDesc != null)
				record.setManufacturerDescription(manDesc);
		} else if (updateType == UpdateType.OVER_WRITE_NULL_VALUES) {
			if (record.getImage() == null)
				record.setImage(manLogo);
			if (record.getManufacturerCode() == null) {
				record.setManufacturerCode(manCode);
			}
			if (manExternalId == null)
				record.setExternalManufacturerId(manExternalId);
			if (manUrl == null)
				record.setManufacturerUrl(manUrl);
			if (manDesc == null)
				record.setManufacturerDescription(manDesc);
		} else {
			record.setImage(manLogo);
			record.setManufacturerCode(manCode);
			record.setExternalManufacturerId(manExternalId);
			record.setManufacturerUrl(manUrl);
			record.setManufacturerDescription(manDesc);
		}

		record.setUpdatedBy(userId);
		record.store();
	}
	
	@Override
	public void udpateManufacturerName(ULong executionId, UpdateType updateType, ULong userId, ULong clientId,
			Set<ULong> clientIds, ULong manId, String manLogo, String manName,String manExternalId, String manUrl,String manDesc) {
		TmbManufacturerRecord record = null;
		try {
			record = this.dslContext.selectFrom(TMB_MANUFACTURER)
					.where(TMB_MANUFACTURER.ID.eq(manId).and(TMB_MANUFACTURER.CLIENT_ID.eq(clientId))).fetchOne();
		} catch (PimException ex) {
			logger.debug("Unable to update manufacturer.", ex);
			throw new PimException(
					"Unable to update manufacturer with id '" + manId + "' due to lack of publisher privilege: ");
		}
		if (record == null)
			throw new PimException(MANUFACTURER_FORBIDDEN_MESSAGE);

		checkForManufacturerNameExists(clientId, manName);
		
		if (updateType == UpdateType.OVER_WRITE_ALL_VALUES_IGNORE_NULL) {
			if (manLogo != null)
				record.setImage(manLogo);
			if (manName != null)
				record.setManufacturerName(manName);
			if (manExternalId != null)
				record.setExternalManufacturerId(manExternalId);
			if (manUrl != null)
				record.setManufacturerUrl(manUrl);
			if (manDesc != null)
				record.setManufacturerDescription(manDesc);
		} else if (updateType == UpdateType.OVER_WRITE_NULL_VALUES) {
			if (record.getImage() == null)
				record.setImage(manLogo);
			if (record.getManufacturerCode() == null) {
				record.setManufacturerName(manName);
			}
			if (manExternalId == null)
				record.setExternalManufacturerId(manExternalId);
			if (manUrl == null)
				record.setManufacturerUrl(manUrl);
			if (manDesc == null)
				record.setManufacturerDescription(manDesc);
		} else {
			record.setImage(manLogo);
			record.setManufacturerName(manName);
			record.setExternalManufacturerId(manExternalId);
			record.setManufacturerUrl(manUrl);
			record.setManufacturerDescription(manDesc);
		}

		record.setUpdatedBy(userId);
		record.store();
	}

	@Cacheable(value = "Brand", key = "{#executionId, #manId, #brandName}", unless = "#result == null")
	@Override
	public TmbBrandRecord getBrandIdByName(ULong executionId, Set<ULong> clientIds, ULong manId, String brandName,
			ULong clientId, ULong publisherId, String clientTypeCode) {
		isTrue(!isBlank(brandName), "Brand name is required");
		
		if("P".equalsIgnoreCase(clientTypeCode)) {
			return this.dslContext.select(TMB_BRAND.fields()).from(TMB_BRAND).join(TMB_MANUFACTURER)
					.on(TMB_MANUFACTURER.ID.eq(TMB_BRAND.MANUFACTURER_ID)).where(TMB_BRAND.MANUFACTURER_ID.eq(manId)
							.and(TMB_BRAND.BRAND_NAME.eq(brandName)).and(TMB_MANUFACTURER.CLIENT_ID.in(clientIds)))
					.fetchOneInto(TmbBrandRecord.class);
		} else {
			TmbBrand aa = TmbBrand.TMB_BRAND.as("aa");
			TmbManufacturer bb = TmbManufacturer.TMB_MANUFACTURER.as("bb");
			return dslContext.select(aa.fields()).from(aa)
					.join(bb).on(aa.MANUFACTURER_ID.eq(bb.ID))
					.where(aa.MANUFACTURER_ID.eq(manId).and(aa.BRAND_NAME.eq(brandName)).and(bb.CLIENT_ID.in(clientIds))
							.and(aa.ID.notIn(dslContext.selectQuery(restrictedBrandRecordCondition(clientId, publisherId)))))
					.fetchOneInto(TmbBrandRecord.class);
		}
	}

	@Override
	@Cacheable(value = "Brand", key = "{#executionId, #manId, #brandCode}", unless = "#result == null")
	public TmbBrandRecord getBrandIdByCode(ULong executionId, Set<ULong> clientIds, ULong manId, String brandCode) {
		isTrue(!isBlank(brandCode), "Brand code is required");
		return this.dslContext.select(TMB_BRAND.fields()).from(TMB_BRAND).join(TMB_MANUFACTURER)
				.on(TMB_MANUFACTURER.ID.eq(TMB_BRAND.MANUFACTURER_ID)).where(TMB_BRAND.MANUFACTURER_ID.eq(manId)
						.and(TMB_BRAND.BRAND_CODE.eq(brandCode)).and(TMB_MANUFACTURER.CLIENT_ID.in(clientIds)))
				.fetchOneInto(TmbBrandRecord.class);
	}

	@Override
	@Cacheable(value = "Brand", key = "{#executionId, #brandId}", unless = "#result == null")
	public TmbBrandRecord getBrandId(ULong executionId, ULong brandId) {
		return this.dslContext.select(TMB_BRAND.fields()).from(TMB_BRAND).where(TMB_BRAND.ID.eq(brandId))
				.fetchOneInto(TmbBrandRecord.class);
	}

	@Override
	@Transactional(isolation = Isolation.READ_COMMITTED)
	public ULong createBrand(ULong executionId, ULong userId, ULong clientId, ULong manId, String brandName,
			String brandDesc, String brandImage, String brandURL, String brandCode,String brandExternalId) {
		Validate.notNull(brandCode, "Brand code can't be null");
		ULong manufacturerId = null;
		try {
			manufacturerId = this.dslContext.select(TMB_MANUFACTURER.ID).from(TMB_MANUFACTURER)
					.where(TMB_MANUFACTURER.ID.eq(manId).and(TMB_MANUFACTURER.CLIENT_ID.eq(clientId))).limit(1)
					.fetchOneInto(ULong.class);
		} catch (PimException ex) {
			logger.debug("Unable to update brand.", ex);
			throw new PimException(
					"Unable to update the brand with brand '" + brandName + "' due to lack of publisher privilege.");
		}

		if (manufacturerId == null)
			throw new PimException(MANUFACTURER_FORBIDDEN_MESSAGE);

		this.checkForBrandNameExists(clientId, brandName,manId);
		this.checkForBrandCodeExists(clientId, brandCode);
		TmbBrandRecord record = this.dslContext.newRecord(TMB_BRAND);
		record.setCreatedBy(userId);
		record.setUpdatedBy(userId);
		record.setManufacturerId(manId);
		record.setBrandName(brandName);
		record.setBrandDescription(brandDesc);
		record.setBrandUrl(brandURL);
		record.setImage(brandImage);
		record.setBrandCode(brandCode);
		record.setExternalBrandId(brandExternalId);
		try {
			record.store();
		} catch (DuplicateKeyException duplicateKeyException) {
			throw new ResourceAlreadyExistException(HttpStatus.CONFLICT,
					PrimeResponseCode.DUPLICATE_BRAND_NAME.getResponseMsg(), duplicateKeyException,
					PrimeResponseCode.DUPLICATE_BRAND_NAME.getResponseCode(), duplicateKeyException.getMessage());
		}
		return record.getId();
	}

	@Override
	public void updateBrand(ULong executionId, UpdateType updateType, ULong clientId, ULong userId, ULong brandId, // NOSONAR
			String brandDesc, String brandImage, String brandURL, String brandCode,String brandExternalId) {

		TmbBrandRecord record = this.dslContext.selectFrom(TMB_BRAND).where(TMB_BRAND.ID.eq(brandId)).fetchOne();

		ULong manufacturerId = null;

		try {
			manufacturerId = this.dslContext
					.select(TMB_MANUFACTURER.ID).from(TMB_MANUFACTURER).where(TMB_MANUFACTURER.ID
							.eq(record.getManufacturerId()).and(TMB_MANUFACTURER.CLIENT_ID.eq(clientId)))
					.limit(1).fetchOneInto(ULong.class);
		} catch (PimException ex) {
			logger.debug("Unable to update brand.", ex);
			throw new PimException(
					"Unable to update the brand with brand id '" + brandId + "' due to lack of publisher privilege.");
		}

		if (manufacturerId == null)
			throw new PimException(MANUFACTURER_FORBIDDEN_MESSAGE);

		if(!record.getBrandCode().toString().equalsIgnoreCase(brandCode))
			this.checkForBrandCodeExists(clientId, brandCode);

		if (updateType == UpdateType.OVER_WRITE_ALL_VALUES_IGNORE_NULL) {
			if (brandImage != null)
				record.setImage(brandImage);
			if (brandDesc != null)
				record.setBrandDescription(brandDesc);
			if (brandExternalId != null)
				record.setExternalBrandId(brandExternalId);
			
			if (brandURL != null)
				record.setBrandUrl(brandURL);
		} else if (updateType == UpdateType.OVER_WRITE_NULL_VALUES) {
			if (record.getImage() == null)
				record.setImage(brandImage);
			if (record.getBrandDescription() == null)
				record.setBrandDescription(brandDesc);
			if (record.getBrandUrl() == null)
				record.setBrandUrl(brandURL);
			if (brandExternalId == null)
				record.setExternalBrandId(brandExternalId);
		} else {
			record.setBrandDescription(brandDesc);
			record.setBrandUrl(brandURL);
			record.setImage(brandImage);
			record.setBrandCode(brandCode);
			record.setExternalBrandId(brandExternalId);
		}
		record.setUpdatedBy(userId);
		try {
			record.store();
		} catch (DuplicateKeyException duplicateKeyException) {
			throw new ResourceAlreadyExistException(HttpStatus.CONFLICT,
					PrimeResponseCode.DUPLICATE_BRAND_RESOURCE.getResponseMsg(), duplicateKeyException,
					PrimeResponseCode.DUPLICATE_BRAND_RESOURCE.getResponseCode(), duplicateKeyException.getMessage());
		}
	}

	@Override
	@Cacheable(value = "IdwManufacturerCurrency", key = "#root.methodName")
	public Map<Object, Object> idwManufacturerCurrency() {
		return dslContext.selectFrom(table(name("idw", "tbco"))).fetchMap(field("sName"), field("sCrncy"));
	}

	@Cacheable(value = "PrimeMfrAndBrand", key = "{#idwPubId, #idwBrandName, #idwMfrName, #executionId}", unless = "#result == null")
	@Override
	public Map<String, Object> getPrimeMfrAndBrand(String idwPubId, String idwBrandName, String idwMfrName,
			ULong executionId) {
		Condition condition = DSL.trueCondition();
		if (isNotBlank(idwPubId))
			condition = condition.and(field("IDW_PUB_ID").eq(idwPubId));
		if (isNotBlank(idwBrandName))
			condition = condition.and(field("IDW_BRAND_NAME").like(idwBrandName));
		if (isNotBlank(idwMfrName))
			condition = condition.and(field("IDW_MFR_NAME").like(idwMfrName));
		Record record = dslContext.selectFrom(table(name("prime", "idw_manufacturer_map"))).where(condition).limit(1)
				.fetchAny();

		return record != null ? record.intoMap() : null;
	}

	@Override
	public void createIdwMfrAndBrandMappingRecord(String idwPubId, String idwBrandName, String idwMfrName, String mfrId,
			String brandId) {
		ULong pubId = null;
		if (isNotBlank(idwPubId) && isCreatable(idwPubId)) {
			pubId = ULong.valueOf(idwPubId);
		}

		dslContext
				.insertInto(IDW_MANUFACTURER_MAP, field("IDW_PUB_ID"), field("IDW_MFR_NAME"), field("IDW_BRAND_NAME"),
						field("CIMM_MFR_NAME"), field("CIMM_BRAND_NAME"), field("CIMM_MFR_ID"), field("CIMM_BRAND_ID"))
				.values(pubId, idwMfrName, idwBrandName, anyToDBFormat(idwMfrName), anyToDBFormat(idwBrandName), mfrId,
						brandId)
				.onDuplicateKeyUpdate().set(field("CIMM_MFR_ID"), mfrId).set(field("CIMM_BRAND_ID"), brandId).execute();
	}

	@Cacheable(value = "getAllowedPublisherIds", key = "{#clientId, #executionId}", unless = "#result == null")
	public List<ULong> getAllowedPublisherIds(ULong executionId, ULong clientId) {

		ULong pubId = dslContext.select(SI_PUBLISHER_CLIENT.ID).from(SI_PUBLISHER_CLIENT)
				.where(SI_PUBLISHER_CLIENT.CLIENT_ID.eq(clientId)).limit(1).fetchOneInto(ULong.class);

		if (pubId != null)
			return List.of(pubId);

		return dslContext.select(SI_PUBLISHER_SUBSCRIBER.PUBLISHER_ID).from(SI_PUBLISHER_SUBSCRIBER)
				.where(SI_PUBLISHER_SUBSCRIBER.CLIENT_ID.eq(clientId)).fetchInto(ULong.class);
	}

	@Override
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void renameBrand(String manufacturerName, String brandName, String newBrandName, ULong brandId) {
		this.dslContext.update(TMB_BRAND_EXCEPTIONS).set(TMB_BRAND_EXCEPTIONS.BRAND_NAME, newBrandName)
				.where(TMB_BRAND_EXCEPTIONS.BRAND_ID.eq(brandId).and(TMB_BRAND_EXCEPTIONS.BRAND_NAME.eq(brandName))
						.and(TMB_BRAND_EXCEPTIONS.MANUFACTURER_NAME.eq(manufacturerName)))
				.execute();

		this.dslContext.update(TMB_BRAND_EXCLUSION_LIST).set(TMB_BRAND_EXCLUSION_LIST.BRAND_NAME, newBrandName)
				.where(TMB_BRAND_EXCLUSION_LIST.BRAND_ID.eq(brandId)
						.and(TMB_BRAND_EXCLUSION_LIST.BRAND_NAME.eq(brandName))
						.and(TMB_BRAND_EXCLUSION_LIST.MANUFACTURER_NAME.eq(manufacturerName)))
				.execute();

		this.dslContext.update(IDW_MANUFACTURER_MAP).set(IDW_MANUFACTURER_MAP.CIMM_BRAND_NAME, newBrandName)
				.where(IDW_MANUFACTURER_MAP.CIMM_BRAND_ID.eq(brandId)
						.and(IDW_MANUFACTURER_MAP.CIMM_BRAND_NAME.eq(brandName))
						.and(IDW_MANUFACTURER_MAP.CIMM_MFR_NAME.eq(manufacturerName)))
				.execute();

		this.dslContext.update(TMB_BRAND).set(TMB_BRAND.BRAND_NAME, newBrandName).where(TMB_BRAND.ID.eq(brandId))
				.execute();

		// Update items whose brand name is renamed
		this.dslContext.update(SI_CLIENT_ITEM).set(SI_CLIENT_ITEM.UPDATED_AT, DSL.currentTimestamp())
				.where(SI_CLIENT_ITEM.BRAND_ID.eq(brandId)).execute();
	}

	@Override
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void renameManufacturer(String manufacturerName, String newManufacturerName, ULong manufacturerId,
			ULong clientId) {
		this.dslContext.update(TmbManufacturer.TMB_MANUFACTURER)
				.set(TmbManufacturer.TMB_MANUFACTURER.MANUFACTURER_NAME, newManufacturerName)
				.where(TmbManufacturer.TMB_MANUFACTURER.MANUFACTURER_NAME.eq(manufacturerName)
						.and(TmbManufacturer.TMB_MANUFACTURER.ID.eq(manufacturerId)))
				.execute();

		this.dslContext.update(TmbManufacturerExceptions.TMB_MANUFACTURER_EXCEPTIONS)
				.set(TmbManufacturerExceptions.TMB_MANUFACTURER_EXCEPTIONS.MANUFACTURER_NAME, newManufacturerName)
				.where(TmbManufacturerExceptions.TMB_MANUFACTURER_EXCEPTIONS.MANUFACTURER_ID.eq(manufacturerId).and(
						TmbManufacturerExceptions.TMB_MANUFACTURER_EXCEPTIONS.MANUFACTURER_NAME.eq(manufacturerName)))
				.execute();

		this.dslContext.update(TmbManufacturerExclusionList.TMB_MANUFACTURER_EXCLUSION_LIST)
				.set(TmbManufacturerExclusionList.TMB_MANUFACTURER_EXCLUSION_LIST.MANUFACTURER_NAME, newManufacturerName)
				.where(TmbManufacturerExclusionList.TMB_MANUFACTURER_EXCLUSION_LIST.MANUFACTURER_ID.eq(manufacturerId)
						.and(TmbManufacturerExclusionList.TMB_MANUFACTURER_EXCLUSION_LIST.MANUFACTURER_NAME
								.eq(manufacturerName)))
				.execute();

		this.dslContext.update(IdwManufacturerMap.IDW_MANUFACTURER_MAP)
				.set(IdwManufacturerMap.IDW_MANUFACTURER_MAP.CIMM_MFR_NAME, newManufacturerName)
				.where(IdwManufacturerMap.IDW_MANUFACTURER_MAP.CIMM_MFR_ID.eq(Long.valueOf(manufacturerId.toString()))
						.and(IdwManufacturerMap.IDW_MANUFACTURER_MAP.CIMM_MFR_NAME.eq(manufacturerName)))
				.execute();

		List<ULong> brandIds = getBrandIdList(manufacturerId);

		// Update items whose manufacturer name is renamed
		this.dslContext.update(SiClientItem.SI_CLIENT_ITEM)
				.set(SiClientItem.SI_CLIENT_ITEM.UPDATED_AT, DSL.currentTimestamp())
				.where(SiClientItem.SI_CLIENT_ITEM.BRAND_ID.in(brandIds)).execute();

	}

	@Override
	public String getClientCode(ULong clientId) {
		return this.dslContext.select(IAM_CLIENT.CODE).from(IAM_CLIENT).where(IAM_CLIENT.ID.eq(clientId)).limit(1)
				.fetchOneInto(String.class);
	}

	@Override
	public List<ULong> getBrandIdList(ULong manufacturerId) {
		return this.dslContext.select(TmbBrand.TMB_BRAND.ID).from(TmbBrand.TMB_BRAND)
				.where(TmbBrand.TMB_BRAND.MANUFACTURER_ID.eq(manufacturerId)).fetchInto(ULong.class);
	}

	@Override
	public List<ULong> getManufacturerIdList(String manufacturerName, ULong clientId) {
		return this.dslContext.select(TmbManufacturer.TMB_MANUFACTURER.ID).from(TmbManufacturer.TMB_MANUFACTURER)
				.where(DSL.cast(TmbManufacturer.TMB_MANUFACTURER.MANUFACTURER_NAME, SQLDataType.BINARY)
						.eq(DSL.cast(manufacturerName, SQLDataType.BINARY))
						.and(TmbManufacturer.TMB_MANUFACTURER.CLIENT_ID.eq(clientId)))
				.fetchInto(ULong.class);
	}

	@Override
	public List<ULong> restrictedManufacturerIdList(ULong manufacturerId) {
		return this.dslContext.select(TMB_MANUFACTURER_EXCEPTIONS.ID).from(TMB_MANUFACTURER_EXCEPTIONS)
				.where(TMB_MANUFACTURER_EXCEPTIONS.MANUFACTURER_ID.eq(manufacturerId)).fetchInto(ULong.class);
	}

	@Override
	public List<ULong> exludedManufacturerIdList(ULong manufacturerId) {
		return this.dslContext.select(TMB_MANUFACTURER_EXCLUSION_LIST.ID).from(TMB_MANUFACTURER_EXCLUSION_LIST)
				.where(TMB_MANUFACTURER_EXCLUSION_LIST.MANUFACTURER_ID.eq(manufacturerId)).fetchInto(ULong.class);
	}

	@Override
	public Result<IdwManufacturerMapRecord> getIdwManufacturerMapRecord(Condition condition) {
		return this.dslContext.selectFrom(IDW_MANUFACTURER_MAP).where(condition).fetch();
	}

	@Override
	public void deleteManufacturer(ULong manufacturerId) {
		this.dslContext.deleteFrom(TmbManufacturer.TMB_MANUFACTURER)
				.where(TmbManufacturer.TMB_MANUFACTURER.ID.eq(manufacturerId)).execute();
	}

	@Override
	public List<ULong> getBrandIdListForManufacturer(String manufacturerName, ULong clientId, String brandName) {
		return this.dslContext.select(TMB_BRAND.ID).from(TMB_BRAND).innerJoin(TMB_MANUFACTURER)
				.on(TMB_BRAND.MANUFACTURER_ID.eq(TMB_MANUFACTURER.ID))
				.where(TMB_MANUFACTURER.MANUFACTURER_NAME.eq(manufacturerName)
						.and(TMB_MANUFACTURER.CLIENT_ID.eq(clientId))
						.and(DSL.cast(TMB_BRAND.BRAND_NAME, SQLDataType.BINARY)
								.eq(DSL.cast(brandName, SQLDataType.BINARY))))
				.fetchInto(ULong.class);
	}

	@Override
	public int isRestrictedBrand(ULong brandId, String manufacturerName, String brandName) {
		return this.dslContext.fetchCount(this.dslContext.selectFrom(TMB_BRAND_EXCEPTIONS)
				.where(TMB_BRAND_EXCEPTIONS.BRAND_ID.eq(brandId).and(TMB_BRAND_EXCEPTIONS.BRAND_NAME.eq(brandName))
						.and(TMB_BRAND_EXCEPTIONS.MANUFACTURER_NAME.eq(manufacturerName))));
	}

	@Override
	public int isExludedBrand(ULong brandId, String manufacturerName, String brandName) {
		return this.dslContext.fetchCount(this.dslContext.selectFrom(TMB_BRAND_EXCLUSION_LIST)
				.where(TMB_BRAND_EXCLUSION_LIST.BRAND_ID.eq(brandId)
						.and(TMB_BRAND_EXCLUSION_LIST.BRAND_NAME.eq(brandName))
						.and(TMB_BRAND_EXCLUSION_LIST.MANUFACTURER_NAME.eq(manufacturerName))));
	}

	@Override
	public Stream<Record3<ULong, String, String>> getBatchSubsetDetails() {
		SiSubsetDefinition parentTable = SI_SUBSET_DEFINITION.as("parent");
		return this.dslContext
				.select(SI_SUBSET_DEFINITION.ID, SI_SUBSET_DEFINITION.SCHEMA_NAME,
						SI_SUBSET_DEFINITION.ITEM_SHADOW_TABLE_NAME)
				.from(SI_SUBSET_DEFINITION).join(parentTable).on(SI_SUBSET_DEFINITION.PARENT_ID.eq(parentTable.ID))
				.where(SI_SUBSET_DEFINITION.PARENT_ID.eq(ULong.valueOf(1l))
						.or(parentTable.PARENT_ID.eq(ULong.valueOf(1l))))
				.fetchStream();
	}

	@Override
	public boolean isBrandModified(ULong brandId, Table<Record> e) {
		return this.dslContext.select(DSL.field("ID", ULong.class)).from(e)
				.where(DSL.field("BRAND_ID", ULong.class).eq(brandId)).limit(1).execute() == 0;
	}

	@Override
	public List<String> getItemIdList(ULong brandId, ULong clientId) {
		return this.dslContext.select(SI_CLIENT_ITEM.ID).from(SI_CLIENT_ITEM)
				.where(SI_CLIENT_ITEM.BRAND_ID.eq(brandId)
						.and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(getPublisherIdThruClientId(clientId))))
				.fetchInto(String.class);
	}

	@Override
	public Stream<Record3<ULong, String, String>> getSubsetsListForSubscriber(ULong brandId, ULong clientId) {
		return this.dslContext
				.select(SI_SUBSET_DEFINITION.ID, SI_SUBSET_DEFINITION.SCHEMA_NAME,
						SI_SUBSET_DEFINITION.ITEM_MERGED_TABLE_NAME)
				.from(SI_SUBSET_DEFINITION).where(SI_SUBSET_DEFINITION.CLIENT_ID.eq(clientId)).fetchStream();
	}

	@Override
	public void deleteBrand(ULong brandId) {
		this.dslContext.deleteFrom(TMB_BRAND).where(TMB_BRAND.ID.eq(brandId)).execute();
	}

	@Override
	public void checkForManufacturerCodeExists(String manufacturerCode, Set<ULong> clientIds) {
	
		if (manufacturerCode==null || manufacturerCode.isBlank() || manufacturerCode.isEmpty()) {
			throw new PrimeException(HttpStatus.BAD_REQUEST, PrimeResponseCode.BLANK_MANUFACTURER_CODE.getResponseMsg(),
					PrimeResponseCode.BLANK_MANUFACTURER_CODE.getResponseCode());
		}
		TmbManufacturerRecord record = this.dslContext.selectFrom(TMB_MANUFACTURER).where(
				TMB_MANUFACTURER.MANUFACTURER_CODE.eq(manufacturerCode).and(TMB_MANUFACTURER.CLIENT_ID.in(clientIds)))
				.fetchAny();
		if (record != null) {
			logger.debug("Unable to create manufacturer : Duplicate Resource");
			throw new PrimeException(HttpStatus.FORBIDDEN, PrimeResponseCode.DUPLICATE_MFR_CODE.getResponseMsg(),
					PrimeResponseCode.DUPLICATE_MFR_CODE.getResponseCode());
		}
	}

	@Override
	public void checkForManufacturerNameExists(ULong clientId, String mfrName) {
		TmbManufacturerRecord record = this.dslContext.selectFrom(TMB_MANUFACTURER)
				.where(TMB_MANUFACTURER.CLIENT_ID.eq(getPublisherClientIdThruClientId(clientId)))
				.or(TMB_MANUFACTURER.CLIENT_ID.eq(clientId)).and(TMB_MANUFACTURER.MANUFACTURER_NAME.eq(mfrName))
				.fetchAny();
		if (record != null) {
			logger.debug("Unable to create manufacturer : Duplicate Resource");
			throw new PrimeException(HttpStatus.FORBIDDEN, PrimeResponseCode.DUPLICATE_MFR_NAME.getResponseMsg(),
					PrimeResponseCode.DUPLICATE_MFR_NAME.getResponseCode());
		}
	}

	@Override
	public void checkForBrandCodeExists(ULong clientId, String brandCode) {
		if (brandCode == null || brandCode.isEmpty() || brandCode.isBlank()) {
			throw new PrimeException(HttpStatus.BAD_REQUEST, PrimeResponseCode.BLANK_BRAND_CODE.getResponseMsg());
		}
		Record tmbBrandRecord = dslContext.select(TmbBrand.TMB_BRAND.fields()).from(TmbBrand.TMB_BRAND)
				.innerJoin(TmbManufacturer.TMB_MANUFACTURER)
				.on(TmbBrand.TMB_BRAND.MANUFACTURER_ID.eq(TMB_MANUFACTURER.ID))
				.where(TmbManufacturer.TMB_MANUFACTURER.CLIENT_ID.eq(clientId))
				.and(TmbBrand.TMB_BRAND.BRAND_CODE.eq(brandCode)).fetchAny();
		if (tmbBrandRecord != null) {
			throw new PrimeException(HttpStatus.CONFLICT, PrimeResponseCode.BRAND_CODE_ALREADY_EXISTS.getResponseMsg());
		}
	}

	@Override
	public void checkForBrandNameExists(ULong clientId, String brandName,ULong manufacturerId) {
		Record tmbBrandRecord = dslContext.select(TmbBrand.TMB_BRAND.fields()).from(TmbBrand.TMB_BRAND)
				.innerJoin(TmbManufacturer.TMB_MANUFACTURER)
				.on(TmbBrand.TMB_BRAND.MANUFACTURER_ID.eq(TMB_MANUFACTURER.ID))
				.where(TmbManufacturer.TMB_MANUFACTURER.CLIENT_ID.eq(clientId))
				.and(TmbBrand.TMB_BRAND.BRAND_NAME.eq(brandName))
				.and(TmbBrand.TMB_BRAND.MANUFACTURER_ID.eq(manufacturerId)).fetchAny();

		if (tmbBrandRecord != null) {
			throw new PrimeException(HttpStatus.CONFLICT, PrimeResponseCode.DUPLICATE_BRAND_NAME.getResponseMsg());
		}
	}

	@Override
	public ULong getClientIdForPublisherId(ULong publisherId) {
		return this.dslContext.select(SI_PUBLISHER_SUBSCRIBER.CLIENT_ID).from(SI_PUBLISHER_SUBSCRIBER)
				.where(SI_PUBLISHER_SUBSCRIBER.PUBLISHER_ID.eq(publisherId)).limit(1).fetchOneInto(ULong.class);
	}

	@Override
	public ULong getPublisherClientIdThruClientId(ULong clientId) {
		String clientTypeCode = getClientTypeCode(clientId);
		if ("P".equals(clientTypeCode))
			return clientId;
		return this.dslContext.select(SI_PUBLISHER_CLIENT.CLIENT_ID).from(SI_PUBLISHER_CLIENT)
				.join(SI_PUBLISHER_SUBSCRIBER).on(SI_PUBLISHER_SUBSCRIBER.PUBLISHER_ID.eq(SI_PUBLISHER_CLIENT.ID))
				.where(SI_PUBLISHER_SUBSCRIBER.CLIENT_ID.eq(clientId)).limit(1).fetchOneInto(ULong.class);
	}

	@Override
	public ULong getPublisherIdThruClientId(ULong clientId) {
		return this.dslContext.select(SI_PUBLISHER_CLIENT.ID).from(SI_PUBLISHER_CLIENT)
				.where(SI_PUBLISHER_CLIENT.CLIENT_ID.eq(clientId)).limit(1).fetchOneInto(ULong.class);
	}

	@Override
	public String getClientTypeCode(ULong clientId) {
		return this.dslContext.select(IAM_CLIENT.CLIENT_TYPE_CODE).from(IAM_CLIENT).where(IAM_CLIENT.ID.eq(clientId))
				.limit(1).fetchOneInto(String.class);
	}

	@Override
	public void validateImage(String imageAddress) {		
		imageAddress=imageAddress.toLowerCase();		
		for(String str : INVALID_FILE_CHARACTERS)
			if(imageAddress.contains(str))
				throw new PrimeException(HttpStatus.FORBIDDEN, PrimeResponseCode.INVALID_FILE_NAME.getResponseMsg(),
						PrimeResponseCode.INVALID_FILE_NAME.getResponseCode());
		this.validateFileExtension(imageAddress);				
	}

	private void validateFileExtension(String imageAddress) {
		//Only files containing images in .JPG, .PNG, .TIFF, .GIF, .BMP, .SVG, .JPEG formats are accepted.
		for (String str : FILE_EXTENSIONS) {
			if (imageAddress.contains(str))
				return;
			else
				continue;
		}
		throw new PrimeException(HttpStatus.FORBIDDEN, PrimeResponseCode.INVALID_FILE_EXTENSION.getResponseMsg(),
				PrimeResponseCode.INVALID_FILE_EXTENSION.getResponseCode());
	}
	
	private SelectOrderByStep<Record1<ULong>> restrictedBrandRecordCondition(ULong clientId, ULong publisherId) {
		return getBrandJointStep(clientId, publisherId).unionAll(getMfrJointStep(clientId));
	}
	
	private SelectConditionStep<Record1<ULong>> getBrandJointStep(ULong clientId, ULong publisherId) {

		TmbBrandExclusionList a = TmbBrandExclusionList.TMB_BRAND_EXCLUSION_LIST.as("a");
		TmbBrand b = TmbBrand.TMB_BRAND.as("b");
		TmbBrandExceptions c = TmbBrandExceptions.TMB_BRAND_EXCEPTIONS.as("c");

		SelectConditionStep<Record1<ULong>> record = dslContext.select(b.ID).from(a).join(b)
				.on(a.MANUFACTURER_ID.eq(b.MANUFACTURER_ID).and(a.BRAND_ID.eq(b.ID)))
				.where(a.PUBLISHER_ID.eq(publisherId))
				.andNotExists(dslContext.select(c.ID).from(c).where(c.CLIENT_ID.eq(clientId))
						.and(c.MANUFACTURER_ID.eq(b.MANUFACTURER_ID)).and(c.BRAND_ID.eq(b.ID)));
		return record;
	}
	
	private SelectConditionStep<Record1<ULong>> getMfrJointStep(ULong clientId) {

		TmbBrand a = TmbBrand.TMB_BRAND.as("a");
		TmbManufacturerExclusionList b = TmbManufacturerExclusionList.TMB_MANUFACTURER_EXCLUSION_LIST.as("b");
		TmbManufacturer c = TmbManufacturer.TMB_MANUFACTURER.as("c");
		TmbManufacturerExceptions d = TmbManufacturerExceptions.TMB_MANUFACTURER_EXCEPTIONS.as("d");

		SelectConditionStep<Record1<ULong>> r1 = dslContext.select(c.ID).from(b).join(c).on(b.MANUFACTURER_ID.eq(c.ID))
				.whereNotExists(dslContext.select(d.ID).from(d).where(d.CLIENT_ID.eq(clientId))
						.and(d.MANUFACTURER_ID.eq(c.ID)));

		SelectConditionStep<Record1<ULong>> record = dslContext.select(a.ID).from(a).where(a.MANUFACTURER_ID.in(r1));
		return record;
	}
	

	@Override
	public Record2<String, String> getManufacturerCategoryMapping(ULong clientIds,String mfrName,String brandName, String manufacturerCategory) {
		return this.dslContext.select(TMB_MANUFACTURER_CATEGORY_MAPPING.CATEGORY_CODE,TMB_MANUFACTURER_CATEGORY_MAPPING.CX1_CATEGORY_NAME)
				.from(TMB_MANUFACTURER_CATEGORY_MAPPING).join(TMB_MANUFACTURER)
				.on(TMB_MANUFACTURER_CATEGORY_MAPPING.MANUFACTURER_ID.eq(TMB_MANUFACTURER.ID)).join(TMB_BRAND)
				.on(TMB_MANUFACTURER_CATEGORY_MAPPING.BRAND_ID.eq(TMB_BRAND.ID)).where(
						TMB_MANUFACTURER_CATEGORY_MAPPING.CLIENT_ID.in(clientIds)
								.and(TMB_MANUFACTURER.MANUFACTURER_NAME.eq(mfrName))
								.and(TMB_BRAND.BRAND_NAME.eq(brandName))
								.and(TMB_MANUFACTURER_CATEGORY_MAPPING.EXTERNAL_MANUFACTURER_CATEGORY
										.eq(manufacturerCategory)))
				.fetchOne();
	}

	@Override
	public void deleteManufacturerCategoryMapping(Condition condition) {
		this.dslContext.delete(TmbManufacturerCategoryMapping.TMB_MANUFACTURER_CATEGORY_MAPPING)
		.where(condition)
		.execute();		
	}


}
