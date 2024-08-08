package com.unilog.cx1.pim.commons.service.impl;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Maps.newHashMap;
import static com.google.common.collect.Maps.newTreeMap;
import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.ENHANCED;
import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.IS_IDW_ITEM;
import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.ITEM_IMPORT_REQUIRED_SERVICES_NAMES;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.APPLICATION;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.BRAND_CODE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.BRAND_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.CLIENT_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.COUNTRY_OF_ORIGIN;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.COUNTRY_OF_SALE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.CREATED_AT;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.CREATED_BY;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.DATA_SOURCE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.DB_STANDARD_APPROVALS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.EAN_UCC13;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ENRICHED_INDICATOR;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.FEATURE_BULLETS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.FROM_EXCEL_COLUMNS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.GENERIC_FROM_EXCEL_COLUMNS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.GENERIC_TO_DB_COLUMNS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.GTIN;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.HEIGHT;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.HEIGHT_UOM;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.IDW_ITEM_PASS_THROUGH_FIELDS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.IDW_ITEM_UPDATABLE_FIELDS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.INCLUDES;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.INVC_DESCRIPTION;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ITEM_DB_FIELDS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ITEM_FEATURES;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.LENGTH;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.LENGTH_UOM;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.LONG_DESCRIPTION;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.LONG_DESCRIPTION_2;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MANUFACTURER_STATUS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MFR_PART_NUMBER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MIN_ORDER_QTY;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MRKT_DESCRIPTION;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MY_PART_NUMBER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ORDER_QTY_INTERVAL;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PACKAGE_QTY;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PACK_DESCRIPTION;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PAGE_TITLE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER_HASH;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER_PREFIX;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER_SUFFIX;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PRINT_DESCRIPTION;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PROGRAM_PART_NUMBER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PUBLISHER_ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.QTY_AVAILABLE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.RECORD_STATUS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.SALES_UOM;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.SHORT_DESCRIPTION;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.TO_DB_COLUMNS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.UNSPSC;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.UOM;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.UPC;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.UPDATED_AT;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.UPDATED_BY;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.VOLUME;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.VOLUME_UOM;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.WARRANTY;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.WEIGHT;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.WEIGHT_UOM;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.WIDTH;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.WIDTH_UOM;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryTableType.ITEM;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryViewType.BASE;
import static com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryViewType.MERGE;
import static com.unilog.iam.jooq.tables.IamClient.IAM_CLIENT;
import static com.unilog.iam.jooq.tables.IamUser.IAM_USER;
import static com.unilog.iam.jooq.tables.SiPublisherClient.SI_PUBLISHER_CLIENT;
import static com.unilog.iam.jooq.tables.SiPublisherSubscriber.SI_PUBLISHER_SUBSCRIBER;
import static com.unilog.prime.commons.util.BooleanUtil.convertToBoolean;
import static com.unilog.prime.commons.util.StringUtil.safeValueOf;
import static com.unilog.prime.csr.jooq.tables.CsrPmitempartnumberExclusion.CSR_PMITEMPARTNUMBER_EXCLUSION;
import static com.unilog.prime.csr.jooq.tables.CsrPreferredManufacturer.CSR_PREFERRED_MANUFACTURER;
import static com.unilog.prime.idw.jooq.tables.Tbitem.TBITEM;
import static com.unilog.prime.jooq.tables.IdwPartnumberMap.IDW_PARTNUMBER_MAP;
import static com.unilog.prime.jooq.tables.SiClientItem.SI_CLIENT_ITEM;
import static com.unilog.prime.jooq.tables.SiItemProperties.SI_ITEM_PROPERTIES;
import static com.unilog.prime.jooq.tables.SiItemUpdateDetails.SI_ITEM_UPDATE_DETAILS;
import static com.unilog.prime.jooq.tables.SiManufacturerPrice.SI_MANUFACTURER_PRICE;
import static com.unilog.prime.jooq.tables.SiSubsetDefinition.SI_SUBSET_DEFINITION;
import static com.unilog.prime.jooq.tables.TmbBrand.TMB_BRAND;
import static com.unilog.prime.jooq.tables.TmbEnrichedIndicatorStatus.TMB_ENRICHED_INDICATOR_STATUS;
import static com.unilog.prime.jooq.tables.TmbManufacturer.TMB_MANUFACTURER;
import static com.unilog.prime.jooq.tables.TmbManufacturerStatus.TMB_MANUFACTURER_STATUS;
import static java.util.Map.entry;
import static java.util.Map.ofEntries;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.jooq.impl.DSL.currentTimestamp;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.name;
import static org.jooq.impl.DSL.notExists;
import static org.jooq.impl.DSL.table;
import static org.jooq.impl.DSL.timestamp;
import static org.jooq.util.mysql.MySQLDSL.values;
import static org.springframework.transaction.annotation.Isolation.READ_COMMITTED;
import static org.springframework.transaction.annotation.Isolation.READ_UNCOMMITTED;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.catalina.security.SecurityUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.InsertValuesStepN;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record4;
import org.jooq.Result;
import org.jooq.Table;
import org.jooq.UpdateConditionStep;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.MappingException;
import org.jooq.impl.DSL;
import org.jooq.types.ULong;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.unilog.cx1.pim.commons.constants.IdwCountryCode;
import com.unilog.cx1.pim.commons.enumeration.SiSubsetQueryViewType;
import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;
import com.unilog.cx1.pim.commons.model.recordtype.CatalogNonSubscribedRecord;
import com.unilog.cx1.pim.commons.model.recordtype.ParentRecord;
import com.unilog.cx1.pim.commons.model.recordtype.PrivateRecord;
import com.unilog.cx1.pim.commons.model.recordtype.PublisherNonSubscribedRecord;
import com.unilog.cx1.pim.commons.model.recordtype.PublisherRecord;
import com.unilog.cx1.pim.commons.model.recordtype.RecordType;
import com.unilog.cx1.pim.commons.service.IItemService;
import com.unilog.iam.jooq.tables.ClientNotificationConfiguration;
import com.unilog.iam.jooq.tables.records.ClientNotificationConfigurationRecord;
import com.unilog.prime.commons.model.SourceFormat;
import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.commons.util.TextUtil;
import com.unilog.prime.jooq.Routines;
import com.unilog.prime.jooq.enums.SiItemUpdateDetailsStatus;
import com.unilog.prime.jooq.enums.SiSubsetDefinitionSubsetType;
import com.unilog.prime.jooq.tables.GlobalAttributeFilterableDefinition;
import com.unilog.prime.jooq.tables.GlobalAttributeFilterableValues;
import com.unilog.prime.jooq.tables.LuGlobalFilterableAttributes;
import com.unilog.prime.jooq.tables.SiManufacturerPrice;
import com.unilog.prime.jooq.tables.SiSubsetQuery;
import com.unilog.prime.jooq.tables.records.GlobalAttributeFilterableDefinitionRecord;
import com.unilog.prime.jooq.tables.records.GlobalAttributeFilterableValuesRecord;
import com.unilog.prime.jooq.tables.records.LuGlobalFilterableAttributesRecord;
import com.unilog.prime.jooq.tables.records.SiClientItemRecord;
import com.unilog.prime.jooq.tables.records.SiSubsetDefinitionRecord;

@Service(ItemServiceImpl.BEAN_ID)
public class ItemServiceImpl extends AbstractImportService implements IItemService {

    public static final String BEAN_ID = "etlItemService";
    private final List<String> STRING_DELETABLE_FIELDS = ImmutableList.of(MFR_PART_NUMBER, MY_PART_NUMBER, UPC,
            VOLUME_UOM, PACKAGE + UOM, COUNTRY_OF_ORIGIN, COUNTRY_OF_SALE, GTIN, EAN_UCC13, SHORT_DESCRIPTION, LONG_DESCRIPTION,
            LONG_DESCRIPTION_2, MRKT_DESCRIPTION, FEATURE_BULLETS, INVC_DESCRIPTION, PRINT_DESCRIPTION, PACK_DESCRIPTION,
            PACKAGE + LENGTH_UOM, PACKAGE + HEIGHT_UOM, PACKAGE + WEIGHT_UOM, PACKAGE + WIDTH_UOM, SALES_UOM, UNSPSC,
            DB_STANDARD_APPROVALS, APPLICATION, PAGE_TITLE, INCLUDES, WARRANTY, MANUFACTURER_STATUS, EAN_UCC13, GTIN);
    private final List<String> NUMERIC_DELETABLE_FIELDS = ImmutableList.of(QTY_AVAILABLE, MIN_ORDER_QTY, PACKAGE_QTY,
            ORDER_QTY_INTERVAL, PACKAGE + LENGTH, PACKAGE + HEIGHT, PACKAGE + WEIGHT, PACKAGE + WIDTH, VOLUME);
    private static final String PACKAGE = "PACKAGE_";
    public static final List<String> ITEM_TABLE_FIELDS = ImmutableList.of(ID, PART_NUMBER_HASH, PART_NUMBER,
            PART_NUMBER_PREFIX, PART_NUMBER_SUFFIX, BRAND_ID, MFR_PART_NUMBER, MY_PART_NUMBER, UPC, VOLUME_UOM,
            COUNTRY_OF_ORIGIN, COUNTRY_OF_SALE, SHORT_DESCRIPTION, LONG_DESCRIPTION, LONG_DESCRIPTION_2,
            MRKT_DESCRIPTION, FEATURE_BULLETS, INVC_DESCRIPTION, PACK_DESCRIPTION, QTY_AVAILABLE, MIN_ORDER_QTY,
            PACKAGE_QTY, ORDER_QTY_INTERVAL, PACKAGE + LENGTH, PACKAGE + HEIGHT, PACKAGE + WEIGHT, PACKAGE + WIDTH,
            VOLUME, PACKAGE + LENGTH_UOM, PACKAGE + HEIGHT_UOM, PACKAGE + WEIGHT_UOM, PACKAGE + WIDTH_UOM, SALES_UOM,
            UNSPSC, DB_STANDARD_APPROVALS, APPLICATION, ENRICHED_INDICATOR, MANUFACTURER_STATUS, INCLUDES, WARRANTY, EAN_UCC13, GTIN);
    private final List<String> IDW_UOM_FIELDS = ImmutableList.of(PACKAGE + LENGTH_UOM,
            PACKAGE + HEIGHT_UOM, PACKAGE + WEIGHT_UOM, PACKAGE + WIDTH_UOM,
            LENGTH_UOM, HEIGHT_UOM, WEIGHT_UOM, WIDTH_UOM);
    public Map<String, String> IDW_UOM_MAPPING_TO_AD = ofEntries(
            entry("IN", "in"), entry("L", "lb"));
    private static final String QUERY_WITH_PART_NUMBER_SATRT = "a1.PART_NUMBER IN ('";
    private static final String QUERY_WITH_PART_NUMBER_END = "')";
    
    @Autowired
    private XTableFieldServiceImpl xTableFieldServiceImpl;
    
    @Override
    @Cacheable(value = "SubsetDefinition", key = "{#executionId, #subsetId}")
    public SiSubsetDefinitionRecord getSubsetDefinition(ULong executionId, ULong subsetId) {

        return this.dslContext.selectFrom(SI_SUBSET_DEFINITION).where(SI_SUBSET_DEFINITION.ID.eq(subsetId)).fetchOne();
    }

    @Override
    @Cacheable(value = "PublisherClient", key = "{#executionId, #clientId}", unless = "#result == null")
    public ULong getPublisherIdForClientId(ULong executionId, ULong clientId) {
        ULong publisherId = this.dslContext.select(SI_PUBLISHER_CLIENT.ID).from(SI_PUBLISHER_CLIENT)
                .where(SI_PUBLISHER_CLIENT.CLIENT_ID.eq(clientId)).limit(1).fetchOneInto(ULong.class);

        if (publisherId == null)
            return this.dslContext.select(SI_PUBLISHER_SUBSCRIBER.PUBLISHER_ID).from(SI_PUBLISHER_SUBSCRIBER)
                    .where(SI_PUBLISHER_SUBSCRIBER.CLIENT_ID.eq(clientId)).limit(1).fetchOneInto(ULong.class);

        return publisherId;
    }

    @SuppressWarnings("unchecked")
	@Override
    @Transactional(isolation = READ_COMMITTED)
	public void doImport(Import anImport, PimDataObject qdo, boolean isItemImportWithReplace,boolean isGenericImport) {
		Tuple<Record, Record> existingRecord = getExistingRecord(anImport, qdo);
		Map<String, Object> parentR = existingRecord.getFirstValue() == null ? newTreeMap()
				: existingRecord.getFirstValue().intoMap();
		Map<String, Object> shadowR = existingRecord.getSecondValue() == null ? newTreeMap()
				: existingRecord.getSecondValue().intoMap();
		
		this.validateValidValue(qdo);
		
		if(!isItemImportWithReplace) {
			ITEM_TABLE_FIELDS.forEach(e -> {
			    if(qdo.getData().containsKey(e) && qdo.getData().get(e) == null && shadowR.get(e) != null
			    		&& shadowR.get(e).toString().isEmpty()) { qdo.getData().put(e, "");}
			    else if(qdo.getData().containsKey(e) && qdo.getData().get(e) == null && shadowR.get(e) != null
			    		&& (shadowR.get(e).toString().equals("-1.0") || shadowR.get(e).toString().equals("-1"))) 
			    	{qdo.getData().put(e, "-1");}
			    
			    });
		}

		Map<String, Object> newR = getNewImportRec(qdo, parentR, anImport,isGenericImport);
		
		Record xField =xTableFieldServiceImpl.getXField("DATE_STATUS_CHANGE", qdo.getClientId(), null);
		if(xField!=null && ((parentR.isEmpty() && newR.get(MANUFACTURER_STATUS)!=null)
				|| (parentR.get(MANUFACTURER_STATUS) != null && !parentR.get(MANUFACTURER_STATUS).equals(newR.get(MANUFACTURER_STATUS))))) {
	       qdo.getData().put("EX_DATE_STATUS_CHANGE", LocalDate.now().format(DateTimeFormatter.ofPattern("dd-MMM-yyyy")).toUpperCase());
			String delimitedRequiredServices = qdo.getValue(ITEM_IMPORT_REQUIRED_SERVICES_NAMES);
			if(delimitedRequiredServices!=null && !delimitedRequiredServices.contains("xField"))
				qdo.getData().put(ITEM_IMPORT_REQUIRED_SERVICES_NAMES, delimitedRequiredServices.concat(",xField"));}
		
		if (!isItemImportWithReplace)
			anImport.filterRecordValues(newR, parentR, shadowR, STRING_DELETABLE_FIELDS, NUMERIC_DELETABLE_FIELDS,
					false);
		
		Integer value = anImport.subset.isExternal() ? null : -1;

		Map<String, Object> data = qdo.getData();
		if (data.get(PACKAGE + LENGTH) != null && 0.0 == Float.valueOf(data.get(PACKAGE + LENGTH).toString()))
			this.putSetDimensionsValuesToNull(newR, PACKAGE + LENGTH, PACKAGE + LENGTH_UOM, value);
		if (data.get(VOLUME) != null && 0.0 == Float.valueOf(data.get(VOLUME).toString()))
			this.putSetDimensionsValuesToNull(newR, VOLUME, VOLUME_UOM, value);
		if (data.get(PACKAGE + WIDTH) != null && 0.0 == Float.valueOf(data.get(PACKAGE + WIDTH).toString()))
			this.putSetDimensionsValuesToNull(newR, PACKAGE + WIDTH, PACKAGE + WIDTH_UOM, value);
		if (data.get(PACKAGE + WEIGHT) != null && 0.0 == Float.valueOf(data.get(PACKAGE + WEIGHT).toString()))
			this.putSetDimensionsValuesToNull(newR, PACKAGE + WEIGHT, PACKAGE + WEIGHT_UOM, value);
		if (data.get(PACKAGE + HEIGHT) != null && 0.0 == Float.valueOf(data.get(PACKAGE + HEIGHT).toString()))
			this.putSetDimensionsValuesToNull(newR, PACKAGE + HEIGHT, PACKAGE + HEIGHT_UOM, value);		
		if (newR.size() > 0) {
            if (anImport.subset.isExternal() && !newR.containsKey(BRAND_ID)) {
                newR.put(BRAND_ID, parentR.get(BRAND_ID));
            }
			List<String> mandatoryFields = newArrayList(ID, PART_NUMBER_HASH, PART_NUMBER_PREFIX, PART_NUMBER_SUFFIX,
					CREATED_BY, UPDATED_BY, PART_NUMBER);
			mandatoryFields.stream().forEach(r -> newR.put(r, qdo.getValue(r)));
			if (parentR.get(CREATED_AT) != null)
				newR.put(CREATED_AT, parentR.get(CREATED_AT));
			anImport.persistToDb(newArrayList(new Tuple<>(newR, parentR)), null, anImport.subset.getItemInsertTable(),
					qdo);
		}

	}

	private void putSetDimensionsValuesToNull(Map<String, Object> newR, String dimension, String uom, Integer value) {
		newR.put(dimension, value);
		newR.put(uom, value == null ? null : " ".trim());
	}

	private Map<String, Object> getNewImportRec(PimDataObject qdo, Map<String, Object> oldR, Import anImport,boolean isGenericImport) {
        List<String> insertFields = newArrayList();
        SourceFormat sourceFormat = qdo.getSourceFormat();
        
        List<String> dbColumns = isGenericImport ? GENERIC_TO_DB_COLUMNS : TO_DB_COLUMNS;
		List<String> excelColumns = isGenericImport ? GENERIC_FROM_EXCEL_COLUMNS : FROM_EXCEL_COLUMNS;
        
        List<String> header = null;
        if (anImport.isPartialImport())
            header = sourceFormat.getHeader();
        else
            header = newArrayList(ITEM_TABLE_FIELDS);
        for (String field : header) {
            if (dbColumns.contains(field)) {
                insertFields.add(field);
            } else if (excelColumns.contains(field)) {
                String mappedColumn = dbColumns.get(excelColumns.indexOf(field));
                insertFields.add(mappedColumn);
            } else if (ITEM_DB_FIELDS.contains(field)) {
                insertFields.add(field);
            } else if (StringUtils.equals(field, ITEM_FEATURES + 1) || StringUtils.equals(field, FEATURE_BULLETS)) {
                insertFields.add(FEATURE_BULLETS);
			} else if (isGenericImport && anImport.isPartialImport() && field.equals(BRAND_CODE)) {
				insertFields.add(BRAND_ID);
			}
		}
        processIdwFields(qdo, insertFields, oldR);
        Map<String, Object> newR = Maps.newHashMap();
        insertFields.forEach(r -> newR.put(r, qdo.getData().get(r)));
        return newR;
    }


    private void processIdwFields(PimDataObject qdo, List<String> updateAbleFields, Map<String, Object> oldR) {
        if (convertToBoolean(qdo.getValue(IS_IDW_ITEM))) {
            updateAbleFields.removeIf(e -> !IDW_ITEM_UPDATABLE_FIELDS.contains(e));
            String enrichIndicator = safeValueOf(oldR.get(ENRICHED_INDICATOR));
            if (!equalsIgnoreCase(enrichIndicator, ENHANCED))
                updateAbleFields.add(BRAND_ID);
            updateIdwPassThoughFieldIfValueBlank(oldR, updateAbleFields, qdo.getData());
            convertIdwUomToAdStandard(qdo);
            convertIdwCountryToAdStandard(qdo);
        }
    }

    private void convertIdwCountryToAdStandard(PimDataObject qdo) {
        String coo = qdo.getValue(COUNTRY_OF_ORIGIN);
        if (isNotBlank(coo) && coo.length() == 3) {
            String twoLetterCoo = IdwCountryCode.COUNTRY_CODE.get(coo);
            if (isNotBlank(twoLetterCoo))
                qdo.getData().put(COUNTRY_OF_ORIGIN, twoLetterCoo);
            else
                throw new IllegalArgumentException("Two letter country code not found for key : " + coo);
        }
    }

    private void convertIdwUomToAdStandard(PimDataObject qdo) {
        IDW_UOM_FIELDS.stream().forEach(r -> {
            String value = qdo.getValue(r);
            if (isNotBlank(value)) {
                String adUom = IDW_UOM_MAPPING_TO_AD.get(value);
                if (isNotBlank(adUom))
                    qdo.getData().put(r, adUom);
            }
        });
    }

    private void updateIdwPassThoughFieldIfValueBlank(Map<String, Object> oldR, List<String> updateAbleFields,
                                                      Map<String, Object> data) {
        if (!StringUtils.equalsIgnoreCase(safeValueOf(oldR.get(ENRICHED_INDICATOR)), "Enhanced")) {
            List<String> collect = IDW_ITEM_PASS_THROUGH_FIELDS.stream()
                    .filter(f -> isBlank(safeValueOf(oldR.get(f))))
                    .filter(f -> isNotBlank(safeValueOf(data.get(f)))).collect(Collectors.toList());
            if (isNotEmpty(collect))
                updateAbleFields.addAll(collect);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	private Tuple<Record, Record> getExistingRecord(Import anImport, PimDataObject qdo) {
        String itemId = qdo.getValue(ID);
        if (anImport.subset.isExternal()) {
            ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
            SiClientItemRecord publisherR = dslContext.selectFrom(SI_CLIENT_ITEM).where(SI_CLIENT_ITEM.ID
                    .eq(itemId).and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(publisherId))).fetchOne();
            return new Tuple(publisherR, null);
        } else if (anImport.subset.isWorkspace()) {
            Record shadowR = dslContext.selectFrom(anImport.subset.getItemInsertTable())
                    .where(field(PART_NUMBER).eq(qdo.getValue(PART_NUMBER))).fetchOne();
            if (anImport.subset.isContentWorkspace()) {
                SiSubsetDefinitionRecord catalogSubset = itemService.getSubsetDefinition(qdo.getExecutionId(), anImport.subset.definition.getParentId());
                Record catalogRecord = dslContext.fetchOne(subsetService.getQuery(qdo.getExecutionId(), catalogSubset, ITEM, MERGE), itemId);

                ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
                SiClientItemRecord publisherRecord = dslContext.selectFrom(SI_CLIENT_ITEM).where(SI_CLIENT_ITEM.ID
                        .eq(itemId).and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(publisherId))).fetchOne();
                boolean publisherRecordExists = dslContext.fetchExists(
                        dslContext.selectFrom(table(name("prime", "publisherdata1_precompute_table")))
                                .where(field("ID").eq(itemId).and(field("PUBLISHER_ID").eq(publisherId))));
                if(!publisherRecordExists && publisherRecord != null)
                    throw new IllegalArgumentException("The Master Catalog was being refreshed during the initial import. Please try again after the Master Catalog is fully refreshed.");

                if (catalogRecord == null) {
                    return new Tuple(publisherRecord, shadowR);
                }
                return new Tuple(catalogRecord, shadowR);
            }
            Record parentRecord = dslContext.fetchOne(subsetService.getQuery(qdo.getExecutionId(), anImport.subset.definition, ITEM, BASE), itemId);
            return new Tuple(parentRecord, shadowR);
        }
        return new Tuple();
    }


    @Override
    @Cacheable(value = "PreferredManufacturer", key = "{#manufacturer, #clientId}")
	public String getPreferredManufacturer(String manufacturer, ULong clientId) {
		return this.dslContext.select(TMB_MANUFACTURER.MANUFACTURER_NAME).from(TMB_MANUFACTURER)
				.join(CSR_PREFERRED_MANUFACTURER).on(CSR_PREFERRED_MANUFACTURER.MANUFACTURER_ID.eq(TMB_MANUFACTURER.ID))
				.where(TMB_MANUFACTURER.MANUFACTURER_NAME.eq(manufacturer)
						.and(TMB_MANUFACTURER.CLIENT_ID.eq(getPublisherClientIdThruClientId(clientId))))
				.limit(1).fetchOneInto(String.class);
	}

    @Override
    public String getPfrdSupplierExcItem(String partnumber) {
        return this.dslContext.select(CSR_PMITEMPARTNUMBER_EXCLUSION.PART_NUMBER).from(CSR_PMITEMPARTNUMBER_EXCLUSION)
                .where(CSR_PMITEMPARTNUMBER_EXCLUSION.PART_NUMBER.eq(partnumber)).limit(1).fetchOneInto(String.class);
    }

    @Override
    public Timestamp createDummyShadowRecord(SiSubsetDefinitionRecord definition, PimDataObject qdo) {
        Map<String, Object> data = qdo.getData();
        String itemId = data.get(ID).toString();
        String whereClause = "ID = '" + itemId + "'";
        if (data.get(PART_NUMBER) == null)
            data.put(PART_NUMBER, data.get(PROGRAM_PART_NUMBER));
        Record shadowRecord = this.dslContext.select()
                .from(table(name(definition.getSchemaName(), definition.getItemShadowTableName()))).where(whereClause)
                .limit(1).fetchOne();
        Timestamp updatedAt = Timestamp.from(Instant.now());
        if (shadowRecord == null) {
            List<Field<?>> insertFields = new ArrayList<>();
            insertFields.add(field(ID));
            insertFields.add(field(PART_NUMBER));
            insertFields.add(field(PART_NUMBER_HASH));
            insertFields.add(field(RECORD_STATUS));
            insertFields.add(field(UPDATED_AT));
            if (data.get(CREATED_AT) != null)
                insertFields.add(field(CREATED_AT));
            insertFields.add(field(RECORD_STATUS));
            insertFields.add(field(UPDATED_BY));
            data.put(ID, itemId);
            data.put(RECORD_STATUS, "C");
            data.put(UPDATED_AT, updatedAt);
            data.put(UPDATED_BY, qdo.getUserId());
            this.dslContext.insertInto(table(name(definition.getSchemaName(), definition.getItemShadowTableName())))
                    .columns(insertFields)
                    .values(insertFields.stream().map(Field::getName).map(data::get).collect(Collectors.toList()))
                    .onDuplicateKeyIgnore().execute();
        } else {
            this.dslContext.update(table(name(definition.getSchemaName(), definition.getItemShadowTableName())))
                    .set(field(RECORD_STATUS), "C")
                    .set(field(UPDATED_AT), updatedAt)
                    .where(whereClause).execute();
        }
        
        return updatedAt;
    }

    @Override
    public List<Map<String, Object>> getItemPrice(String partNumber, ULong clientId, ULong executionId) {

        SiManufacturerPrice a = SI_MANUFACTURER_PRICE.as("a");
        SiManufacturerPrice b = SI_MANUFACTURER_PRICE.as("b");
        return this.dslContext
                .selectFrom(a).where(
                        a.PART_NUMBER.eq(partNumber).and(a.CLIENT_ID.in(itemService.getMyAllowedClientIds(clientId, executionId)))
                                .and(currentTimestamp().between(a.START_DATE, timestamp("9999-12-31 23:59:59"))
                                        .and(notExists(this.dslContext.selectFrom(b)
                                                .where(b.ID.ne(a.ID).and(b.PART_NUMBER.eq(a.PART_NUMBER)
                                                        .and(b.LOCALE.eq(a.LOCALE).and(currentTimestamp()
                                                                .between(b.START_DATE, timestamp("9999-12-31 23:59:59"))
                                                                .and(b.START_DATE.gt(a.START_DATE)
                                                                        .or(b.START_DATE.eq(a.START_DATE).and(
                                                                                b.CREATED_AT.gt(a.CREATED_AT))))))))))))
                .orderBy(a.LOCALE.desc()).fetchMaps();
    }


    @Override
    public void updateItemCreatedAt(SiSubsetDefinitionRecord definition, Map<String, Object> data) {
        dslContext.update(table(name(definition.getSchemaName(), definition.getItemTableName())))
                .set(field(CREATED_AT), data.get(CREATED_AT))
                .where(field(PART_NUMBER).eq(data.get(PART_NUMBER)), field(PUBLISHER_ID).eq(data.get(PUBLISHER_ID)))
                .execute();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
    @Transactional
	public void doItemPropertiesImport(Import anImport, PimDataObject qdo,
			Tuple<Map<String, Object>, Condition> tuple) {
		if (!anImport.getUserUtil().isPublisher())
			return;
		ULong publisherId = (ULong) qdo.getData().get(PUBLISHER_ID);
		if (publisherId != null) {
			Record existingRec = dslContext
					.selectFrom(anImport.subset.getItemPropertiesTable()).where(SI_ITEM_PROPERTIES.PART_NUMBER
							.eq(qdo.getValue(PART_NUMBER)).and(SI_ITEM_PROPERTIES.CLIENT_ID.eq(qdo.getClientId())))
					.fetchOne();
			Map<String, Object> oldR = existingRec == null ? newTreeMap() : existingRec.intoMap();
			Map<String, Object> newR = tuple.getFirstValue();
			anImport.filterRecordValues(newR, oldR, ImmutableList.of(DATA_SOURCE), ImmutableList.of(), false);
			if (!newR.isEmpty() || tuple.getSecondValue() != null) {
				if (!newR.isEmpty()) {
					newR.put(PART_NUMBER, qdo.getValue(PART_NUMBER));
					newR.put(CLIENT_ID, qdo.getClientId());
					newR.put(UPDATED_AT, DSL.currentTimestamp());
					if (isNotBlank(safeValueOf(oldR.get(ID))))
						newR.putIfAbsent(ID, oldR.get(ID));
				}
				if (tuple.getFirstValue().get(DATA_SOURCE) != null
						&& "".equals(tuple.getFirstValue().get(DATA_SOURCE).toString()))
					newR.put(DATA_SOURCE, null);

				anImport.persistToGlobalTable(newArrayList(new Tuple(newR, oldR)), tuple.getSecondValue(),
						anImport.subset.getItemPropertiesTable(), qdo);
			}
		}
	}

    @Override
    public String getAdPartnumber(String nItemId) {
        Record record = dslContext.selectFrom(table(name("prime", "idw_partnumber_map")))
                .where(field("EXTERNAL_SYSTEM_ITEM_ID").eq(nItemId)).limit(1).fetchOne();
        return record != null ? record.get("PART_NUMBER").toString() : null;
    }

    @Override
    public void insertIdwAdPartnumberMapping(Object nItemId, Object partNumber) {
        dslContext.insertInto(table(name("prime", "idw_partnumber_map"))).set(field("EXTERNAL_SYSTEM_ITEM_ID"), nItemId)
                .set(field("PART_NUMBER"), partNumber).onDuplicateKeyIgnore().execute();
    }

    @Override
    @Cacheable(value = "ItemRecord", key = "{#executionId, #publisherId, #partNumber}", unless = "#result == null")
    public Record getItemRecord(SiSubsetDefinitionRecord definition, String partNumber, ULong executionId,
                                ULong publisherId) {
        if (definition.getSubsetType() == SiSubsetDefinitionSubsetType.E) {
            Validate.notNull(publisherId, "Something is wrong, you need to be publisher to execute the operation");
            return dslContext.selectFrom(SI_CLIENT_ITEM)
                    .where(SI_CLIENT_ITEM.PART_NUMBER.eq(partNumber).and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(publisherId)))
                    .fetchOne();
        } else {
			Record itemRecord = dslContext.fetchOne(
					subsetService.getItemQueryWithPnOrMpn(executionId, definition, ITEM, MERGE),
					DSL.sql(QUERY_WITH_PART_NUMBER_SATRT + partNumber + QUERY_WITH_PART_NUMBER_END));
            if (itemRecord == null && convertToBoolean(definition.getZeroMembershipAllowed())) {
                SiSubsetDefinitionRecord parentSubSet = itemService.getSubsetDefinition(executionId, definition.getParentId());
				itemRecord = dslContext.fetchOne(
						subsetService.getItemQueryWithPnOrMpn(executionId, parentSubSet, ITEM, MERGE),
						DSL.sql(QUERY_WITH_PART_NUMBER_SATRT + partNumber + QUERY_WITH_PART_NUMBER_END));
                if (itemRecord == null) {
                    Validate.notNull(publisherId, "Something is wrong, you need to be publisher to execute the operation");
                    parentSubSet = itemService.getSubsetDefinition(executionId, parentSubSet.getParentId());
                    if (parentSubSet.getSubsetType() == SiSubsetDefinitionSubsetType.E) {
                        itemRecord = dslContext.selectFrom(SI_CLIENT_ITEM).where(SI_CLIENT_ITEM.PART_NUMBER
                                .eq(partNumber).and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(publisherId))).fetchOne();
                    }
                }
            }
            return itemRecord;
        }
     }
   	
	public Record getItemRecordFromItemHistory(SiSubsetDefinitionRecord definition, String partNumber, ULong executionId,
			ULong publisherId) {
				
		Record1<String> fetchOne = dslContext.select(field("ITEM_HISTORY", String.class)).from(SiSubsetQuery.SI_SUBSET_QUERY)
				.where(SiSubsetQuery.SI_SUBSET_QUERY.SUBSET_ID.eq(definition.getId()).and(SiSubsetQuery.SI_SUBSET_QUERY.TABLE_TYPE
					.eq("ITEM").and(SiSubsetQuery.SI_SUBSET_QUERY.VIEW_TYPE.eq("EXPORT"))))
				.fetchOne();
		if (fetchOne != null && fetchOne.size() > 0) {
		String query = fetchOne.value1();
		query=query.replaceAll("a.ID IN \\(###REPLACE###\\)", "{0}");
			
		Record itemRecord = dslContext.fetchOne(
				query,
				DSL.sql("a.PART_NUMBER IN ('" + partNumber + QUERY_WITH_PART_NUMBER_END));
			 
		 if(itemRecord != null)
			 return itemRecord;
		 else 
			 return null;
		}
		else {
			throw new IllegalArgumentException("ITEM_HISTORY Query not found, SubsetId = "+ definition.getId());
		}
	}

    public boolean isItemExistsInCatalogOrExternal(ULong executionId, SiSubsetDefinitionRecord subset,
                                                   String partNumber, ULong publisherId) {
        SiSubsetDefinitionRecord catalogSubset = subsetService.getSubsetDefinitionBySubsetId(executionId,
                subset.getParentId());
        Record itemInCatalogBaseTable = dslContext.fetchOne(
                subsetService.getItemQueryWithPnOrMpn(executionId, catalogSubset, ITEM, SiSubsetQueryViewType.BASE),
                DSL.sql(QUERY_WITH_PART_NUMBER_SATRT + partNumber + QUERY_WITH_PART_NUMBER_END));
        if (itemInCatalogBaseTable != null) {
            return true;
        } else {
            return isItemExistsInExternal(executionId, partNumber, publisherId, catalogSubset);
        }
    }

    boolean isItemExistsInExternal(ULong executionId, String partNumber, ULong publisherId,
                                   SiSubsetDefinitionRecord catalogSubset) {
        SiSubsetDefinitionRecord externalSubset = subsetService.getSubsetDefinitionBySubsetId(executionId,
                catalogSubset.getParentId());
        if (externalSubset.getSubsetType() == SiSubsetDefinitionSubsetType.E) {
            Record itemInExternalSubset = dslContext.selectFrom(SI_CLIENT_ITEM).where(
                    SI_CLIENT_ITEM.PART_NUMBER.eq(partNumber).and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(publisherId)))
                    .fetchOne();
            if (itemInExternalSubset != null) {
                return true;
            } else {
                return false;
            }
        } else {
            return false;
        }
    }

    @Override
    @Cacheable(value = "IsItemExistsInExternal", key = "{#executionId, #partNumber, #publisherId}", unless = "#result == null")
    public boolean isItemExistsInExternal(ULong executionId, String partNumber, ULong publisherId) {
            Record itemInExternalSubset = dslContext.selectFrom(SI_CLIENT_ITEM).where(
                    SI_CLIENT_ITEM.PART_NUMBER.eq(partNumber).and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(publisherId)))
                    .fetchOne();
            if (itemInExternalSubset != null)
                return true;
            return false;
    }

	@Override
	public Timestamp updateExternalSubsetItemTimeStamp(String itemId, ULong publisherId) {
		Timestamp currentTime = Timestamp.from(Instant.now());
		dslContext.update(SI_CLIENT_ITEM).set(SI_CLIENT_ITEM.UPDATED_AT, currentTime)
				.where(SI_CLIENT_ITEM.ID.eq(itemId).and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(publisherId))).execute();

		return currentTime;
	}

    @Override
    public RecordType getRecordType(SiSubsetDefinitionRecord subset, PimDataObject qdo) {
        String partNumber = safeValueOf(qdo.getData().get(PART_NUMBER));
        ULong executionId = qdo.getExecutionId();
        if (subset.getSubsetType() == SiSubsetDefinitionSubsetType.E) {
            return new PublisherRecord();
        } else if (subset.getSubsetType() == SiSubsetDefinitionSubsetType.W) {
            SiSubsetDefinitionRecord catalogSubset = subsetService.getSubsetDefinitionBySubsetId(executionId,
                    subset.getParentId());
            ULong publisherId = itemService.getPublisherIdForClientId(qdo.getExecutionId(), qdo.getClientId());
            Record itemInCatalogBaseTable = dslContext.fetchOne(
					subsetService.getItemQueryWithPnOrMpn(executionId, catalogSubset, ITEM, MERGE),
					DSL.sql(QUERY_WITH_PART_NUMBER_SATRT + partNumber + QUERY_WITH_PART_NUMBER_END));
            Record itemInExternalSubset = dslContext.selectFrom(SI_CLIENT_ITEM).where(
                    SI_CLIENT_ITEM.PART_NUMBER.eq(partNumber).and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(publisherId)))
                    .fetchOne();
            if (convertToBoolean(subset.getZeroMembershipAllowed())) {
                Validate.notNull(publisherId, "Something is wrong, you need to be publisher to execute the operation");
                if (itemInExternalSubset != null || itemInCatalogBaseTable != null)
                    return new ParentRecord();
                else
                    return new PrivateRecord();
            } else {
                Record itemInWorkspaceBaseTable = dslContext.fetchOne(
                        subsetService.getItemQueryWithPnOrMpn(executionId, subset, ITEM, BASE),
                        DSL.sql(QUERY_WITH_PART_NUMBER_SATRT + partNumber + QUERY_WITH_PART_NUMBER_END));
                if(itemInExternalSubset != null && itemInCatalogBaseTable == null) {
                    return new PublisherNonSubscribedRecord();
                } else if (itemInCatalogBaseTable != null && itemInWorkspaceBaseTable == null)
                    return new CatalogNonSubscribedRecord();
                else if (itemInCatalogBaseTable != null)
                    return new ParentRecord();
                else
                    return new PrivateRecord();
            }
        }
        return new PrivateRecord();
    }

    @Override
    @Cacheable(value = "MyAllowedClientIds", key = "{#clientId, #executionId}", unless = "#result == null")
    public Set<ULong> getMyAllowedClientIds(ULong clientId, ULong executionId) {
        HashSet<ULong> set = dslContext.select(SI_PUBLISHER_CLIENT.CLIENT_ID).from(SI_PUBLISHER_SUBSCRIBER)
                .join(SI_PUBLISHER_CLIENT).on(SI_PUBLISHER_CLIENT.ID.eq(SI_PUBLISHER_SUBSCRIBER.PUBLISHER_ID))
                .where(SI_PUBLISHER_SUBSCRIBER.CLIENT_ID.eq(clientId)).fetchInto(ULong.class).stream()
                .collect(Collectors.toCollection(HashSet::new));
        set.add(clientId);
        return set;
    }

	@Override
	public void deleteItem(String clientName, String partNumber, ULong externalSubsetId) {
		Routines.deleteItem(this.dslContext.configuration(), externalSubsetId, clientName, null, partNumber, null);
	}

	@Override
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void updateIdwPartNumberMapWithRetainId(String partNumber, String retainPartNumber,
			List<String> externalSystemItemIdList) {

		this.dslContext.update(IDW_PARTNUMBER_MAP).set(IDW_PARTNUMBER_MAP.PART_NUMBER, retainPartNumber)
				.where(IDW_PARTNUMBER_MAP.EXTERNAL_SYSTEM_ITEM_ID.in(externalSystemItemIdList)).execute();

		this.dslContext.update(TBITEM).set(TBITEM.PART_NUMBER, retainPartNumber)
				.where(TBITEM.NITEMID.in(externalSystemItemIdList)).execute();
	}

	@Override
	public List<String> getExternalIds(String partNumber) {
		return this.dslContext.select(IDW_PARTNUMBER_MAP.EXTERNAL_SYSTEM_ITEM_ID).from(IDW_PARTNUMBER_MAP)
				.where(IDW_PARTNUMBER_MAP.PART_NUMBER.eq(partNumber)).fetchInto(String.class);
	}

	@Override
	public String getClientName(ULong clientId) {
		return this.dslContext.select(IAM_CLIENT.CLIENT_NAME).from(IAM_CLIENT)
				.where(IAM_CLIENT.ID.eq(clientId).and(IAM_CLIENT.CLIENT_TYPE_CODE.eq("P"))).limit(1)
				.fetchOneInto(String.class);
	}

	@Override
	public List<String> getItems(String partNumber, ULong publisherId) {
		return this.dslContext.select(SI_CLIENT_ITEM.ID).from(SI_CLIENT_ITEM)
				.where(SI_CLIENT_ITEM.PART_NUMBER.eq(partNumber).and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(publisherId)))
				.fetchInto(String.class);
	}

	@Override
	public int getCountFromIdwPartNumberMap(String partNumber) {
		return this.dslContext.fetchCount(
				this.dslContext.selectFrom(IDW_PARTNUMBER_MAP).where(IDW_PARTNUMBER_MAP.PART_NUMBER.eq(partNumber)));
	}

	@Override
	public Stream<Record4<ULong, String, String, String>> getBatchSubsetDetails(ULong clientId) {
		return this.dslContext
				.select(SI_SUBSET_DEFINITION.ID, SI_SUBSET_DEFINITION.SCHEMA_NAME,
						SI_SUBSET_DEFINITION.MASTER_LIST_TABLE_NAME, SI_SUBSET_DEFINITION.LINK_MERGED_TABLE_NAME)
				.from(SI_SUBSET_DEFINITION).where(SI_SUBSET_DEFINITION.ZERO_MEMBERSHIP_ALLOWED.isTrue()
						.and(SI_SUBSET_DEFINITION.CLIENT_ID.eq(clientId)))
				.fetchStream();
	}

	@Override
	public boolean isItemExistsInBatchSubset(String itemId, Table<Record> e) {
		return this.dslContext.selectFrom(e).where(DSL.field("id", String.class).eq(itemId)).limit(1).execute() == 1;
	}

	@Override
	public boolean isLinkedItemExistsInBatchSubset(String itemId, Table<Record> e) {
		return this.dslContext.selectFrom(e).where(DSL.field("LINKED_ITEM_ID", String.class).eq(itemId)).limit(1)
				.execute() == 1;
	}

	@Override
	@Transactional(propagation = Propagation.REQUIRES_NEW)
	public void removeRetainId(List<String> externalSystemItemIdList) {
		this.dslContext.deleteFrom(IDW_PARTNUMBER_MAP)
				.where(IDW_PARTNUMBER_MAP.EXTERNAL_SYSTEM_ITEM_ID.in(externalSystemItemIdList)).execute();

		this.dslContext.deleteFrom(TBITEM).where(TBITEM.NITEMID.in(externalSystemItemIdList)).execute();
	}
	
	@Override
	@Transactional(isolation = READ_UNCOMMITTED)
	@Async
	public void insertOrUpdateImportDetails(ULong subsetId, ULong publisherId, String itemId, ULong updatedBy, ULong executionId,
			Timestamp currentTime, SiItemUpdateDetailsStatus siItemUpdateDetailsStatus) {
		try {
			logger.info("Update in si_item_update_details table started for job:{} with item id: {} and subset_id: {}",
					executionId, itemId, subsetId);
			Record2<String, String> result = this.dslContext.select(IAM_USER.USER_NAME, IAM_USER.EMAIL).from(IAM_USER)
					.where(IAM_USER.ID.eq(updatedBy)).fetchOne();
			this.dslContext
					.insertInto(SI_ITEM_UPDATE_DETAILS, SI_ITEM_UPDATE_DETAILS.SUBSET_ID,
							SI_ITEM_UPDATE_DETAILS.PUBLISHER_ID, SI_ITEM_UPDATE_DETAILS.ITEM_ID, DSL.field(UPDATED_AT),
							SI_ITEM_UPDATE_DETAILS.UPDATED_BY, SI_ITEM_UPDATE_DETAILS.UPDATED_BY_USER_ID,
							SI_ITEM_UPDATE_DETAILS.UPDATED_BY_USER_EMAIL, SI_ITEM_UPDATE_DETAILS.JOB_EXECUTION_ID,
							SI_ITEM_UPDATE_DETAILS.STATUS)
					.values(subsetId, publisherId, itemId, currentTime, result.value1(), updatedBy, result.value2(),
							executionId, siItemUpdateDetailsStatus)
					.execute();
			logger.info("Update in si_item_update_details table finished for job:{} with item id: {} and subset_id: {}",
					executionId, itemId, subsetId);
		} catch (Exception ex) {
			logger.error(
					"Unable to process update in si_item_update_details with jobId: {} with item id: {} and subset_id: {}",
					executionId, itemId, subsetId, ex);
		}
	}

    @Override
	public Result<Record> getExternalSubsetItem(String mfrName, String mfrPartNumber, ULong publisherId) {
		mfrName = TextUtil.anyToDBFormat(mfrName);
		return dslContext.select(SI_CLIENT_ITEM.fields()).from(SI_CLIENT_ITEM).innerJoin(TMB_BRAND)
				.on(TMB_BRAND.ID.eq(SI_CLIENT_ITEM.BRAND_ID)).innerJoin(TMB_MANUFACTURER)
				.on(TMB_MANUFACTURER.ID.eq(TMB_BRAND.MANUFACTURER_ID))
				.where(SI_CLIENT_ITEM.MFR_PART_NUMBER.eq(mfrPartNumber)
						.and(TMB_MANUFACTURER.MANUFACTURER_NAME.eq(mfrName))
						.and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(publisherId)))
				.fetch();
	}

	@Override
	public void createOrUpdateBulkDummyShadowRecord(ULong subsetId, List<Map<String, String>> dataList, ULong updatedBy) {
		SiSubsetDefinitionRecord definition = this.getSubsetDefinition(null, subsetId);
		List<String> idList = dataList.stream().map(e -> e.get("ITEM_ID")).collect(Collectors.toList());
		Result<Record> shadowRecord = this.dslContext.select()
				.from(table(name(definition.getSchemaName(), definition.getItemShadowTableName())))
				.where(DSL.field(ID).in(idList)).fetch();
		Timestamp updatedAt = Timestamp.from(Instant.now());

		if (shadowRecord != null && !shadowRecord.isEmpty()) {
			List<UpdateConditionStep<Record>> updates = new ArrayList<>();
			shadowRecord.stream().forEach(e -> {
				idList.remove(e.get(ID));
				e.set(field("record_status"), "C");
				e.set(field(UPDATED_AT), updatedAt);
				updates.add(this.dslContext
						.update(table(name(definition.getSchemaName(), definition.getItemShadowTableName()))).set(e)
						.where(DSL.field("ID").eq(e.get(ID))));
			});
			this.dslContext.batch(updates).execute();
		}

		if (!idList.isEmpty()) {
			List<Field<Object>> insertFields = new ArrayList<>();
			insertFields.add(field(ID));
			insertFields.add(field(PART_NUMBER));
			insertFields.add(field(PART_NUMBER_HASH));
			insertFields.add(field(RECORD_STATUS));
			insertFields.add(field(UPDATED_AT));
			insertFields.add(field(UPDATED_BY));
			InsertValuesStepN<Record> insert = this.dslContext
					.insertInto(table(name(definition.getSchemaName(), definition.getItemShadowTableName())))
					.columns(insertFields);

			dataList.stream().filter(f -> idList.contains(f.get("ITEM_ID"))).forEach(e -> {
				e.put(RECORD_STATUS, "C");
				e.put(UPDATED_AT, updatedAt.toString());
				e.put(UPDATED_BY, updatedBy.toString());

				insert.values(insertFields.stream().map(Field::getName)
						.map(v -> (v == ID) ? e.get("ITEM_ID") : e.get(v)).collect(Collectors.toList()));
			});
			Map<Field<Object>, Field<Object>> maps = newHashMap();
			insertFields.stream().forEach(col -> maps.put(col, values(col)));
			insert.onDuplicateKeyUpdate().set(maps).execute();
		}
	}
	
	@Override
	public String getClientTypeCode(ULong clientId) {
		return this.dslContext.select(IAM_CLIENT.CLIENT_TYPE_CODE).from(IAM_CLIENT).where(IAM_CLIENT.ID.eq(clientId))
				.fetchOneInto(String.class);
	}
	
    @Override
	public ULong getPublisherClientIdThruClientId(ULong clientId) {
		String clientTypeCode = this.dslContext.select(IAM_CLIENT.CLIENT_TYPE_CODE).from(IAM_CLIENT)
				.where(IAM_CLIENT.ID.eq(clientId)).limit(1).fetchOneInto(String.class);
		if ("P".equals(clientTypeCode))
			return clientId;
		return this.dslContext.select(SI_PUBLISHER_CLIENT.CLIENT_ID).from(SI_PUBLISHER_CLIENT)
				.join(SI_PUBLISHER_SUBSCRIBER).on(SI_PUBLISHER_SUBSCRIBER.PUBLISHER_ID.eq(SI_PUBLISHER_CLIENT.ID))
				.where(SI_PUBLISHER_SUBSCRIBER.CLIENT_ID.eq(clientId)).limit(1).fetchOneInto(ULong.class);

	}

	@Override
	public String getDefaultManufacturerStatus(ULong publisherId) {

			try {
				return dslContext.select(TMB_MANUFACTURER_STATUS.MANUFACTURER_STATUS).from(TMB_MANUFACTURER_STATUS)
						.where(TMB_MANUFACTURER_STATUS.PUBLISHER_ID.eq(publisherId.longValue())).and(TMB_MANUFACTURER_STATUS.DEFAULT_VALUE.eq(Byte.valueOf("1"))).fetchOneInto(String.class);
			} catch (MappingException e) {
				logger.info("Mapping exception in while fetching data for the publisher id" + publisherId);
				throw new MappingException(
						"Mapping exception in while fetching data for the publisher id" + publisherId);
			} catch (DataAccessException e) {
				logger.info("DataAccessException in while fetching data for the publisher id" + publisherId);
				throw new DataAccessException(
						"DataAccessException in while fetching data for the publisher id" + publisherId);
			}
		}
	
	@Override
	public String getDefaultEnrichedIndicatorStatus(ULong publisherId) {

			try {
				return dslContext.select(TMB_ENRICHED_INDICATOR_STATUS.ENRICHED_INDICATOR_STATUS).from(TMB_ENRICHED_INDICATOR_STATUS)
						.where(TMB_ENRICHED_INDICATOR_STATUS.PUBLISHER_ID.eq(publisherId.longValue())).and(TMB_ENRICHED_INDICATOR_STATUS.DEFAULT_VALUE.eq(Byte.valueOf("1"))).fetchOneInto(String.class);
			} catch (MappingException e) {
				logger.info("Mapping exception in while fetching data for the publisher id" + publisherId);
				throw new MappingException(
						"Mapping exception in while fetching data for the publisher id" + publisherId);
			} catch (DataAccessException e) {
				logger.info("DataAccessException in while fetching data for the publisher id" + publisherId);
				throw new DataAccessException(
						"DataAccessException in while fetching data for the publisher id" + publisherId);
			}
		}
	@Override
	public String  getByIdWithHeaderImagePath(ULong publisherclientId) {
	ClientNotificationConfiguration notificationConfigurationTable = ClientNotificationConfiguration.CLIENT_NOTIFICATION_CONFIGURATION;
	ClientNotificationConfigurationRecord record = null;
	
	try {
		record = dslContext.selectFrom(notificationConfigurationTable)
				.where(notificationConfigurationTable.CLIENT_ID.eq(publisherclientId)).fetchOne();
	} catch (DataAccessException e) {
		logger.debug("DataAccessException ", e);
		return null;
	} catch (Exception e) {
		logger.debug("Exception ", e);
		return null;
	}
	
	if (null != record) {
		return record.getHeaderImage();
	}
	return null;
   }
	
	@Override
	public String imageNameFromFilePath(String filePath) {

		String imageNameFromPath = filePath;
		if (null != filePath && !filePath.trim().isEmpty()) {
			int lastIndexStr = filePath.trim().lastIndexOf("/");
			imageNameFromPath = filePath.trim().substring(++lastIndexStr);
		}
		return imageNameFromPath;
	}
	
    @Override
    @Cacheable(value = "ItemRecord", key = "{#executionId, #publisherId, #partNumber}", unless = "#result == null")
    public Record getItemRecordWhenNull(SiSubsetDefinitionRecord definition, String partNumber, ULong executionId,
                                ULong publisherId) {
        if (definition.getSubsetType() == SiSubsetDefinitionSubsetType.E) {
            Validate.notNull(publisherId, "Something is wrong, you need to be publisher to execute the operation");
            return dslContext.selectFrom(SI_CLIENT_ITEM)
                    .where(SI_CLIENT_ITEM.PART_NUMBER.eq(partNumber).and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(publisherId)))
                    .fetchOne();
        } else {
			Record itemRecord = dslContext.fetchOne(
					subsetService.getItemQueryWithPnOrMpn(executionId, definition, ITEM, MERGE),
					DSL.sql(QUERY_WITH_PART_NUMBER_SATRT + partNumber + QUERY_WITH_PART_NUMBER_END));
            if (itemRecord == null || convertToBoolean(definition.getZeroMembershipAllowed())) {
                SiSubsetDefinitionRecord parentSubSet = itemService.getSubsetDefinition(executionId, definition.getParentId());
				itemRecord = dslContext.fetchOne(
						subsetService.getItemQueryWithPnOrMpn(executionId, parentSubSet, ITEM, MERGE),
						DSL.sql(QUERY_WITH_PART_NUMBER_SATRT + partNumber + QUERY_WITH_PART_NUMBER_END));
                if (itemRecord == null) {
                    Validate.notNull(publisherId, "Something is wrong, you need to be publisher to execute the operation");
                    parentSubSet = itemService.getSubsetDefinition(executionId, parentSubSet.getParentId());
                    if (parentSubSet.getSubsetType() == SiSubsetDefinitionSubsetType.E) {
                        itemRecord = dslContext.selectFrom(SI_CLIENT_ITEM).where(SI_CLIENT_ITEM.PART_NUMBER
                                .eq(partNumber).and(SI_CLIENT_ITEM.PUBLISHER_ID.eq(publisherId))).fetchOne();
                    }
                }
            }
            return itemRecord;
        }
     }

	@Override
	public Record checkItems(String partNumber, ULong subsetId, ULong publisherId,SiSubsetDefinitionRecord subsetDefition) {
		Record itemRecord = dslContext.fetchOne(
				subsetService.getItemQueryWithPnOrMpn(null, subsetDefition, ITEM, BASE),
				DSL.sql(QUERY_WITH_PART_NUMBER_SATRT + partNumber + QUERY_WITH_PART_NUMBER_END));
		return itemRecord;
	}
	
	/**
	 * Method to support valid value checks for global att.
	 */
    @SuppressWarnings("unchecked")
	private void validateValidValue(PimDataObject qdo) {
    	
		ULong publisherClientId = this.getPubClientIdViaClientId(qdo.getClientId()); 
    	
		List<LuGlobalFilterableAttributesRecord> globalFields = this.dslContext.selectFrom(LuGlobalFilterableAttributes.LU_GLOBAL_FILTERABLE_ATTRIBUTES)
				.where(LuGlobalFilterableAttributes.LU_GLOBAL_FILTERABLE_ATTRIBUTES.IS_VALID_VALUE_ENABLED.eq(Byte.valueOf("1"))).fetch();
    	
		for(LuGlobalFilterableAttributesRecord rec : globalFields) {
			if(qdo.getData().containsKey(rec.getItemColumnName().toUpperCase())) {
				String atrName = rec.getItemColumnName();
				Object atrVal = qdo.getData().get(atrName);
				GlobalAttributeFilterableDefinitionRecord def = this.dslContext.selectFrom(GlobalAttributeFilterableDefinition.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION)
						.where(GlobalAttributeFilterableDefinition.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION.GLOBAL_OR_XFIELD_ATTRIBUTE_ID.eq(rec.getId()))
						.and(GlobalAttributeFilterableDefinition.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION.CLIENT_ID.eq(publisherClientId))
						.fetchOne();
				
				if(def==null) continue;

				if(atrVal!=null) {
					GlobalAttributeFilterableValuesRecord valueRec = this.dslContext.selectFrom(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES)
							.where(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION_ID.eq(def.getId()))
							.and(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.VALID_VALUE.eq(atrVal.toString()))
							.fetchOne();

					if(valueRec==null) {
						List<String> valsName = this.dslContext.select(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.VALID_VALUE)
								.from(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES)
								.where(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION_ID.eq(def.getId()))
								.fetchInto(String.class);
						
						if(valsName.isEmpty()) continue;

						throw new IllegalArgumentException("The value entered for "+ atrName +" is not an acceptable value. The valid values are " + valsName + ".");
					}
		    	}  else if(def.getHasDefault().intValue()==1) {
		    		atrVal = this.dslContext.select(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.VALID_VALUE)
							.from(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES)
							.where(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.GLOBAL_ATTRIBUTE_FILTERABLE_DEFINITION_ID.eq(def.getId()))
							.and(GlobalAttributeFilterableValues.GLOBAL_ATTRIBUTE_FILTERABLE_VALUES.IS_DEFAULT.eq(Byte.valueOf("1")))
							.fetchOneInto(String.class);
		    		
		    		qdo.getData().put(atrName, atrVal);
		    	}
			}
		}
    }
    
	private ULong getPubClientIdViaClientId(ULong clientId) {
		String clientTypeCode = this.dslContext.select(IAM_CLIENT.CLIENT_TYPE_CODE).from(IAM_CLIENT)
				.where(IAM_CLIENT.ID.eq(clientId)).limit(1).fetchOneInto(String.class);

		if (!"P".equals(clientTypeCode)) {
			ULong publisherClientId = this.dslContext.select(SI_PUBLISHER_CLIENT.CLIENT_ID).from(SI_PUBLISHER_CLIENT)
					.join(SI_PUBLISHER_SUBSCRIBER).on(SI_PUBLISHER_SUBSCRIBER.PUBLISHER_ID.eq(SI_PUBLISHER_CLIENT.ID))
					.where(SI_PUBLISHER_SUBSCRIBER.CLIENT_ID.eq(clientId)).limit(1).fetchOneInto(ULong.class);
			
			return publisherClientId;
		}

		return clientId;
	}
}
