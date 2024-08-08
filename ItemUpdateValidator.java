package com.unilog.cx1.pim.commons.validator;

import static com.google.common.collect.ImmutableList.of;
import static com.google.common.collect.Lists.newArrayList;
import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.AUTOGENERATE_PARTNUMBER;
import static com.unilog.cx1.pim.commons.constants.Cx1PimConstants.GENERATED_PART_NUMBER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.BRAND_CODE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.BRAND_NAME;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.CUSTOM_KEYWORDS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ENRICHED_INDICATOR;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.GENERIC_EAN_OR_UCC_13;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.GTIN;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.HEIGHT;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ITEM_DOCUMENT_EXCLUDE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ITEM_DOCUMENT_NAME;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ITEM_IMAGE_DEFAULT;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ITEM_IMAGE_EXCLUDE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ITEM_IMAGE_ITEMIMAGE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.LENGTH;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MANUFACTURER_NAME;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MANUFACTURER_STATUS;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MIN_ORDER_QTY;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.MOBILE_DESC;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ORDER_QTY_INTERVAL;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PACKAGE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PACKAGE_QTY;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.QTY_AVAILABLE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.THREED_IMAGE_NAME;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.UNSPSC;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.UPC;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.VOLUME;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.WEIGHT;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.WIDTH;
import static com.unilog.cx1.pim.commons.enumeration.UpdateType.RETAIN_OLD_DATA;
import static com.unilog.prime.commons.constant.CommonAssetsConstants.VALID_DOCUMENT_FILE_EXTN;
import static com.unilog.prime.commons.constant.CommonAssetsConstants.VALID_IMAGE_FILE_EXTN;
import static com.unilog.prime.commons.util.BooleanUtil.convertToBoolean;
import static org.apache.commons.lang.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang.math.NumberUtils.isNumber;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.length;
import static org.apache.commons.lang3.StringUtils.startsWithIgnoreCase;
import static org.apache.commons.lang3.StringUtils.substringAfterLast;
import static org.apache.commons.lang3.Validate.isTrue;

import java.util.List;
import java.util.Map;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;
import com.unilog.cx1.pim.commons.service.IConfigurationService;
import com.unilog.cx1.pim.commons.service.IItemService;
import com.unilog.prime.commons.enumeration.PrimeResponseCode;
import com.unilog.prime.commons.exception.PrimeException;
import static com.unilog.prime.jooq.tables.TmbEnrichedIndicatorStatus.TMB_ENRICHED_INDICATOR_STATUS;

@Component
@Primary
public class ItemUpdateValidator {

    protected final Logger logger;
    private final int MAX_DOC_ASSET_COUNT = 5;
    private final int MAX_IMG_ASSET_COUNT = 5;
//    List<String> ENRICH_INDICATOR_VALID_VALUES = of("Enhanced", "Not Enhanced", "Synced");
    private List<String> manufacturerStatus;
//    List<String> STATUS_VALID_VALUES = newArrayList("Active", "Change GTIN - Old GTIN", "Delete",
//            "Introductory", "Item Alert", "Mature", "Non-price Maintained", "Inactive",
//            "Planned Obsolescence", "Obsolete", "Replacement GTIN - new GTIN", "Unlisted",
//            "Non Price Maintained", "Withdrawn", "Pending For Delete", "Replacement/Repair Part",
//            "Withdrawn from IDW", "Discontinued","Not Found");

    static Map<String, String> dbToCustomFeild;

    @Autowired
    private IConfigurationService configurationService;
    
    @Autowired
    private DSLContext dslContext;
    
    @Autowired
	private IItemService itemService;

    public ItemUpdateValidator() {
        logger = LoggerFactory.getLogger(this.getClass());
    }

    
    public void validate(PimDataObject pdo, Record oldItem, Import anImport,  Map<String, String> dbToCustomFeild) {
    	ItemUpdateValidator.dbToCustomFeild = dbToCustomFeild;
        validateRequiredFields(pdo, anImport);
        validateProvidedPartnumberShouldExistsInSystem(pdo, oldItem, anImport);
        validateNumericFields(pdo);
        validateUpc(pdo.getValue(UPC), anImport);
        validateUnspc(pdo.getValue(UNSPSC), anImport);
        validateEanUcc13(pdo, anImport);
        validateGtin(pdo.getValue(GTIN), anImport);
//        validateMfrStatusIndicator(pdo.getValue(MANUFACTURER_STATUS), anImport);
//        validateEnrichIndicator(pdo, oldItem, anImport);
        validatePartnumber(pdo, oldItem, anImport);
        validateBrandNameAndMfrName(pdo, anImport);
        validateAssets(pdo, anImport);
        validateKeywords(pdo, anImport);
		validateXFields(pdo, anImport);
    }

	private void validateXFields(PimDataObject pdo, Import anImport) {
		if (pdo.contains("EX_MOBILE_DESC")) {
			if (isNotBlank(pdo.getValue("EX_MOBILE_DESC")))
				if(dbToCustomFeild != null)
					isTrue(pdo.getValue("EX_MOBILE_DESC").length() <= 80,"The value for "+dbToCustomFeild.get("EX_MOBILE_DESC")+" cannot exceed 80 characters.");
				else
					isTrue(pdo.getValue("EX_MOBILE_DESC").length() <= 80, "Mobile Description can't be more than 80 characters");
		}
	}

	private void validateKeywords(PimDataObject pdo, Import anImport) {
		validateKeywordsForPublisher(pdo, anImport);
	}

	private void validateKeywordsForPublisher(PimDataObject pdo, Import anImport) {
		if(pdo.contains(CUSTOM_KEYWORDS) && anImport.getUserUtil().isPublisher())
			if(dbToCustomFeild != null)
				isTrue(isBlank(pdo.getValue(CUSTOM_KEYWORDS)),"Publisher does not have permission to add/edit "+dbToCustomFeild.get(CUSTOM_KEYWORDS));
			else
				isTrue(isBlank(pdo.getValue(CUSTOM_KEYWORDS)),"Publisher does not have permission to add/edit CUSTOM_KEYWORDS");
	}

	private void validateAssets(PimDataObject pdo, Import anImport) {
        validatePrimaryImage(pdo);
        validateAssetsName(pdo);
        validateAssetsCount(pdo);
    }

	private void validateAssetsCount(PimDataObject pdo) {
		int count = 0;
		for (int i = 1; i < Integer.MAX_VALUE; i++) {
			if (pdo.contains(ITEM_IMAGE_ITEMIMAGE + i)) {
				String imgName = pdo.getValue(ITEM_IMAGE_ITEMIMAGE + i);
				if (isNotBlank(imgName) && !convertToBoolean(pdo.getValue(ITEM_IMAGE_EXCLUDE + i))) {
					count++;
					if (count > MAX_IMG_ASSET_COUNT)
						throw new PrimeException(HttpStatus.FORBIDDEN,
								PrimeResponseCode.MAX_IMG_ASSET_COUNT.getResponseMsg(),
								PrimeResponseCode.MAX_IMG_ASSET_COUNT.getResponseCode());
				}
			} else
				break;
		}
		count = 0;
		for (int i = 1; i < Integer.MAX_VALUE; i++) {
			if (pdo.contains(ITEM_DOCUMENT_NAME + i)) {
				String documentName = pdo.getValue(ITEM_DOCUMENT_NAME + i);
				if (isNotBlank(documentName) && !convertToBoolean(pdo.getValue(ITEM_DOCUMENT_EXCLUDE + i))) {
					count++;
					if (count > MAX_DOC_ASSET_COUNT)
						throw new PrimeException(HttpStatus.FORBIDDEN,
								PrimeResponseCode.MAX_DOC_ASSET_COUNT.getResponseMsg(),
								PrimeResponseCode.MAX_DOC_ASSET_COUNT.getResponseCode());
				}
			} else
				break;
		}
	}

    private void validateAssetsName(PimDataObject pdo) {
        for (int i = 1; i < Integer.MAX_VALUE; i++) {
            if (pdo.contains(ITEM_IMAGE_ITEMIMAGE + i)) {
                String currentImgName = pdo.getValue(ITEM_IMAGE_ITEMIMAGE + i);
                if (isNotBlank(currentImgName)) {
                    String extension = substringAfterLast(currentImgName, ".");
                    if (isNotBlank(extension) && containsIgnoreCase(VALID_IMAGE_FILE_EXTN, extension)) {
                        continue;
                    } else if (startsWithIgnoreCase(currentImgName, "http") || startsWithIgnoreCase(currentImgName, "www")) {
                        continue;
                    } else
                        throw new IllegalArgumentException("Image name is invalid. " +
                                "Image name should start with either 'http' or 'www' if it is a URL. " +
                                "If the image is a file, the valid extensions are: " + VALID_IMAGE_FILE_EXTN);
                }
            } else break;
        }
        for (int i = 1; i < Integer.MAX_VALUE; i++) {
            if (pdo.contains(ITEM_DOCUMENT_NAME + i)) {
                String currentDocName = pdo.getValue(ITEM_DOCUMENT_NAME + i);
                if (isNotBlank(currentDocName)) {
                    String extension = substringAfterLast(currentDocName, ".");
                    if (isNotBlank(extension) && containsIgnoreCase(VALID_DOCUMENT_FILE_EXTN, extension)) {
                        continue;
                    } else if (startsWithIgnoreCase(currentDocName, "http") || startsWithIgnoreCase(currentDocName, "www")) {
                        continue;
                    } else
                        throw new IllegalArgumentException("Invalid document asset name ('"+ currentDocName +"') found. " +
                                "Asset name should start either with 'http' or 'www' in case of URL and  " +
                                "in case of file, extension should be " + VALID_DOCUMENT_FILE_EXTN);
                }
            } else break;
        }
        for (int i = 1; i < Integer.MAX_VALUE; i++) {
            if (pdo.contains(THREED_IMAGE_NAME + i)) {
                String current360AssetName = pdo.getValue(THREED_IMAGE_NAME + i);
                if (isNotBlank(current360AssetName))
                    if (startsWithIgnoreCase(current360AssetName, "http") || startsWithIgnoreCase(current360AssetName, "www")) {
                        continue;
                    } else
                        throw new IllegalArgumentException("Invalid 360 asset name ('"+ current360AssetName +"') found. " +
                                "Asset name should start either with 'http' or 'www' in case of URL.");
            } else break;
        }
    }


    private void validatePrimaryImage(PimDataObject pdo) {
        boolean foundPrimaryImg = false;
        for (int i = 1; i < Integer.MAX_VALUE; i++) {
            if (pdo.getData().containsKey(ITEM_IMAGE_DEFAULT + i) && !convertToBoolean(pdo.getValue(ITEM_IMAGE_EXCLUDE + i))) {
                boolean currentVal = convertToBoolean(pdo.getData().get(ITEM_IMAGE_DEFAULT + i));
                if (foundPrimaryImg && currentVal)
                    throw new IllegalArgumentException("Only one primary image allowed");
                else if (currentVal)
                    foundPrimaryImg = currentVal;
            } else
                break;
        }
    }

    private void validateProvidedPartnumberShouldExistsInSystem(PimDataObject pdo, Record oldItem, Import anImport) {
        if (oldItem == null && !convertToBoolean(pdo.getData().get(GENERATED_PART_NUMBER))
                && configurationService.getConfigValue(anImport.getUserUtil().getClientId(), AUTOGENERATE_PARTNUMBER, Boolean.class)) {
        	if(dbToCustomFeild != null)
        		isTrue(false, "Item doesn't exists: "+ dbToCustomFeild.get(PART_NUMBER)+" = " + pdo.getValue(PART_NUMBER));
        	else
        		isTrue(false, "Item doesn't exists: partnumber = " + pdo.getValue(PART_NUMBER));
        }
    }

    protected void validateNumericFields(PimDataObject pdo) {
        List<String> fields = List.of(UPC, UNSPSC, VOLUME, PACKAGE + LENGTH, PACKAGE + HEIGHT, PACKAGE + WIDTH,
                PACKAGE + WEIGHT, QTY_AVAILABLE, MIN_ORDER_QTY, PACKAGE_QTY, ORDER_QTY_INTERVAL, GENERIC_EAN_OR_UCC_13, GTIN);
        fields.forEach(e -> {
            String value = pdo.getValue(e);
            if (isNotBlank(value)) {
            	if(dbToCustomFeild != null) 
            		isTrue(isNumber(value), dbToCustomFeild.get(e) + " should be numeric");
            	else
            		isTrue(isNumber(value), e + " should be numeric");
            }
        });
    }

    private void validatePartnumber(PimDataObject pdo, Record oldItem, Import anImport) {
        validateItemShouldNotExistsInExternal(pdo, oldItem, anImport);
        if(dbToCustomFeild !=null )
        	isTrue(pdo.getValue(PART_NUMBER).length() >= 3, dbToCustomFeild.get(PART_NUMBER)+" should be at least 3 and cannot exceed 100 characters");
        else
        	isTrue(pdo.getValue(PART_NUMBER).length() >= 3, "Part number should be at least 3 and cannot exceed 100 characters");
        validateItemExistence(pdo, oldItem, anImport);
    }

    private void validateItemExistence(PimDataObject pdo, Record oldItem, Import anImport) {
        if (anImport.isPartialImport() && oldItem == null) {
        	if(dbToCustomFeild !=null )
                isTrue(false, "The provided PART_NUMBER does not exist in the selected workspace.");
        	else
        		isTrue(false, "Item doesn't exists: partnumber = " + pdo.getValue(PART_NUMBER));
        }
    }

    private void validateItemShouldNotExistsInExternal(PimDataObject pdo, Record oldItem, Import anImport) {
        if (!anImport.isImportByPublisher()) {
        	if(dbToCustomFeild !=null) {
        		this.isTrueValue(!anImport.recordType.isPublisherNonSubscribedRecord(),
                        dbToCustomFeild.get(PART_NUMBER)+" already exists in publisher data");
                this.isTrueValue(!anImport.recordType.isCatalogNonSubscribedRecord(),
                        "The provided PART_NUMBER does exist in the Parent Catalog, but does not exist in the selected workspace.");
        	}
        	else
        	{
	            this.isTrueValue(!anImport.recordType.isPublisherNonSubscribedRecord(),
	                    "PART_NUMBER already exists in publisher data");
	            this.isTrueValue(!anImport.recordType.isCatalogNonSubscribedRecord(),
	                    "The provided PART_NUMBER does exist in the Parent Catalog, but does not exist in the selected workspace.");
        	}
        }
    }

    private void validateRequiredFields(PimDataObject pdo, Import anImport) {
        if (!anImport.isPartialImport()) {
        	if(dbToCustomFeild != null) {
        		isTrue(isNotBlank(pdo.getValue(BRAND_NAME)), dbToCustomFeild.get(BRAND_NAME)+" is missing");
                isTrue(isNotBlank(pdo.getValue(MANUFACTURER_NAME)), dbToCustomFeild.get(MANUFACTURER_NAME)+ " is missing");
                isTrue(isNotBlank(pdo.getValue(PART_NUMBER)), dbToCustomFeild.get(PART_NUMBER)+" is missing");
        	}
        	else {
	            isTrue(isNotBlank(pdo.getValue(BRAND_NAME)), "BRAND_NAME is missing");
	            isTrue(isNotBlank(pdo.getValue(MANUFACTURER_NAME)), "MANUFACTURER_NAME is missing");
	            isTrue(isNotBlank(pdo.getValue(PART_NUMBER)), "PART_NUMBER is missing");
        	}
        }
    }

    @SuppressWarnings("unlikely-arg-type")
	private void validateMfrStatusIndicator(String mfrStatus, Import anImport) {
        if (isNotBlank(mfrStatus) && anImport.getUserUtil().getPublisherId()!=null) {
        	if(dbToCustomFeild != null)
        		isTrue(manufacturerStatus.contains(mfrStatus), "Invalid value of, "+dbToCustomFeild.get(MANUFACTURER_STATUS)+
                        "valid values are :" + manufacturerStatus.contains(mfrStatus));
        	else
        		isTrue(manufacturerStatus.contains(mfrStatus), "Invalid value of Manufacturer Status, " +
                    "valid values are :" + manufacturerStatus);
        }
    }

	protected void validateEnrichIndicator(PimDataObject pdo, Record oldItem, Import anImport) {
        if (pdo.getData().containsKey(ENRICHED_INDICATOR)) {
        	String enrichedIndicator = pdo.getValue(ENRICHED_INDICATOR);
        	if (isNotBlank(enrichedIndicator)) {
        	    ULong publisherId = itemService.getPublisherIdForClientId(pdo.getExecutionId(), pdo.getClientId());
       	
        	    List<String> ENRICHED_INDICATOR_VALID_VALUES = this.dslContext.select().from(TMB_ENRICHED_INDICATOR_STATUS)
        			    .where(TMB_ENRICHED_INDICATOR_STATUS.PUBLISHER_ID.eq(publisherId.longValue()))
        			    .fetch().getValues(TMB_ENRICHED_INDICATOR_STATUS.ENRICHED_INDICATOR_STATUS, String.class);
        			
            
            	if(dbToCustomFeild != null)
            		isTrue(ENRICHED_INDICATOR_VALID_VALUES.contains(enrichedIndicator),
                            "The only valid values for the ENRICHED_INDICATOR column are " + ENRICHED_INDICATOR_VALID_VALUES);
            	else
            		isTrue(ENRICHED_INDICATOR_VALID_VALUES.contains(enrichedIndicator),
                        "The only valid values for the ENRICHED_INDICATOR column are " + ENRICHED_INDICATOR_VALID_VALUES);
            } else {
                if (oldItem != null && !anImport.getUpdateType().equals(RETAIN_OLD_DATA)) {
                	if(dbToCustomFeild != null)
                		isTrue(false, dbToCustomFeild.get(ENRICHED_INDICATOR)+" is missing.");
                	else
                		isTrue(false, "ENRICHED_INDICATOR is missing.");
                }
            }
        }
    }

    private void validateUpc(String upc, Import anImport) {
        if (isNotBlank(upc)) {
        	if(dbToCustomFeild != null) 
        		 isTrue(length(upc) <= 14, dbToCustomFeild.get(UPC)+" can't be more than 14 characters");
        	else
        		isTrue(length(upc) <= 14, "UPC can't be more than 14 characters");
        }
    }

    private void validateGtin(String gtin, Import anImport) {
        if (isNotBlank(gtin)) {
        	if(dbToCustomFeild != null)
        		isTrue(length(gtin) <= 14, dbToCustomFeild.get(GTIN)+" can't be more than 14 characters");
        	else
        		isTrue(length(gtin) <= 14, "GTIN can't be more than 14 characters");
        }
    }

    protected void validateEanUcc13(PimDataObject pdo, Import anImport) {
        String eanUcc13 = pdo.getValue(GENERIC_EAN_OR_UCC_13);
        if (isNotBlank(eanUcc13)) {
            isTrue(length(eanUcc13) <= 14, "EAN_UCC_13 can't be more than 14 characters");
        }
    }

    private void validateUnspc(String unspsc, Import anImport) {
        if (isNotBlank(unspsc)) {
        	if(dbToCustomFeild != null)
        		 isTrue(length(unspsc) <= 8, dbToCustomFeild.get(UNSPSC)+" can't be more than 8 characters");
        	else
        		isTrue(length(unspsc) <= 8, "UNSPSC can't be more than 8 characters");
        }
    }

    private void validateBrandNameAndMfrName(PimDataObject pdo, Import anImport) {
        if (isBrandIdAbsent(pdo)) {
            if (anImport.isPartialImport()) {
            	if(dbToCustomFeild != null) {
            		 if (isMfrPresentButBrandAbsent(pdo)) 
                         isTrue(false,dbToCustomFeild.get(BRAND_NAME)+" and "+dbToCustomFeild.get(MANUFACTURER_NAME)+" both are required");
                     if (isBrandPresentButMfrAbsent(pdo)) 
                         isTrue(false,dbToCustomFeild.get(BRAND_NAME)+" and "+dbToCustomFeild.get(MANUFACTURER_NAME)+" both are required");
            	}
            	else {
            		if (isMfrPresentButBrandAbsent(pdo)) 
                        isTrue(false, "Brand Name and Manufacturer Name both are required");
                    if (isBrandPresentButMfrAbsent(pdo)) 
                        isTrue(false, "Brand Name and Manufacturer Name both are required");
            	}
            } else {
                if (isBlank(pdo.getValue(MANUFACTURER_NAME)) || isBlank(pdo.getValue(BRAND_NAME))) {
                	if(dbToCustomFeild != null)
                        isTrue(false,dbToCustomFeild.get(BRAND_NAME)+" and "+dbToCustomFeild.get(MANUFACTURER_NAME)+" both are required");
                	else
                		isTrue(false, "Brand Name and Manufacturer Name both are required");
                }
            }
        }
    }

    protected boolean isBrandIdAbsent(PimDataObject pdo) {
        return pdo.getData().get(BRAND_CODE) == null;
    }

    private boolean isBrandPresentButMfrAbsent(PimDataObject pdo) {
        return pdo.contains(BRAND_NAME) && (isBlank(pdo.getValue(MANUFACTURER_NAME)) || isBlank(pdo.getValue(BRAND_NAME)));
    }

    private boolean isMfrPresentButBrandAbsent(PimDataObject pdo) {
        return pdo.contains(MANUFACTURER_NAME) && (isBlank(pdo.getValue(MANUFACTURER_NAME)) || isBlank(pdo.getValue(BRAND_NAME)));
    }

    private void isTrueValue(boolean flag, String exceptionMsg) {
        if (flag == false)
            throw new PrimeException(HttpStatus.BAD_REQUEST, exceptionMsg);
    }


	public List<String> getManufacturerStatus() {
		return manufacturerStatus;
	}

	public void setManufacturerStatus(List<String> manufacturerStatus) {
		this.manufacturerStatus = manufacturerStatus;
	}
    
    
}
