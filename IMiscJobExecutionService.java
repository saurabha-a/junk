package com.unilog.prime.etl2.service;

import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jooq.Record3;
import org.jooq.Record4;
import org.jooq.types.ULong;
import org.springframework.cache.annotation.Cacheable;

import com.unilog.cx1.pim.commons.enumeration.ImportSaveType;
import com.unilog.cx1.pim.commons.enumeration.UpdateType;
import com.unilog.cx1.pim.commons.model.MiscJobExecutionModel;
import com.unilog.cx1.pim.commons.util.UserUtil;
import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.etl2.model.QueDataObject;
import com.unilog.prime.misc.jooq.enums.MiscEtlEtlType;
import com.unilog.prime.misc.jooq.tables.records.MiscEtlRecord;
import com.unilog.prime.misc.jooq.enums.MiscJobExecutionFilesType;
import com.unilog.prime.misc.jooq.enums.MiscJobExecutionFinishingStatus;
import com.unilog.prime.misc.jooq.enums.MiscJobExecutionFtpStatus;
import com.unilog.prime.misc.jooq.enums.MiscJobExecutionStatus;
import com.unilog.prime.misc.jooq.enums.MiscJobJobCategory;

public interface IMiscJobExecutionService {

	public void increment(ULong executionId, int counterNumber);

	public void setCounter(ULong executionId, int counterNumber, int count);

	public boolean getStatusChacheIfFailed(ULong executionId);

	public void setStatus(ULong executionId, MiscJobExecutionStatus status);

	public void readingDone(ULong executionId);

	public MiscJobExecutionModel getById(ULong executionId);

	public Tuple<ImportSaveType, UpdateType> getImportAndUpdateType(ULong executionId);

	public Integer getMaxRecordsPerFile(ULong executionId);

	public Path getExportFilePath(ULong executionId, Integer i);

	public void setFinishingStatus(ULong executionId, MiscJobExecutionFinishingStatus f);

	@Cacheable(value = "JobParameter", key = "{#executionId, #parameterName}")
	Object getJobParameter(ULong executionId, String parameterName);

	public Tuple<String, MiscJobJobCategory> getJobNameAndType(ULong executionId);

	public MiscEtlRecord getETLRecord(ULong executionId);

	public void setFileNames(ULong executionId, List<String> fileNames,
			MiscJobExecutionFilesType miscJobExecutionFilesType);

	public List<String> getHeaders(ULong executionId,QueDataObject qdo);

	public Tuple<MiscEtlEtlType, Object> getLogic(ULong executionId);

	public Tuple<MiscEtlEtlType, Object> getLogic(MiscEtlRecord record);

	public Object getExecutionParameter(ULong executionId, String parameterName);

	void updateFtpStatus(ULong executionId, MiscJobExecutionFtpStatus ftpStatus);

	Path getADSyncFilePath(ULong executionId, String fileName);

	public Path getADSyncZipFilePath(ULong executionId);

	public List<Path> getADSyncFileParts(ULong executionId);

	public void addToCounter(ULong executionId, int counter, int value);

	Path getExportBasePath(ULong executionId);

	MiscJobExecutionFtpStatus getFtpStatus(ULong executionId);

	Record3<String, String, String> getUserDetails(ULong userId);

	Tuple<String, String> getManufacturerAndBrandNameHeadersFromETL(ULong executionId);

	MiscEtlRecord getETLOthRecord(ULong executionId);

	public ULong getPubliserId(ULong clientId);

	public String getPubliserCode(ULong publisherId);
	
	UserUtil getUserUtil(ULong executionId);

	MiscJobExecutionModel getByIdFromCache(ULong executionId);

	Path getBgFilePath(ULong executionId, Integer safeValueOf);

	List<String> getAttributes(ULong executionId);

    Date getLastExecutionDate(ULong executionId);

	Map<String, Object> getPartialHeader(ULong executionId);

	public Long getMaxFileSize(ULong executionId);
	
	 Map<String , String> getEtlDefinition(ULong executionId);
	 
	 boolean checkMultipleCategoriesPermission(ULong clientId);

	List<String> getManufacturerStatus(Integer publisherId);

	Path getFilePathCount(ULong executionId, Integer index, int count);

	Record4<String, String, String, String> getUserDetailsUserName(ULong userId);
	 
	Path getItemMatchingFilePath(ULong executionId, int count);

	void setItemMatchingFileNames(ULong executionId, List<String> fileNames,
			MiscJobExecutionFilesType miscJobExecutionFilesType);

}
