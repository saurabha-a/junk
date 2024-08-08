package com.unilog.prime.etl2.service;

import static com.unilog.iam.jooq.tables.IamClient.IAM_CLIENT;
import static com.unilog.iam.jooq.tables.IamUser.IAM_USER;
import static com.unilog.iam.jooq.tables.SiPublisherClient.SI_PUBLISHER_CLIENT;
import static com.unilog.iam.jooq.tables.SiPublisherSubscriber.SI_PUBLISHER_SUBSCRIBER;
import static com.unilog.prime.commons.util.DateUtil.MYSQL_TIMESTAMP_FORMAT;
import static com.unilog.prime.commons.util.DateUtil.getStringAsDate;
import static com.unilog.prime.jooq.tables.SiClientContentRules.SI_CLIENT_CONTENT_RULES;
import static com.unilog.prime.misc.jooq.tables.MiscEtl.MISC_ETL;
import static com.unilog.prime.misc.jooq.tables.MiscJob.MISC_JOB;
import static com.unilog.prime.misc.jooq.tables.MiscJobExecution.MISC_JOB_EXECUTION;
import static com.unilog.prime.jooq.tables.TmbManufacturerStatus.TMB_MANUFACTURER_STATUS;
import static com.unilog.prime.misc.jooq.tables.MiscJobExecutionFiles.MISC_JOB_EXECUTION_FILES;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Record4;
import org.jooq.Result;
import org.jooq.UpdateSetMoreStep;
import org.jooq.exception.DataAccessException;
import org.jooq.exception.MappingException;
import org.jooq.impl.DSL;
import org.jooq.types.UInteger;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.Cacheable;

import com.unilog.cx1.pim.commons.enumeration.ImportSaveType;
import com.unilog.cx1.pim.commons.enumeration.UpdateType;
import com.unilog.cx1.pim.commons.model.MiscJobExecutionModel;
import com.unilog.cx1.pim.commons.model.MiscJobModel;
import com.unilog.cx1.pim.commons.util.UserUtil;
import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.commons.util.EnumUtil;
import com.unilog.prime.commons.util.StringUtil;
import com.unilog.prime.etl2.constant.ETLConstants;
import com.unilog.prime.etl2.enumeration.Job;
import com.unilog.prime.etl2.exception.ETLException;
import com.unilog.prime.etl2.model.QueDataObject;
import com.unilog.prime.misc.jooq.enums.MiscEtlEtlType;
import com.unilog.prime.misc.jooq.enums.MiscJobExecutionFilesType;
import com.unilog.prime.misc.jooq.enums.MiscJobExecutionFinishingStatus;
import com.unilog.prime.misc.jooq.enums.MiscJobExecutionFtpStatus;
import com.unilog.prime.misc.jooq.enums.MiscJobExecutionStatus;
import com.unilog.prime.misc.jooq.enums.MiscJobJobCategory;
import com.unilog.prime.misc.jooq.tables.records.MiscEtlRecord;
import com.unilog.prime.misc.jooq.tables.records.MiscJobExecutionRecord;

public abstract class AbstractMiscJobExecutionService implements IMiscJobExecutionService {

	private static final String SOURCE_FORMAT = "sourceFormat";

	protected final Logger logger;

	@Autowired
	protected DSLContext dslContext;

	@Autowired
	protected AmqpTemplate amqpTemplate;

	@Autowired
	protected IMiscJobExecutionService miscJobExecutionService;

	@Value("${import.file.path}")
	private String importFilePath;

	@Value("${export.file.path}")
	private String exportFilePath;

	@Value("${bgJob.file.path}")
	private String bgFilePath;

	private static final String COUNTER = "COUNTER";
	private static final String REGEX_CLIENT_ID = "\\{clientId\\}";

	public AbstractMiscJobExecutionService() {
		logger = LoggerFactory.getLogger(this.getClass());
	}

	@Override
	public void increment(ULong executionId, int counterNumber) {

		Field<UInteger> field = MISC_JOB_EXECUTION.field(COUNTER + counterNumber, UInteger.class);

		int count = 3;
		do {
			try {
				this.dslContext.transaction(ctx -> DSL.using(ctx).update(MISC_JOB_EXECUTION).set(field, field.add(1))
						.where(MISC_JOB_EXECUTION.ID.eq(executionId)).execute());
				break;
			} catch (Exception ex) {
				logger.debug("Increment update exception for Execution ID : {}, Counter Number : {}", executionId,
						counterNumber, ex);
			}
			count--;
		} while (count > 0);

		if (count == 0) {
			throw new ETLException("Unable to increase the counter for Counter Number : " + counterNumber);
		}
	}

	@Override
	public void addToCounter(ULong executionId, int counterNumber, int value) {
		if (value == 0)
			return;
		Field<UInteger> field = MISC_JOB_EXECUTION.field(COUNTER + counterNumber, UInteger.class);
		this.dslContext.update(MISC_JOB_EXECUTION).set(field, field.add(value))
				.where(MISC_JOB_EXECUTION.ID.eq(executionId)).execute();
	}

	@Override
	public void setCounter(ULong executionId, int counterNumber, int count) {

		Field<UInteger> field = MISC_JOB_EXECUTION.field(COUNTER + counterNumber, UInteger.class);
		this.dslContext.update(MISC_JOB_EXECUTION).set(field, UInteger.valueOf(count))
				.where(MISC_JOB_EXECUTION.ID.eq(executionId)).execute();
	}

	@Override
	public void setStatus(ULong executionId, MiscJobExecutionStatus status) {

		try {

			UpdateSetMoreStep<MiscJobExecutionRecord> setStep = this.dslContext.update(MISC_JOB_EXECUTION)
					.set(MISC_JOB_EXECUTION.STATUS, status);

			if (status != MiscJobExecutionStatus.P)
				setStep.set(MISC_JOB_EXECUTION.FINISHED_AT, DSL.currentTimestamp());

			setStep.where(
					MISC_JOB_EXECUTION.ID.eq(executionId).and(MISC_JOB_EXECUTION.STATUS.eq(MiscJobExecutionStatus.P)))
					.execute();
		} catch (Exception ex) {

			this.logger.error("Exception in executing query : setStatus of MiscJobExecution.", ex);
		}
	}

	@Cacheable(value = "executionStatus", key = "#executionId", unless = "#result == false")
	@Override
	public boolean getStatusChacheIfFailed(ULong executionId) {

		MiscJobExecutionStatus status = this.dslContext.select(MISC_JOB_EXECUTION.STATUS).from(MISC_JOB_EXECUTION)
				.where(MISC_JOB_EXECUTION.ID.eq(executionId)).limit(1).fetchOneInto(MiscJobExecutionStatus.class);
		return (status != null && status == MiscJobExecutionStatus.F);
	}

	@Override
	public void readingDone(ULong executionId) {
		try {
			this.dslContext.update(MISC_JOB_EXECUTION).set(MISC_JOB_EXECUTION.READING_END, (byte) 1)
					.where(MISC_JOB_EXECUTION.ID.eq(executionId)).execute();
		} catch (Exception ex) {

			this.logger.error("Exception in executing query : readingDone of MiscJobExecution.", ex);
		}
	}

	@Override
	public MiscJobExecutionModel getById(ULong executionId) {
		MiscJobExecutionModel miscJobExecutionModel = this.dslContext.selectFrom(MISC_JOB_EXECUTION)
				.where(MISC_JOB_EXECUTION.ID.eq(executionId)).fetchOneInto(MiscJobExecutionModel.class);
		miscJobExecutionModel.setMiscJob(dslContext.selectFrom(MISC_JOB)
				.where(MISC_JOB.ID.eq(miscJobExecutionModel.getMiscJobId())).fetchOneInto(MiscJobModel.class));
		miscJobExecutionModel
				.setFiles(this.dslContext.select(MISC_JOB_EXECUTION_FILES.FILE).from(MISC_JOB_EXECUTION_FILES)
						.where(MISC_JOB_EXECUTION_FILES.MISC_JOB_EXECUTION_ID.eq(executionId)).fetchInto(String.class));
		return miscJobExecutionModel;
	}

	@Cacheable(value = "ExecutionImportAndUpdateType", key = "{#executionId}")
	@Override
	public Tuple<ImportSaveType, UpdateType> getImportAndUpdateType(ULong executionId) {

		@SuppressWarnings("rawtypes")
		Result<Record1<Map>> paramsResult = this.dslContext.select(MISC_JOB_EXECUTION.EXECUTION_PARAMETERS)
				.from(MISC_JOB_EXECUTION).where(MISC_JOB_EXECUTION.ID.eq(executionId)).fetch();
		if (paramsResult == null || paramsResult.isEmpty())
			return null;

		Object imSType = paramsResult.get(0).value1().get(ETLConstants.JOB_PARAMETER_IMPORT_SAVE_TYPE);
		Object updateType = paramsResult.get(0).value1().get(ETLConstants.JOB_PARAMETER_UPDATE_TYPE);

		return new Tuple<>(EnumUtil.safeValueOf(ImportSaveType.class, imSType, ImportSaveType.INSERT_UPDATE),
				EnumUtil.safeValueOf(UpdateType.class, updateType, UpdateType.OVER_WRITE_ALL_VALUES));
	}

	private Path getFilePath(String filePath, ULong executionId, Integer index) {

		MiscJobExecutionModel execution = this.getById(executionId);
		Map<String, Object> params = execution.getExecutionParameters();

		Path path = Paths
				.get(filePath.replaceAll(REGEX_CLIENT_ID, params.get(ETLConstants.JOB_PARAMETER_CLIENT_ID).toString())
						+ File.separator + execution.getFiles().get(0) + (index == null ? "" : "_" + index));
		File folders = path.getParent().toFile();
		if (!folders.exists())
			folders.mkdirs();

		return path;
	}
	
	private Path getItemMatchingFilePath(String filePath, ULong executionId, Integer fileCount) {

		MiscJobExecutionModel execution = this.getById(executionId);
		Map<String, Object> params = execution.getExecutionParameters();

		Path path = Paths
				.get(filePath.replaceAll(REGEX_CLIENT_ID, params.get(ETLConstants.JOB_PARAMETER_CLIENT_ID).toString())
						+ File.separator + execution.getFiles().get(fileCount));
		File folders = path.getParent().toFile();
		if (!folders.exists())
			folders.mkdirs();

		return path;
	}
	
	@Override
	public Path getItemMatchingFilePath(ULong executionId, int count) {
		return this.getItemMatchingFilePath(bgFilePath, executionId,count);
	}
	
	@Override
	public void setItemMatchingFileNames(ULong executionId, List<String> fileNames,
			MiscJobExecutionFilesType miscJobExecutionFilesType) {

		this.dslContext.delete(MISC_JOB_EXECUTION_FILES)
				.where(MISC_JOB_EXECUTION_FILES.MISC_JOB_EXECUTION_ID.eq(executionId)
						.and(MISC_JOB_EXECUTION_FILES.TYPE.eq(miscJobExecutionFilesType))).execute();

		if (fileNames.isEmpty())
			return;

		var insertStatement = this.dslContext.insertInto(MISC_JOB_EXECUTION_FILES).columns(
				MISC_JOB_EXECUTION_FILES.MISC_JOB_EXECUTION_ID, MISC_JOB_EXECUTION_FILES.FILE,
				MISC_JOB_EXECUTION_FILES.TYPE);

		for (String e : fileNames)
			insertStatement = insertStatement.values(executionId, e, miscJobExecutionFilesType);
		insertStatement.execute();
	}



	
	private Path getFilePathCount(String filePath, ULong executionId, Integer index, int count) {

		MiscJobExecutionModel execution = this.getById(executionId);
		Map<String, Object> params = execution.getExecutionParameters();

		Path path = Paths
				.get(filePath.replaceAll(REGEX_CLIENT_ID, params.get(ETLConstants.JOB_PARAMETER_CLIENT_ID).toString())
						+ File.separator + execution.getFiles().get(count) + (index == null ? "" : "_" + index));
		File folders = path.getParent().toFile();
		if (!folders.exists())
			folders.mkdirs();

		return path;
	}


	@Cacheable(value = "MaxRecordsPerFile", key = "{#executionId}")
	@Override
	public Integer getMaxRecordsPerFile(ULong executionId) {

		MiscJobExecutionModel execution = this.getById(executionId);
		Map<String, Object> params = execution.getExecutionParameters();

		if (!params.containsKey(ETLConstants.JOB_PARAMETER_MAX_RECORDS_PER_FILE))
			return null;

		return Integer.valueOf(params.get(ETLConstants.JOB_PARAMETER_MAX_RECORDS_PER_FILE).toString());
	}
	
	@Cacheable(value = "MaxFileSize", key = "{#executionId}")
	@Override
	public Long getMaxFileSize(ULong executionId) {
		MiscJobExecutionModel execution = this.getById(executionId);
		Map<String, Object> params = execution.getExecutionParameters();

		if (!params.containsKey(ETLConstants.JOB_PARAMETER_MAX_FILE_SIZE))
			return null;

		return Long.valueOf(params.get(ETLConstants.JOB_PARAMETER_MAX_FILE_SIZE).toString());
	}

	@Cacheable(value = "ImportFilePathWithIndex", key = "{#executionId, #index}")
	@Override
	public Path getExportFilePath(ULong executionId, Integer index) {
		return this.getFilePath(exportFilePath, executionId, index);
	}

	@Cacheable(value = "BgJobFilePathWithIndex", key = "{#executionId, #index}")
	@Override
	public Path getBgFilePath(ULong executionId, Integer index) {
		return this.getFilePath(bgFilePath, executionId, index);
	}
	
	@Cacheable(value = "BgJobFilePathWithIndex", key = "{#executionId, #index}")
	@Override
	public Path getFilePathCount(ULong executionId, Integer index,int count) {
		return this.getFilePathCount(bgFilePath, executionId, index,count);
	}

	@Cacheable(value = "ADSyncFilenames", key = "{#executionId, #fileName}")
	@Override
	public Path getADSyncFilePath(ULong executionId, String fileName) {

		MiscJobExecutionModel execution = this.getById(executionId);
		Map<String, Object> params = execution.getExecutionParameters();

		Path path = Paths.get(
				exportFilePath.replaceAll(REGEX_CLIENT_ID, params.get(ETLConstants.JOB_PARAMETER_CLIENT_ID).toString()),
				executionId + "_" + fileName);
		File folders = path.getParent().toFile();
		if (!folders.exists())
			folders.mkdirs();

		return path;
	}

	@Cacheable(value = "ExecutionParameter", key = "{#executionId, #parameterName}")
	@Override
	public Object getExecutionParameter(ULong executionId, String parameterName) {

		MiscJobExecutionModel execution = this.getById(executionId);
		if (execution == null)
			return null;

		Map<String, Object> params = execution.getExecutionParameters();
		if (params == null)
			return null;

		return params.get(parameterName);
	}

	@Cacheable(value = "JobParameter", key = "{#executionId, #parameterName}")
	@Override
	public Object getJobParameter(ULong executionId, String parameterName) {

		MiscJobExecutionModel execution = this.getById(executionId);
		if (execution == null)
			return null;

		Map<String, Object> params = execution.getMiscJob().getJobParameters();
		if (params == null)
			return null;

		return params.get(parameterName);
	}

	@Override
	public Tuple<String, MiscJobJobCategory> getJobNameAndType(ULong executionId) {

		Record2<String, MiscJobJobCategory> record = this.dslContext.select(MISC_JOB.JOB_NAME, MISC_JOB.JOB_CATEGORY)
				.from(MISC_JOB_EXECUTION).leftJoin(MISC_JOB).on(MISC_JOB.ID.eq(MISC_JOB_EXECUTION.MISC_JOB_ID))
				.where(MISC_JOB_EXECUTION.ID.eq(executionId)).limit(1).fetchOne();
		if (record == null)
			return null;

		return new Tuple<>(record.get(MISC_JOB.JOB_NAME), record.get(MISC_JOB.JOB_CATEGORY));
	}

	@Override
	public void setFinishingStatus(ULong executionId, MiscJobExecutionFinishingStatus f) {
		this.dslContext.update(MISC_JOB_EXECUTION).set(MISC_JOB_EXECUTION.FINISHING_STATUS, f)
				.where(MISC_JOB_EXECUTION.ID.eq(executionId)).execute();
	}

	@Cacheable(value = "ETLRecord", key = "{#executionId}")
	@Override
	public MiscEtlRecord getETLRecord(ULong executionId) {

		MiscJobExecutionModel record = this.getById(executionId);

		if (record.getExecutionParameters() == null
				|| !record.getExecutionParameters().containsKey(ETLConstants.JOB_PARAMETER_ETL_ID))
			return null;

		try {

			return this.dslContext.selectFrom(MISC_ETL)
					.where(MISC_ETL.ID.eq(ULong.valueOf(
							record.getExecutionParameters().get(ETLConstants.JOB_PARAMETER_ETL_ID).toString())))
					.fetchOne();
		} catch (Exception ex) {
			logger.error("Unable to find the ETL record for execution id : {}", executionId, ex);
		}

		return null;
	}

	@Cacheable(value = "ETLOthRecord", key = "{#executionId}")
	@Override
	public MiscEtlRecord getETLOthRecord(ULong executionId) {

		MiscJobExecutionModel record = this.getById(executionId);

		if (record.getExecutionParameters() == null
				|| !record.getExecutionParameters().containsKey(ETLConstants.JOB_PARAMETER_OTHER_ETL_ID))
			return null;

		try {

			return this.dslContext.selectFrom(MISC_ETL)
					.where(MISC_ETL.ID.eq(ULong.valueOf(
							record.getExecutionParameters().get(ETLConstants.JOB_PARAMETER_OTHER_ETL_ID).toString())))
					.fetchOne();
		} catch (Exception ex) {
			logger.error("Unable to find the Other ETL record for execution id : {}", executionId, ex);
		}

		return null;
	}

	@Override
	public void setFileNames(ULong executionId, List<String> fileNames,
			MiscJobExecutionFilesType miscJobExecutionFilesType) {

		this.dslContext.delete(MISC_JOB_EXECUTION_FILES)
				.where(MISC_JOB_EXECUTION_FILES.MISC_JOB_EXECUTION_ID.eq(executionId)).execute();

		if (fileNames.isEmpty())
			return;

		var insertStatement = this.dslContext.insertInto(MISC_JOB_EXECUTION_FILES).columns(
				MISC_JOB_EXECUTION_FILES.MISC_JOB_EXECUTION_ID, MISC_JOB_EXECUTION_FILES.FILE,
				MISC_JOB_EXECUTION_FILES.TYPE);

		for (String e : fileNames)
			insertStatement = insertStatement.values(executionId, e, miscJobExecutionFilesType);
		insertStatement.execute();
	}

	@SuppressWarnings("unchecked")
	@Cacheable(value = "ETLHeader", key = "{#executionId}")
	@Override
	public List<String> getHeaders(ULong executionId, QueDataObject qdo) {

		MiscEtlRecord record = this.getETLRecord(executionId);
		if (record == null || record.getEtlLogic() == null || !record.getEtlLogic().containsKey(SOURCE_FORMAT)) {
			record = this.getETLOthRecord(executionId);
			if (record == null || record.getEtlLogic() == null || !record.getEtlLogic().containsKey(SOURCE_FORMAT))
				return Collections.emptyList();
		}
		try {
			Map<String, Object> sf = (Map<String, Object>) record.getEtlLogic().get(SOURCE_FORMAT);
			Object headers = sf.get("headers");
			if (!(headers instanceof List))
				return Collections.emptyList();
			List<String> headerList = new ArrayList<>((List<String>) headers);
			Tuple<String, MiscJobJobCategory> jobNameAndType = this.getJobNameAndType(executionId);
			if (MiscJobJobCategory.I == jobNameAndType.getSecondValue()
					&& (EnumUtil.equals(Job.IMPORT_ITEMS, jobNameAndType.getFirstValue())
							|| EnumUtil.equals(Job.IMPORT_ITEMS_PARTIAL, jobNameAndType.getFirstValue()) 
							|| EnumUtil.equals(Job.IMPORT_LINKED_ITEMS, jobNameAndType.getFirstValue())
							)
					&& qdo != null && qdo.getSourceFormat() != null && qdo.getSourceFormat().getHeader() != null
					&& !qdo.getSourceFormat().getHeader().isEmpty()
					&& qdo.getSourceFormat().getHeader().contains("DATA_SOURCE"))
				headerList.add("DATA_SOURCE");
			
			return headerList;

		} catch (Exception ex) {
			logger.error("Unable to find the headers for the execution : {}", executionId);
		}

		return Collections.emptyList();
	}

	@SuppressWarnings("unchecked")
	@Cacheable(value = "ETLHeader", key = "{#executionId}")
	@Override
	public Tuple<String, String> getManufacturerAndBrandNameHeadersFromETL(ULong executionId) {

		MiscEtlRecord record = this.getETLRecord(executionId);
		if (record == null || record.getEtlLogic() == null || !record.getEtlLogic().containsKey(SOURCE_FORMAT))
			return null;

		try {
			Map<String, Object> sf = (Map<String, Object>) record.getEtlLogic().get(SOURCE_FORMAT);
			Object manufacturerFieldName = sf.get("manufacturerFieldName");
			Object brandFieldName = sf.get("brandFieldName");

			if (manufacturerFieldName != null && brandFieldName != null) {
				return new Tuple<>(manufacturerFieldName.toString(), brandFieldName.toString());
			}
			return null;
		} catch (Exception ex) {
			logger.error("Unable to find the Manufacturer and brand name headers for the execution : {}", executionId);
		}

		return null;
	}

	@SuppressWarnings("unchecked")
	@Cacheable(value = "ETLHeader", key = "{#executionId}")
	@Override
	public Map<String, Object> getPartialHeader(ULong executionId) {

		MiscEtlRecord record = this.getETLRecord(executionId);
		if (record == null || record.getEtlLogic() == null || !record.getEtlLogic().containsKey(SOURCE_FORMAT))
			return Collections.emptyMap();
		
		Map<String, Object> sf = null;
		try {
			sf = (Map<String, Object>) record.getEtlLogic().get(SOURCE_FORMAT);
		} catch (Exception ex) {
			logger.error("Unable to find the Manufacturer and brand name headers for the execution : {}", executionId);
		}

		return sf;
	}

	@Cacheable(value = "ETLLogic", key = "{#executionId}")
	@Override
	public Tuple<MiscEtlEtlType, Object> getLogic(ULong executionId) {

		MiscEtlRecord record = this.getETLRecord(executionId);
		if (record == null || record.getEtlLogic() == null)
			return null;

		if (record.getEtlType() == MiscEtlEtlType.C) {
			if (!record.getEtlLogic().containsKey("code"))
				return null;
			Object o = record.getEtlLogic().get("code");
			if (o instanceof String)
				return new Tuple<>(MiscEtlEtlType.C, o);
		} else {
			if (!record.getEtlLogic().containsKey("definition"))
				return null;
			Object o = record.getEtlLogic().get("definition");
			if (!(o instanceof Map))
				return new Tuple<>(MiscEtlEtlType.D, o);
		}

		return null;
	}

	@Override
	public Tuple<MiscEtlEtlType, Object> getLogic(MiscEtlRecord record) {

		if (record == null || record.getEtlLogic() == null)
			return null;

		if (record.getEtlType() == MiscEtlEtlType.C) {
			if (!record.getEtlLogic().containsKey("code"))
				return null;
			Object o = record.getEtlLogic().get("code");
			if (o instanceof String)
				return new Tuple<>(MiscEtlEtlType.C, o);
		} else {
			if (!record.getEtlLogic().containsKey("definition"))
				return null;
			Object o = record.getEtlLogic().get("definition");
			if ((o instanceof Map))
				return new Tuple<>(MiscEtlEtlType.D, o);
		}

		return null;
	}

	@Override
	public void updateFtpStatus(ULong executionId, MiscJobExecutionFtpStatus ftpStatus) {
		this.dslContext.update(MISC_JOB_EXECUTION).set(MISC_JOB_EXECUTION.FTP_STATUS, ftpStatus)
				.where(MISC_JOB_EXECUTION.ID.eq(executionId)).execute();
	}

	@Override
	public List<Path> getADSyncFileParts(ULong executionId) {

		MiscJobExecutionModel execution = this.getById(executionId);
		Map<String, Object> params = execution.getExecutionParameters();

		Path folderPath = Paths.get(exportFilePath.replaceAll(REGEX_CLIENT_ID,
				params.get(ETLConstants.JOB_PARAMETER_CLIENT_ID).toString()));

		String executionIdString = executionId + "_";

		if (!Files.isDirectory(folderPath))
			return Collections.emptyList();

		try (var s = Files.find(folderPath, 1,
				(path, basicFileAttributes) -> path.getFileName().toString().startsWith(executionIdString))) {

			return s.collect(Collectors.toList());
		} catch (Exception ex) {
			this.logger.error("Unable to find the adsync file parts for the execution id : {}", executionId, ex);
		}

		return Collections.emptyList();
	}

	@Override
	public Path getADSyncZipFilePath(ULong executionId) {

		MiscJobExecutionModel execution = this.getById(executionId);

		Map<String, Object> params = execution.getExecutionParameters();

		String fileName = execution.getFiles().get(0);
		if (fileName.toLowerCase().endsWith(".zip"))
			fileName = fileName + ".zip";
		return Paths.get(
				exportFilePath.replaceAll(REGEX_CLIENT_ID, params.get(ETLConstants.JOB_PARAMETER_CLIENT_ID).toString()),
				fileName);
	}

	@Override
	public Path getExportBasePath(ULong executionId) {
		MiscJobExecutionModel execution = this.getById(executionId);
		Map<String, Object> params = execution.getExecutionParameters();
		return Paths.get(exportFilePath.replaceAll(REGEX_CLIENT_ID,
				params.get(ETLConstants.JOB_PARAMETER_CLIENT_ID).toString()));
	}

	@Override
	public MiscJobExecutionFtpStatus getFtpStatus(ULong executionId) {
		return this.dslContext.select(MISC_JOB_EXECUTION.FTP_STATUS).from(MISC_JOB_EXECUTION)
				.where(MISC_JOB_EXECUTION.ID.eq(executionId)).fetchOneInto(MiscJobExecutionFtpStatus.class);
	}

	@Override
	public Record3<String, String, String> getUserDetails(ULong userId) {
		if (userId == null)
			return null;
		return this.dslContext.select(IAM_USER.FIRST_NAME, IAM_USER.LAST_NAME, IAM_USER.EMAIL).from(IAM_USER)
				.where(IAM_USER.ID.eq(userId)).fetchOne();
	}
	
	@Override
	public Record4<String, String, String,String> getUserDetailsUserName(ULong userId) {
		if (userId == null)
			return null;
		return this.dslContext.select(IAM_USER.FIRST_NAME, IAM_USER.LAST_NAME, IAM_USER.EMAIL,IAM_USER.USER_NAME).from(IAM_USER)
				.where(IAM_USER.ID.eq(userId)).fetchOne();
	}

	@Override
	public ULong getPubliserId(ULong clientId) {

		ULong pubId = this.dslContext.select(SI_PUBLISHER_CLIENT.ID).from(SI_PUBLISHER_CLIENT)
				.where(SI_PUBLISHER_CLIENT.CLIENT_ID.eq(clientId)).limit(1).fetchOneInto(ULong.class);
		if (pubId == null)
			pubId = this.dslContext.select(SI_PUBLISHER_SUBSCRIBER.PUBLISHER_ID).from(SI_PUBLISHER_SUBSCRIBER)
					.where(SI_PUBLISHER_SUBSCRIBER.CLIENT_ID.eq(clientId)).limit(1).fetchOneInto(ULong.class);
		return pubId;
	}
	
	@Override
	public String getPubliserCode(ULong publisherId) {
		return this.dslContext.select(IAM_CLIENT.CODE).from(IAM_CLIENT).join(SI_PUBLISHER_CLIENT)
				.on(SI_PUBLISHER_CLIENT.CLIENT_ID.eq(IAM_CLIENT.ID)).where(SI_PUBLISHER_CLIENT.ID.eq(publisherId))
				.fetchOneInto(String.class);
	}

	@SuppressWarnings("unchecked")
	@Override
	@Cacheable(value = "UserUtil", key = "{#executionId}", unless = "#result == null")
	public UserUtil getUserUtil(ULong executionId) {
		MiscJobExecutionModel executionModel = miscJobExecutionService.getById(executionId);
		if (executionModel == null || executionModel.getMiscJob() == null)
			return null;
		MiscJobModel miscJob = executionModel.getMiscJob();
		Object security = miscJob.getJobParameters().get("security");
		if (security == null)
			return null;
		Map<String, Object> userDetails = (Map<String, Object>) security;
		ULong clientId = ULong.valueOf(userDetails.get("clientId").toString());
		ULong publiserId = miscJobExecutionService.getPubliserId(clientId);
		return new UserUtil(security, publiserId, dslContext);
	}

	@Override
	@Cacheable(value = "MiscJobExecutionModel", key = "{#executionId}", unless = "#result == null")
	public MiscJobExecutionModel getByIdFromCache(ULong executionId) {
		return getById(executionId);
	}
	
	@SuppressWarnings("unchecked")
	@Cacheable(value = "ETLAttributes", key = "{#executionId}")
	@Override
	public List<String> getAttributes(ULong executionId) {

		MiscEtlRecord record = this.getETLRecord(executionId);
		if (record == null || record.getEtlLogic() == null || !record.getEtlLogic().containsKey(SOURCE_FORMAT))
			return Collections.emptyList();
		try {
			Map<String, Object> sf = (Map<String, Object>) record.getEtlLogic().get(SOURCE_FORMAT);
			Object attribuets = sf.get("ATTRIBUTES");
			if (!(attribuets instanceof List))
				return Collections.emptyList();
			return (List<String>) attribuets;
		} catch (Exception ex) {
			logger.error("Unable to find the headers for the execution : {}", executionId);
		}

		return Collections.emptyList();
	}

	@Override
	public Date getLastExecutionDate(ULong executionId) {
		Object prvSyncedDateObj = getExecutionParameter(executionId, ETLConstants.JOB_PARAMETER_PRV_SYNCED_DATE);
		if (prvSyncedDateObj != null) {
			try {
				return getStringAsDate(prvSyncedDateObj.toString(), MYSQL_TIMESTAMP_FORMAT);
			} catch (ParseException e) {
				logger.info("Invalid date or format, Valid date format is  = " + MYSQL_TIMESTAMP_FORMAT);
				throw new IllegalArgumentException(
						"Invalid date or format, Valid date format is  = " + MYSQL_TIMESTAMP_FORMAT);
			}
		}

		MiscJobExecutionModel executionModel = getById(executionId);
		Record1<Timestamp> timestampRecord = dslContext.select(MISC_JOB_EXECUTION.CREATED_AT).from(MISC_JOB_EXECUTION)
				.innerJoin(MISC_JOB)
				.on(MISC_JOB_EXECUTION.MISC_JOB_ID.eq(MISC_JOB.ID))
				.where(MISC_JOB.JOB_NAME.eq(executionModel.getMiscJob().getJobName()))
				.and(MISC_JOB_EXECUTION.FINISHING_STATUS.eq(MiscJobExecutionFinishingStatus.F)).orderBy(MISC_JOB_EXECUTION.ID.desc())
				.limit(1).fetchOne();
		if(timestampRecord != null && timestampRecord.value1() != null)
			return new Date(timestampRecord.value1().getTime());
		return null;
	}
	
	@Override
	@Cacheable(value = "ETLDefinition", key = "{#executionId}")
	public Map<String , String> getEtlDefinition(ULong executionId){
		
		Map<String, Object> etlDefinition = new LinkedHashMap<>();
		MiscEtlRecord record = this.miscJobExecutionService.getETLOthRecord(executionId);
		if (record != null) {

			Tuple<MiscEtlEtlType, Object> definition = this.miscJobExecutionService.getLogic(record);
			if (definition != null && definition.getFirstValue() == MiscEtlEtlType.D)
				etlDefinition.put("publisher", definition.getSecondValue());
		}

		record = this.miscJobExecutionService.getETLRecord(executionId);
		if (record != null) {

			Tuple<MiscEtlEtlType, Object> definition = this.miscJobExecutionService.getLogic(record);
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
	
	@Override
	public boolean checkMultipleCategoriesPermission(ULong clientId) {
		ULong multipleCategoriesAllowed = this.dslContext.select(SI_CLIENT_CONTENT_RULES.MULTIPLE_CATEGORIES_ALLOWED)
				.from(SI_CLIENT_CONTENT_RULES).where(SI_CLIENT_CONTENT_RULES.CLIENT_ID.eq(clientId))
				.fetchOneInto(ULong.class);
		if (multipleCategoriesAllowed == null)
			return false;
		
		return multipleCategoriesAllowed.equals(ULong.valueOf(1));
	}
	
	@Override
	public List<String> getManufacturerStatus(Integer publisherId) {

		try {
			return dslContext.select(TMB_MANUFACTURER_STATUS.MANUFACTURER_STATUS).from(TMB_MANUFACTURER_STATUS)
					.where(TMB_MANUFACTURER_STATUS.PUBLISHER_ID.eq(publisherId.longValue())).orderBy(TMB_MANUFACTURER_STATUS.MANUFACTURER_STATUS.desc()).fetchInto(String.class);
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
}