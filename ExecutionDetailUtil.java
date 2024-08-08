package com.unilog.prime.etl2.util;

import static com.unilog.prime.misc.jooq.tables.MiscJobExecutionDetails.MISC_JOB_EXECUTION_DETAILS;

import java.util.Map;

import org.jooq.DSLContext;
import org.jooq.types.ULong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unilog.prime.commons.model.RowDataObject;
import com.unilog.prime.commons.util.ExceptionUtil;
import com.unilog.prime.commons.util.StringUtil;
import com.unilog.prime.misc.jooq.enums.MiscJobExecutionDetailsDetailType;
import com.unilog.prime.misc.jooq.tables.records.MiscJobExecutionDetailsRecord;

public class ExecutionDetailUtil {

	private static final Logger logger = LoggerFactory.getLogger(ExecutionDetailUtil.class);

	private ExecutionDetailUtil() {
	}

	public static void createDetail(DSLContext dslContext, ULong executionId, String details, String shortDetails,
			MiscJobExecutionDetailsDetailType detailType) {
		createDetail(dslContext, executionId, null, null, details, shortDetails, detailType);
	}

	public static void createDetail(DSLContext dslContext, ULong executionId, String rowKey, RowDataObject rowData,
			String details, String shortDetails, MiscJobExecutionDetailsDetailType detailType) {

		MiscJobExecutionDetailsRecord record = dslContext.newRecord(MISC_JOB_EXECUTION_DETAILS);
		record.setMiscJobExecutionId(executionId);	
		record.setRowKey((rowKey!=null)  ? (rowKey.length() <= 100 ? rowKey : rowKey.substring(0, 100)) : rowKey);
		record.setRowData(objectToString(rowData));
		record.setDetails(details);
		record.setShortDetails(shortDetails);
		record.setDetailType(detailType);
		record.store();
	}

	public static String createDetailWithExceptionId(DSLContext dslContext, ULong executionId, String rowKey,
			RowDataObject rowData, String details, String shortDetails, MiscJobExecutionDetailsDetailType detailType) {

		String exceptionId = ExceptionUtil.createExceptionId();
		createDetail(dslContext, executionId, rowKey, rowData, details + " - Exception ID: " + exceptionId,
				shortDetails, detailType);

		return exceptionId;
	}

	public static boolean createDetailIfMissingField(DSLContext dslContext, ULong executionId, String rowKey,
			Map<String, Object> data, RowDataObject rowData, String shortDetails,
			MiscJobExecutionDetailsDetailType detailType, String... fields) {

		for (String each : fields) {

			if (data != null && data.containsKey(each) && data.get(each) != null)
				continue;

			createDetail(dslContext, executionId, rowKey, rowData, each + " is missing.", each + " is missing.", detailType);
			return true;
		}

		return false;
	}

	public static String objectToString(Object userObject) {
		if (userObject == null)
			return null;

		ObjectMapper om = new ObjectMapper();
		String value = null;
		try {
			value = om.writeValueAsString(userObject);
		} catch (JsonProcessingException e) {
			logger.error("Unable to convert {} to string.", userObject, e);
		}
		return value;
	}
}
