package com.unilog.prime.commons.util;

import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ofPattern;

import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.util.StdDateFormat;

public class DateUtil {

	public static final String YYYYMMDD_DATE_FORMAT = "yyyy-MM-dd";
	public static final String MMDDYYYY_DATE_FORMAT = "MM/dd/yyyy";
	public static final Logger logger = LoggerFactory.getLogger(DateUtil.class);
	public static final String UI_DATE_FORMAT = "dd-MMM-yyyy hh:mm a";
	public static final String MYSQL_TIMESTAMP_FORMAT = "yyyy-MM-dd HH:mm:ss";
	public static final String MYSQL_TIMESTAMP_FORMAT_WITH_MILLISEC = "yyyy-MM-dd HH:mm:ss.SSS";
	public static final String DDMMYY_DATE_FORMAT = "dd-MM-yy";
	public static final SimpleDateFormat ISO8061_DATE_FORMAT = new SimpleDateFormat(
			StdDateFormat.DATE_FORMAT_STR_ISO8601);
	public static final String DDMMYYYY_DATE_FORMAT = "dd-MM-yyyy";
	public static final String DDMMMYYYY_DATE_FORMAT = "dd-MMM-yyyy";
	public static final String DDMMYYYY_HHMMSS_DATE_FORMAT = "dd-MMM-yyyy HH:mm:ss.SSS";
	public static final String UTC_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS";
	public static final String MMM_DD_YYYY_DATE_FORMAT = "MMM dd, yyyy";
	public static final String UTC_DATE_FORMAT_WITH_ZONEOFFSET = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";


	static {
		ISO8061_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	public static ZonedDateTime toUtcZonedDateTime(String inputDateTime, String inputFormat, String timeZone) {
		ZoneId zoneId = TimeZone.getTimeZone(timeZone).toZoneId();
		LocalDateTime localDateAndTime = LocalDateTime.parse(inputDateTime, ofPattern(inputFormat));
		ZonedDateTime dateAndTimeInTimeZone = ZonedDateTime.of(localDateAndTime, zoneId);
		ZonedDateTime utcDate = dateAndTimeInTimeZone.withZoneSameInstant(UTC);
		return utcDate;
	}

	public static String toUtc(String inputDateTime, String inputFormat, String timeZone, String outputFormat) {
		return toUtcZonedDateTime(inputDateTime, inputFormat, timeZone).format(ofPattern(outputFormat));
	}

	public static Timestamp toUtcTimeStamp(String inputDateTime, String inputFormat, String timeZone) {
		return Timestamp.from(toUtcZonedDateTime(inputDateTime, inputFormat, timeZone).toInstant());
	}

	public static String toUTCFromISO8061(String sDateInISO8061, String format) {
		try {
			Date isoDate = DateUtil.getISO8061DateFormat(sDateInISO8061);
			return ZonedDateTime.ofInstant(Instant.ofEpochSecond(isoDate.getTime() / 1000), UTC)
					.format(ofPattern(format));
		} catch (Exception e) {
			logger.error("Unable to parse date = " + sDateInISO8061);
			return null;
		}
	}

	public static Date getISO8061DateFormat(String sDate) throws ParseException {
		return ISO8061_DATE_FORMAT.parse(sDate);
	}

	public static String getISO8061DateAsString(Date date) {
		return ISO8061_DATE_FORMAT.format(date);
	}

	/**
	 * Returns current date and time in java.uti.Date with ISO 8601 Date Format
	 *
	 * @return
	 */
	public static Date getCurrentDateAndTimeAsDate() {
		return Date.from(Instant.now());
	}

	/**
	 * Returns current date and time as String with ISO 8601 Date Format
	 *
	 * @return
	 */
	public static String getCurrentDateAndTimeAsString() {
		return Instant.now().toString();
	}

	public static java.sql.Date getSqlDateAsString(String sDate, String format) throws ParseException {
		java.util.Date dateParsed = DateUtil.getStringAsDate(sDate, format);
		return new java.sql.Date(dateParsed.getTime());
	}

	public static java.sql.Time getSqlTimeAsString(String sDate, String format) throws ParseException {
		java.util.Date dateParsed = DateUtil.getStringAsDate(sDate, format);
		return new java.sql.Time(dateParsed.getTime());
	}

	public static java.sql.Timestamp getSqlTimestampAsString(String sDate, String format) throws ParseException {
		java.util.Date dateParsed = DateUtil.getStringAsDate(sDate, format);
		return new java.sql.Timestamp(dateParsed.getTime());
	}

	public static String getDateAsString(Date date, String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.format(date);
	}

	public static String getDateAsString(Date date) {
		return getISO8061DateAsString(date);
	}

	public static Date getDateAsString(String date, String format) {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		try {
			return sdf.parse(date);
		} catch (ParseException e) {
			return null;
		}
	}

	public static Date getStringAsDate(String dateString) throws ParseException {
		return getISO8061DateFormat(dateString);
	}

	public static Date getStringAsDate(String dateString, String format) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		return sdf.parse(dateString);
	}

	public static Date getStringAsDate(String dateString, String format, TimeZone timezone) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		sdf.setTimeZone(timezone);
		return sdf.parse(dateString);
	}

	public static Date getDateFromStringUsingDefaultFormat(String sDate) throws ParseException {
		return getISO8061DateFormat(sDate);
	}

	public static String getDateAsStringUsingDefaultFormat(Date date) {
		return getISO8061DateAsString(date);
	}

	public static Date getDate(int year, int month, int dayOfMonth) {
		Calendar cal = Calendar.getInstance();
		Date date = null;
		int lMonth = month;
		if (lMonth != 0 && dayOfMonth != 0 && year != 0) {
			if (lMonth > 0)
				lMonth--;
			cal.set(year, lMonth, dayOfMonth);
			date = cal.getTime();
		}
		return date;
	}

	public static Date getUTCDateFromUICalendarDate(String date) {
		DateFormat formatterUTC = new SimpleDateFormat("dd-MMM-yyyy hh:mm a z");
		try {
			return formatterUTC.parse(date + " UTC");
		} catch (ParseException e) {
			logger.error("Unable to parse the date : {}", date, e);
		}
		return null;
	}

	public static String getCurrentDateInUtc(String dateFormat) {
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		return sdf.format(new Date());
	}

	public static String convertDate(String date, String dateTimeFormat) {
		try {
			long epoch = new java.text.SimpleDateFormat(dateTimeFormat).parse(date).getTime();
			return new java.text.SimpleDateFormat(dateTimeFormat).format(new java.util.Date(epoch));
		} catch (Exception e) {
			logger.error("Could not parse date {}", date);
		}
		return null;
	}

	public static String formatDate(String date, String initDateFormat, String endDateFormat) throws ParseException {

		Date initDate = new SimpleDateFormat(initDateFormat).parse(date);
		SimpleDateFormat formatter = new SimpleDateFormat(endDateFormat);
		return formatter.format(initDate);
	}

	public static Timestamp convertDateIntoSQLDate(String dateAdded, String format) {

		try {
			return new Timestamp(new SimpleDateFormat(format).parse(dateAdded).getTime());
		} catch (Exception e) {
			logger.error("Exception converting date : {}", dateAdded, e);
		}
		return null;
	}

	private DateUtil() {

	}
}
