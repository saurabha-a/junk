package com.unilog.prime.downloadupload.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.springframework.web.multipart.MultipartFile;

import com.monitorjbl.xlsx.StreamingReader;
import com.unilog.prime.commons.util.CSVParseUtil;
import com.unilog.prime.commons.util.TSVParseUtil;
import com.unilog.prime.downloadupload.DUFileFormat;

public class FlatFileReader extends DUFileReader<String, Map<String, String>> {

	private boolean initialized = false;
	private Iterator<Row> iterator;
	private Workbook workbook;
	private BufferedReader reader;
	private List<String> header;

	public FlatFileReader(boolean hasHeader, File file) throws IOException {
		super(hasHeader, file);
	}

	public FlatFileReader(boolean hasHeader, Path path) throws IOException {
		super(hasHeader, path);
	}
	
	public FlatFileReader(boolean hasHeader, MultipartFile multipart) throws IOException {
		super(hasHeader, multipart);
	}

	public FlatFileReader(boolean preHeaders, boolean hasHeader, MultipartFile multipart) throws IOException {
		super(preHeaders, hasHeader, multipart);
	}

	public FlatFileReader(File file) throws IOException {
		super(file);
	}

	public FlatFileReader(Path path) throws IOException {
		super(path);
	}
	
	public FlatFileReader(MultipartFile multipart) throws IOException {
		super(multipart);
	}

	@Override
	public Map<String, String> read() throws IOException {

		if (this.fileType == DUFileFormat.CSV || this.fileType == DUFileFormat.TSV) {

			return this.textFormatFileReader();
		} else if (this.fileType == DUFileFormat.XLS || this.fileType == DUFileFormat.XLSX) {

			return this.microsoftFormatFileReader();
		}

		return null;
	}

	private Map<String, String> textFormatFileReader() throws IOException {

		if (!this.initialized) {
			this.reader = this.getReader();
			this.initialized = true;
		}

		if (this.hasHeader && this.header == null) {

			String line = this.reader.readLine();
			if (line == null)
				return null;
			this.header = this.fileType == DUFileFormat.CSV
					? CSVParseUtil.stringToColumns(line.startsWith("\uFEFF") ? line.substring(1) : line)
					: TSVParseUtil.stringToColumns(line);
		}

		String line = this.reader.readLine();
		if (line == null)
			return null;

		List<String> row = this.fileType == DUFileFormat.CSV ? CSVParseUtil.stringToColumns(line)
				: TSVParseUtil.stringToColumns(line);

		Map<String, String> record = new LinkedHashMap<>();
		for (int i = 0; i < header.size(); i++) {

			String value = row.size() > i ? row.get(i) : null;
			record.put(this.hasHeader ? header.get(i) : "" + i, value);
		}

		return record;
	}

	private Map<String, String> microsoftFormatFileReader() throws IOException {

		if (!this.initialized) {
			try {
				workbook = StreamingReader.builder().rowCacheSize(100).bufferSize(4096).open(this.getInputStream());
				Sheet firstSheet = workbook.getSheetAt(0);
				iterator = firstSheet.iterator();
			} catch (Exception ex) {

				if (ex.getMessage().contains("Office 2003 XML are not supported")) {
					workbook = WorkbookFactory.create(this.getInputStream());
					Sheet firstSheet = workbook.getSheetAt(0);
					iterator = firstSheet.iterator();
				}
			}
			this.initialized = true;
		}

		if (this.hasPreHeader && iterator.hasNext())
			iterator.next();

		if (this.hasHeader && this.header == null) {
			if (iterator.hasNext()) {
				Row row = iterator.next();
				Iterator<Cell> cellIterator = row.cellIterator();
				int cellIndex = 0;
				this.header = new ArrayList<String>();
				while (cellIterator.hasNext()) {
					Cell cell = row.getCell(cellIndex++, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
					String cellValue = null;
					if (cell != null) {
						cellValue = cell.getStringCellValue() == null ? null
								: cell.getStringCellValue().trim().toUpperCase();
						if (cellValue.isEmpty())
							cellValue = null;
					}
					this.header.add(cellValue);
					cellIterator.next();
				}
			}
		}

		if (!iterator.hasNext())
			return null;

		Row row = iterator.next();
		Map<String, String> record = new LinkedHashMap<>();
		for (int i = 0; i < header.size(); i++) {
			Cell cell = row.getCell(i, Row.MissingCellPolicy.RETURN_BLANK_AS_NULL);
			String columnValue = cell == null ? null : cell.getStringCellValue().trim();
			record.put(this.hasHeader ? header.get(i) : "" + i, columnValue);
		}
		return record;
	}

	private InputStream getInputStream() throws IOException {

		if (this.filePath != null)
			return Files.newInputStream(this.filePath, StandardOpenOption.READ);
		else if (this.multipart != null)
			return this.multipart.getInputStream();

		return null;
	}

	private BufferedReader getReader() throws IOException {

		if (this.filePath != null)
			return Files.newBufferedReader(this.filePath, StandardCharsets.UTF_8);
		else if (this.multipart != null)
			return new BufferedReader(new InputStreamReader(this.multipart.getInputStream()));

		return null;
	}

	@Override
	public void close() throws Exception {

		if (this.workbook != null)
			workbook.close();
		else if (this.reader != null)
			reader.close();
	}

	public List<String> getHeaders() {
		return this.header;
	}
}
