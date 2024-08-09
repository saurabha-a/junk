package com.unilog.prime.downloadupload.reader;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.springframework.web.multipart.MultipartFile;

import com.unilog.prime.downloadupload.DUFileFormat;

public abstract class DUFileReader<V, T extends Map<String, V>> implements AutoCloseable {

	protected DUFileFormat fileType;
	protected boolean hasHeader = false;
	protected boolean hasPreHeader = false;
	protected Path filePath;
	protected MultipartFile multipart;

	public DUFileReader(boolean hasHeader, Path filePath) throws IOException {

		String fileName = filePath.getFileName().toString();
		this.fileType = DUFileFormat.valueOf(fileName.substring(fileName.lastIndexOf('.') + 1).toUpperCase());
		this.filePath = filePath;
		this.hasHeader = hasHeader;
	}

	public DUFileReader(boolean hasHeader, File file) throws IOException {
		this(Paths.get(file.getAbsolutePath()));
	}
	
	public DUFileReader(boolean hasHeader, MultipartFile multipart) throws IOException {
		
		String fileName = multipart.getOriginalFilename();
		this.fileType = DUFileFormat.valueOf(fileName.substring(fileName.lastIndexOf('.') + 1).toUpperCase());
		this.multipart = multipart;
		this.hasHeader = hasHeader;
	}

	public DUFileReader(boolean preHeaders, boolean hasHeader, MultipartFile multipart) throws IOException {

		String fileName = multipart.getOriginalFilename();
		this.fileType = DUFileFormat.valueOf(fileName.substring(fileName.lastIndexOf('.') + 1).toUpperCase());
		this.multipart = multipart;
		this.hasHeader = hasHeader;
		this.hasPreHeader = preHeaders;
	}
	
	public DUFileReader(Path filePath) throws IOException {
		this(true, filePath);
	}

	public DUFileReader(File file) throws IOException {
		this(true, file);
	}
	
	public DUFileReader(MultipartFile multipart) throws IOException {
		this(true, multipart);
	}

	public abstract T read() throws IOException;
}
