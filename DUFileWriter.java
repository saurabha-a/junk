package com.unilog.prime.downloadupload.writer;

import java.io.Closeable;
import java.io.File;
import java.io.Flushable;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import com.unilog.prime.downloadupload.DUFileFormat;

public abstract class DUFileWriter<V, M extends Map<String, V>> implements Closeable, Flushable{

	protected DUFileFormat fileType;
	protected Path filePath;
	
	public DUFileWriter(DUFileFormat fileType, Path filePath) throws IOException {
		
		this.fileType = fileType;
		this.filePath = filePath;
	}
	
	public DUFileWriter(DUFileFormat fileType, File file) throws IOException {
		
		this(fileType, Paths.get(file.getAbsolutePath()));
	}
	
	public DUFileWriter(DUFileFormat fileType) throws IOException {
		
		this(fileType, (Path) null);
	}
	
	public abstract void write(M map) throws IOException;
}
