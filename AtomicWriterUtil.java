package com.unilog.prime.commons.util;

import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class AtomicWriterUtil {

	public static void write(Path exportFilePath, char[] row) throws IOException {

		try (FileChannel channel = FileChannel.open(exportFilePath, StandardOpenOption.CREATE,
				StandardOpenOption.APPEND)) {
			channel.write(StandardCharsets.UTF_8.encode(CharBuffer.wrap(row)));
			channel.force(true);
		}
	}

	private AtomicWriterUtil() {
		
	}
}
