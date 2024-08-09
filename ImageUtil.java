package com.unilog.prime.commons.util;

import java.awt.Color;
import java.awt.Graphics2D;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImageUtil {
	
	private static final Logger logger = LoggerFactory.getLogger(ImageUtil.class);

	public static BufferedImage padWhiteBandsToMakeSquare(File file) throws IOException {
		
		BufferedImage image = ImageIO.read(file);
		return padWhiteBandsToMakeSquare(image);
	}
	
	public static BufferedImage padWhiteBandsToMakeSquare(BufferedImage image) {
		
		int max = Integer.max(image.getWidth(), image.getHeight());
		int imageType = image.getType();
		BufferedImage newImage = new BufferedImage(max, max, imageType == 0 ? 5 : imageType);
		Graphics2D graphics = newImage.createGraphics();
		graphics.setColor(Color.WHITE);
		graphics.fillRect(0, 0, max, max);
		graphics.drawImage(image, (max - image.getWidth()) / 2, (max - image.getHeight()) / 2, null);
		graphics.dispose();
		
		return newImage;
	}
	
	public static InputStream bufferedImageToInputStream(BufferedImage image, InputStream is) throws IOException {
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ImageIO.write(image, ImageUtil.getFormatName(is), bos);
		return new ByteArrayInputStream(bos.toByteArray());
	}
	
	public static InputStream bufferedImageToInputStream(BufferedImage image, String format) throws IOException {
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ImageIO.write(image, format, bos);
		return new ByteArrayInputStream(bos.toByteArray());
	}
	
	public static String getFormatName(Object o) {
		try {
			ImageInputStream iis = ImageIO.createImageInputStream(o);
			Iterator<ImageReader> iter = ImageIO.getImageReaders(iis);
			if (!iter.hasNext()) {
				return null;
			}
			ImageReader reader = (ImageReader) iter.next();
			iis.close();

			return reader.getFormatName();
		} catch (IOException e) {
			logger.error("Error while getting format name:: {}",e);
		}
		return null;
	}
	
	private ImageUtil() {}
}
