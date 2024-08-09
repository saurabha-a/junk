package com.unilog.prime.commons.service.impl;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.awt.Color;
import java.awt.Font;
import java.awt.Graphics2D;
import java.awt.Polygon;
import java.awt.RenderingHints;
import java.awt.font.FontRenderContext;
import java.awt.geom.Rectangle2D;
import java.awt.image.BufferedImage;
import java.awt.image.WritableRaster;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.Random;

import javax.imageio.ImageIO;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

import com.unilog.prime.commons.enumeration.PrimeResponseCode;
import com.unilog.prime.commons.exception.PrimeException;
import com.unilog.prime.commons.model.Captcha;
import com.unilog.prime.commons.service.ICaptchaService;
import com.unilog.prime.commons.util.CommonUtil;

@Service(CaptchaServiceImpl.BEAN_ID)
public class CaptchaServiceImpl implements ICaptchaService {

	public static final String BEAN_ID = "captchaServiceImpl";

	@Autowired
	private PasswordEncoder passwordEncoder;

	@Value("${captcha.encrypt.key}")
	private String captchaKey;

	private static final Logger logger = LoggerFactory.getLogger(CaptchaServiceImpl.class);

	@Override
	public Captcha getCaptcha(int width, int height) {
		Random random = new Random();

		String generatedString = this.getRandomAlphaNumeric(random);

		BufferedImage bufferedImage = this.getRandomImage(random, generatedString, width, height);

		String imageDataString = this.convertToBase64URI(bufferedImage);

		String encryptedKey = this.getEncodedKey(generatedString);

		if (isBlank(encryptedKey))
			throw new PrimeException(HttpStatus.NOT_FOUND,
					PrimeResponseCode.ENCRYPTED_KEY_NOT_FOUND.getResponseMsg(),
					PrimeResponseCode.ENCRYPTED_KEY_NOT_FOUND.getResponseCode());

		return new Captcha(encryptedKey, imageDataString);
	}

	public String getEncodedKey(String generatedString) {

		long minutes = System.currentTimeMillis() / 60000;

		// encrypting alphaNumberic
		String encodedKey = CommonUtil.encrypt(generatedString + minutes, captchaKey);

		return passwordEncoder.encode(encodedKey);
	}

	private String convertToBase64URI(BufferedImage img) {
		String imageDataString = null;
		final ByteArrayOutputStream os = new ByteArrayOutputStream();

		try {
			ImageIO.write(img, "png", os);
			imageDataString = Base64.getEncoder().encodeToString(os.toByteArray());
			logger.info("Image Successfully converted into Base64 String !!!!");
			os.close();
		} catch (IOException e) {
			throw new PrimeException(HttpStatus.NOT_FOUND, PrimeResponseCode.CAPTCHA_NOT_FOUND.getResponseMsg(),
					PrimeResponseCode.CAPTCHA_NOT_FOUND.getResponseCode());
		}

		return "data:image/png;base64,"+imageDataString;
	}

	private BufferedImage getFishyEye(BufferedImage bufferedImage) {
		int width = bufferedImage.getWidth();
		int height = bufferedImage.getHeight();
		int[] startPixels = new int[width * height];
		bufferedImage.getRGB(0, 0, width, height, startPixels, 0, width);

		int[] endPixels = fisheye(startPixels, width, height);

		return transformPixels(bufferedImage, endPixels, width, height);
	}

	private BufferedImage getRandomImage(Random random, String generatedString, int width, int height) {

		Color randomBgColor = getRandomcolor(random);
		Color randomShapeColor = getRandomcolor(random);

		BufferedImage bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
		Graphics2D g2d = bufferedImage.createGraphics();

		g2d.setColor(Color.GREEN);
		Font f = new Font("Arial", Font.BOLD, 30);
		g2d.setFont(f);
		Rectangle2D rect = f.getStringBounds(generatedString, new FontRenderContext(null, true, false));
		int fontSize = (int) (width / rect.getWidth());
		fontSize -= (int) (fontSize / 4);
		f = new Font("Arial", Font.BOLD, (int) ((fontSize * 30)));
		g2d.setFont(f);
		rect = f.getStringBounds(generatedString, new FontRenderContext(null, true, false));
		g2d.setRenderingHint(RenderingHints.KEY_TEXT_ANTIALIASING, RenderingHints.VALUE_TEXT_ANTIALIAS_LCD_HRGB);
		g2d.setColor(Color.BLACK);
		g2d.drawString(generatedString, (int) (width - rect.getWidth()) / 2,
				(int) (((height - rect.getHeight()) / 2) + (rect.getHeight() * 3 / 4)));

		g2d.dispose();

		BufferedImage distorted = getFishyEye(bufferedImage);

		bufferedImage = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
		g2d = bufferedImage.createGraphics();
		g2d.setColor(randomBgColor);
		g2d.fillRect(0, 0, width, height);

		getRandomShape(g2d, randomShapeColor, width, height, random);
		g2d.setColor(Color.BLACK);
		g2d.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR);
		g2d.drawImage(distorted, 0, 0, width, height, null);
		g2d.dispose();

		return bufferedImage;

	}

	private void getRandomShape(Graphics2D g2d, Color randomShapeColor, int width, int height, Random random) {
		int n = random.nextInt() % 3;

		for (int i = 0; i < 10; i++) {
			g2d.setColor(randomShapeColor);
			int x = random.nextInt(width + 1);
			int y = random.nextInt(height + 1);
			int shapeWidth = random.nextInt(width / 2);
			int shapeHeight = random.nextInt(height / 2);
			switch (n) {
			case 0:
				g2d.fillOval(x, y, shapeWidth, shapeHeight);
				break;
			case 1:
				g2d.fillRect(x, y, shapeWidth, shapeHeight);
				break;
			case 2:
				g2d.fillPolygon(new Polygon(new int[] { x + shapeWidth / 2, x + shapeWidth, x },
						new int[] { y, y + shapeHeight, y + shapeHeight }, 3));
			default:
				break;
			}// shape switch
		} // end of for loop
	}

	private Color getRandomcolor(Random random) {
		int r = random.nextInt(255);
		int g = random.nextInt(255);
		int b = random.nextInt(255);

		return new Color(r, g, b);
	}

	private String getRandomAlphaNumeric(Random random) {
		int leftLimit = 48; // numeral '0'
		int rightLimit = 122; // letter 'z'
		int targetStringLength = 5;
		return random.ints(leftLimit, rightLimit + 1).filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
				.limit(targetStringLength)
				.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append).toString();
	}

	public int decideSize(Random random, int dimension) {
		return random.nextInt(dimension / 2);
	}

	private BufferedImage transformPixels(BufferedImage bufferedImage, int[] endPixels, int width, int height) {
		int[] imA = new int[endPixels.length];
		int[] imR = new int[endPixels.length];
		int[] imG = new int[endPixels.length];
		int[] imB = new int[endPixels.length];

		for (int i = 0; i < endPixels.length; i++) {
			imA[i] = (endPixels[i] >> 24) & 0x000000FF;
			imR[i] = (endPixels[i] >> 16) & 0x000000FF;
			imG[i] = (endPixels[i] >> 8) & 0x000000FF;
			imB[i] = (endPixels[i]) & 0x000000FF;
		}

		WritableRaster wRaster = bufferedImage.getData().createCompatibleWritableRaster();

		wRaster.setSamples(0, 0, width, height, 3, imA);
		wRaster.setSamples(0, 0, width, height, 0, imR);
		wRaster.setSamples(0, 0, width, height, 1, imG);
		wRaster.setSamples(0, 0, width, height, 2, imB);

		bufferedImage.setData(wRaster);

		return bufferedImage;
	}

	private int[] fisheye(int[] srcpixels, double w, double h) {

		int[] dstpixels = new int[(int) (w * h)];
		for (int y = 0; y < h; y++) {
			double ny = ((2 * y) / h) - 1;
			double ny2 = ny * ny;
			for (int x = 0; x < w; x++) {
				double nx = ((2 * x) / w) - 1;
				double nx2 = nx * nx;
				double r = Math.sqrt(nx2 + ny2);
				if (0.0 <= r && r <= 1.0) {
					double nr = Math.sqrt(1.0 - r * r);
					nr = (r + (1.0 - nr)) / 2.0;
					if (nr <= 1.0) {
						double theta = Math.atan2(ny, nx);
						double nxn = nr * Math.cos(theta);
						double nyn = nr * Math.sin(theta);
						int x2 = (int) (((nxn + 1) * w) / 2.0);
						int y2 = (int) (((nyn + 1) * h) / 2.0);
						int srcpos = (int) (y2 * w + x2);
						if (srcpos >= 0 && srcpos < w * h) {
							dstpixels[(int) (y * w + x)] = srcpixels[srcpos];
						}
					}
				}
			}
		}

		return dstpixels;
	}

}
