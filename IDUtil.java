package com.unilog.prime.commons.util;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;


public class IDUtil {

	private static final String NULL_VALUE_STRING = "__NULL__";

	public static String createHash(Object... strings) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");

            // Concatenate strings with a delimiter
            String concatenated = Arrays.stream(strings)
                    .map(e -> e == null ? NULL_VALUE_STRING : e.toString())
                    .collect(Collectors.joining());

            // Compute hash
            byte[] hashBytes = digest.digest(concatenated.getBytes(StandardCharsets.UTF_8));

            // Convert byte array to a hex string
            StringBuilder hexString = new StringBuilder();
            for (byte b : hashBytes) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }

            return hexString.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
            return null;
        }
    }

	public static String shortUUID() {
		UUID uuid = UUID.randomUUID();
		long l = ByteBuffer.wrap(uuid.toString().getBytes()).getLong();
		return Long.toString(l, Character.MAX_RADIX);
	}

	private IDUtil() {
	}
}
