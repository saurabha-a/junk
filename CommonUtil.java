package com.unilog.prime.commons.util;

import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.join;

import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CommonUtil {

    private static final Logger logger = LoggerFactory.getLogger(CommonUtil.class);

    public static String decrypt(String ftpPassword, String ftpKey) {
        try {
            Key aesKey = new SecretKeySpec(ftpKey.getBytes(), "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.DECRYPT_MODE, aesKey);
            return new String(cipher.doFinal(Base64.getDecoder().decode(ftpPassword)));
        } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException | IllegalBlockSizeException
                | BadPaddingException e) {
            logger.error("Exception while decrypting the password {}", ftpPassword, e);
        }
        return ftpPassword;
    }

    public static String encrypt(String ftpPassword, String ftpKey) {
        try {
            Key aesKey = new SecretKeySpec(ftpKey.getBytes(), "AES");
            Cipher cipher = Cipher.getInstance("AES");
            cipher.init(Cipher.ENCRYPT_MODE, aesKey);
            byte[] encrypted = cipher.doFinal(ftpPassword.getBytes());
            return new String(Base64.getEncoder().encode(encrypted));
        } catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException | IllegalBlockSizeException
                | BadPaddingException e) {
            logger.error("Exception while encrypting the password {}", ftpPassword, e);
        }
        return ftpPassword;
    }

    public static String getTimeDiff(Date dateOne, Date dateTwo) {
        String diff = "";
        long timeDiff = Math.abs(dateOne.getTime() - dateTwo.getTime());
        diff = String.format("%d hour(s) %d min(s) %d sec(s)", TimeUnit.MILLISECONDS.toHours(timeDiff),
                TimeUnit.MILLISECONDS.toMinutes(timeDiff)
                        - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(timeDiff)),
                TimeUnit.MILLISECONDS.toSeconds(timeDiff)
                        - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(timeDiff)));
        return diff;
    }

    public static final String SHA256(Map<String, String> record) { // NOSONAR
        String joinedString = join(record.values().toArray());
        return DigestUtils.sha256Hex(joinedString);
    }

    public static Map<String, Object> flatMapToNestedMap(Map<String, ?> allParams) {

        final Map<String, Object> retMap = new HashMap<>();

        allParams.entrySet().stream().forEach(e -> {

            List<String> splitPath = splitPath(e.getKey());
            Object value;

            if (e.getValue() instanceof String[]) {
                String[] arr = (String[]) e.getValue();
                value = arr.length == 1 ? arr[0] : Arrays.asList(arr);
            } else
                value = e.getValue();

            putDataInSplitPath(splitPath, retMap, value);
        });

        return retMap;
    }

    @SuppressWarnings("unchecked")
    private static void putDataInSplitPath(List<String> path, Map<String, Object> retMap, Object value) {

        if (path == null || path.isEmpty())
            return;

        Object parent = retMap;
        Object data = retMap;
        for (int i = 0; i < path.size(); i++) {

            if (data == null) {
                if (path.get(i).startsWith("["))
                    data = new ArrayList<>();
                else
                    data = new HashMap<String, Object>();
            }

            String tp = path.get(i);
            if (tp.indexOf('[') != -1)
                tp = tp.substring(1, tp.length() - 1);
            parent = data;

            data = nextLevel(path, parent, i, tp);

        }
        String tp = path.get(path.size() - 1);
        if (tp.indexOf('[') != -1)
            tp = tp.substring(1, tp.length() - 1);

        if (parent instanceof List) {
            List<Object> list = ((List<Object>) parent);
            int index = Integer.parseInt(tp);
            increaseList(list, index);
			if (list.size() > index && list.get(index) == null) {
				list.remove(index);
			}
            list.add(index, value);
        } else
            ((Map<String, Object>) parent).put(tp, value);

    }

    @SuppressWarnings("unchecked")
    private static Object nextLevel(List<String> path, Object parent, int i, String tp) {
        Object data;
        if (parent instanceof List<?>) {
            List<?> list = ((List<?>) parent);
            int index = Integer.parseInt(tp);
            if (list.size() > index)
                data = list.get(index);
            else
                data = null;
        } else
            data = ((Map<String, Object>) parent).get(tp);

        if (data == null && (i + 1) != path.size()) {
            if (path.get(i + 1).startsWith("["))
                data = new ArrayList<>();
            else
                data = new HashMap<>();
            if (parent instanceof List<?>) {
                List<Object> list = ((List<Object>) parent);
                int index = Integer.parseInt(tp);
                increaseList(list, index);
				if (list.size() > index && list.get(index) == null) {
					list.remove(index);
				}
                list.add(index, data);
            } else
                ((Map<String, Object>) parent).put(tp, data);
        }
        return data;
    }

    private static void increaseList(List<Object> list, int index) {
        if (index < list.size())
            return;

        for (int i = list.size(); i < index; i++) {
            list.add(null);
        }
    }

    private static List<String> splitPath(String srcPath) {

        String[] path = srcPath.split("\\.");
        List<String> finalPath = new ArrayList<>();
        for (int i = 0; i < path.length; i++) {

            if (path[i].indexOf('[') == -1) {
                finalPath.add(path[i]);
                continue;
            }

            String[] parts = path[i].split("\\[");

            for (int j = 0; j < parts.length; j++) {

                if (parts[j].trim().isEmpty())
                    continue;

                String tp = parts[j];
                if (tp.indexOf(']') != -1) {
                    finalPath.add("[" + tp);
                } else
                    finalPath.add(tp);
            }
        }
        return finalPath;
    }

    public static boolean canDeleteField(Object fieldValue) {
    	
    	 if (fieldValue == null) {
             return  true;
         }
    	 
    	 if(isBlank(fieldValue.toString())) {
             return true;
         }
    	 
         return equalsIgnoreCase(fieldValue.toString(), "null");
    }
    
    private CommonUtil() {}
}