package com.unilog.prime.commons.util;

import java.util.Map;
import java.util.WeakHashMap;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Component;

@Component
public class CaptchaValidator {

	@Autowired
	private PasswordEncoder passwordEncoder;

	@Value("${captcha.encrypt.key}")
	private String captchaKey;

	protected static final Map<String, String> WEEK_HASH_MAP = new WeakHashMap<>();

	public boolean isValidCaptha(String captchaValue, String key) {

		boolean flag = false;
		long minutes = System.currentTimeMillis() / 60000;

		for (int i = 0; i <= 3; i++) {
			String encodedKey = CommonUtil.encrypt(captchaValue + (minutes - i), captchaKey);
			if (WEEK_HASH_MAP.containsKey(encodedKey))
				break;
			flag = passwordEncoder.matches(encodedKey, key);
			if (flag) {
				WEEK_HASH_MAP.put(encodedKey, encodedKey);
				return flag;
			}
		}

		return flag;
	}

}
