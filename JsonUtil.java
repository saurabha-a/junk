package com.unilog.prime.commons.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

	private static final Logger logger = LoggerFactory.getLogger(JsonUtil.class);

	private JsonUtil() {}
	
	public static Map<String, Object> convertJsonToMap(String jsonString) {
		Map<String, Object> map = new HashMap<>();
		if(StringUtils.isNotBlank(jsonString)) {
			try {
				map = new ObjectMapper().readValue(jsonString, new TypeReference<Map<String, Object>>() {
				});
			} catch (IOException e) {
				logger.error("Exception occurred while converting to json {}", e);
			}
		}
		return map;
	}
	
	public static List<Map<String, Object>> convertJsonToListMap(String jsonString) {
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		if (StringUtils.isNotBlank(jsonString)) {
			try {
				list = new ObjectMapper().readValue(jsonString, new TypeReference<List<Map<String, Object>>>() {
				});
			} catch (IOException e) {
				logger.error("Exception occurred while converting to json {}", e);
			}
		}
		return list;
	}


	public static Object[] convertJsonToArray(String jsonString) {
		Object[] arry = null;
		if(StringUtils.isNotBlank(jsonString)) {
			try {
				arry = new ObjectMapper().readValue(jsonString, new TypeReference<Object[]>() {
				});
			} catch (IOException e) {
				logger.error("Exception occurred while converting to json {}", e);
			}
		}
		return arry;
	}



	@SuppressWarnings("rawtypes")
	public static String convertMapToJsonString(Map map) {
		String string = "";
		try {
			string = new ObjectMapper().writeValueAsString(map);
		} catch (IOException e) {
			logger.error("Exception occurred while converting to String {}", e);
		}
		return string;
	}
}
