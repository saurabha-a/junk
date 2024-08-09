package com.unilog.prime.commons.web.controller;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractRestController {
	
	protected final Logger logger;
	
	@Autowired
	protected ObjectMapper objectMapper;

	protected String controllerMappingFullString;
	
	protected String resourceName;
	
	public AbstractRestController() {
		this.logger = LoggerFactory.getLogger(this.getClass());
		controllerMappingFullString = "/"+this.getClass().getAnnotation(RequestMapping.class).value()[0];
		int lastIndex = controllerMappingFullString.lastIndexOf('/');
		this.resourceName = controllerMappingFullString.substring(lastIndex+1);
	}
	
	protected String mapToJson(Object value) throws JsonProcessingException {
		return this.objectMapper.writeValueAsString(value);
	}
	
	protected  <T> T mapFromJson(String jsonString,Class<T> clazz) throws IOException {
		return this.objectMapper.readValue(jsonString,clazz);
	}

	/**
	 * @return the objectMapper
	 */
	protected ObjectMapper getObjectMapper() {
		return objectMapper;
	}
}
