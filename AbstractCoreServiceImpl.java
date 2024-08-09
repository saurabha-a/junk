package com.unilog.prime.commons.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.ObjectMapper;


public abstract class AbstractCoreServiceImpl {
	
	protected final Logger logger;
	
	@Autowired
	protected ObjectMapper objectMapper;
	
	public AbstractCoreServiceImpl() {
		logger = LoggerFactory.getLogger(this.getClass());
	}
}
