package com.unilog.prime.etl2.service;

import java.util.Map;

import org.jooq.types.ULong;

public interface IETLDefinitionService {

	Map<String, String> getMappedDBHeaders(ULong executionId);

}
