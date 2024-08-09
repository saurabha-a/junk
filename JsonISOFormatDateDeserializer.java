package com.unilog.prime.commons.converter.json.deserializer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

@Component
public class JsonISOFormatDateDeserializer extends JsonDeserializer<Date>{
	
	private Logger logger = LoggerFactory.getLogger(this.getClass());
	
	@Override
	public Date deserialize(JsonParser p, DeserializationContext ctxt)
			throws IOException {
		String sDate = p.getValueAsString();
		Date dt = null;
		if(sDate!=null) {
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-dd-MM");
			try {
				dt = formatter.parse(sDate);
			} catch (ParseException e) {
				logger.error("Parse exception {}", e.getMessage());
			}
		}
		return dt;
	}

}
