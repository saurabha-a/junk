package com.unilog.prime.commons.converter.json.deserializer;

import java.io.IOException;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdScalarDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;

@Component
public class WhiteSpaceRemover extends SimpleModule {
	private static final long serialVersionUID = -170393251438622997L;

	public WhiteSpaceRemover() {
		addDeserializer(String.class, new StdScalarDeserializer<String>(String.class) {
			private static final long serialVersionUID = 695908752518274508L;

			@Override
			public String deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
				return jp.getValueAsString().trim();
			}
		});
	}
}
