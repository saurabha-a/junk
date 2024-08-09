package com.unilog.prime.dbcore.web.controller;

import java.beans.PropertyEditorSupport;
import java.util.Map;

import org.jooq.types.ULong;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.RequestBody;

import com.unilog.prime.commons.constant.CommonAPIConstants;
import com.unilog.prime.commons.enumeration.PrimeResponseCode;
import com.unilog.prime.commons.exception.PrimeException;
import com.unilog.prime.commons.model.AbstractDomainModel;
import com.unilog.prime.commons.service.IBaseDomainService;
import com.unilog.prime.commons.web.controller.AbstractDomainRestV1Controller;

public abstract class AbstractDatabaseRestV1Controller <D extends AbstractDomainModel<ULong>, 
				S extends IBaseDomainService<D, ULong>> 
				extends AbstractDomainRestV1Controller<D, ULong, S> {
	
	public AbstractDatabaseRestV1Controller() {
		super();
	}

	@InitBinder
	public void initBinder(final WebDataBinder binder){		
	  binder.registerCustomEditor(ULong.class, new PropertyEditorSupport() {
		  @Override
	      public void setAsText (String text) {
		   if(text == null) setValue(null);
		   setValue(ULong.valueOf(text));
		  }
	  });
	}
	
	@PatchMapping
	public ResponseEntity<D> patchWithOutId(@RequestBody Map<String, Object> entityMap) {
		if (!entityMap.containsKey(CommonAPIConstants.ID))
			throw new PrimeException(HttpStatus.BAD_REQUEST, PrimeResponseCode.REQUIRED_FIELD.getResponseMsg(),
					PrimeResponseCode.REQUIRED_FIELD.getResponseCode(), "id is missing in the request");
		ULong id = ULong.valueOf(entityMap.get(CommonAPIConstants.ID).toString());
		//Removing id from entityMap as id is restricted field from update
		entityMap.remove(CommonAPIConstants.ID);
		D patchedEntity = dbService.update(id, entityMap);
		return ResponseEntity.ok(patchedEntity);
	}
}
