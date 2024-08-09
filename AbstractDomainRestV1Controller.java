package com.unilog.prime.commons.web.controller;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.GenericTypeResolver;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.unilog.prime.commons.constant.CommonAPIConstants;
import com.unilog.prime.commons.enumeration.PrimeResponseCode;
import com.unilog.prime.commons.exception.PrimeException;
import com.unilog.prime.commons.model.AbstractDomainModel;
import com.unilog.prime.commons.model.Tuple;
import com.unilog.prime.commons.service.IBaseDomainService;

import io.swagger.annotations.ApiParam;

public abstract class AbstractDomainRestV1Controller<D extends AbstractDomainModel<I>, I extends Serializable, S extends IBaseDomainService<D, I>>
		extends AbstractRestController {

	@Autowired
	protected S dbService;

	protected final Class<D> entityType;

	@SuppressWarnings("unchecked")
	public AbstractDomainRestV1Controller() {
		super();
		Class<?>[] classArray = GenericTypeResolver.resolveTypeArguments(getClass(),
				AbstractDomainRestV1Controller.class);
		this.entityType = (Class<D>) classArray[0];
	}

	protected URI createLocation(D entity) {
		return URI.create(controllerMappingFullString + "/" + entity.getId());
	}

	@GetMapping()
	public ResponseEntity<Page<D>> findAll(Pageable pageable, HttpServletRequest request) {
		pageable = (pageable == null ? PageRequest.of(0, 10, Direction.ASC, "id") : pageable);
		Map<String, Object> parameterMapToMap = this.parameterMapToMap(request.getParameterMap());
		Page<D> list = this.dbService.list(pageable, parameterMapToMap);
		return ResponseEntity.ok(list);
	}

	protected Map<String, Object> parameterMapToMap(Map<String, String[]> parameterMap) {

		return parameterMap.entrySet().stream().map(e ->{
			String[] value = e.getValue();
			if (value.length == 0) return new Tuple<String, Object>(e.getKey(), "");
			return new Tuple<String, Object>(e.getKey(), value.length == 1 ? value[0] : Stream.of(value).collect(Collectors.toList()));
		}).collect(Collectors.toMap(Tuple::getFirstValue, Tuple::getSecondValue));
	}

	@GetMapping(CommonAPIConstants.ID_PATH_VARIABLE)
	public ResponseEntity<D> get(@PathVariable(CommonAPIConstants.ID) final I id, HttpServletRequest request) {
		D resource = this.dbService.getById(id);
		if (resource == null) {
			throw new PrimeException(HttpStatus.NOT_FOUND,
					String.format(PrimeResponseCode.RESOURCE_NOT_FOUND.getResponseMsg(), id),
					PrimeResponseCode.RESOURCE_NOT_FOUND.getResponseCode());
		}
		return ResponseEntity.ok(resource);
	}

	@PostMapping
	public ResponseEntity<D> create(@Valid @RequestBody D entity) {
		D createdEntity = this.dbService.create(entity);
		return ResponseEntity.created(createLocation(createdEntity)).body(createdEntity);
	}

	@PutMapping(path = CommonAPIConstants.ID_PATH_VARIABLE)
	public ResponseEntity<D> put(@PathVariable(CommonAPIConstants.ID) final I id, @RequestBody D entity) {
		if (id != null)
			entity.setId(id);
		return ResponseEntity.ok(this.dbService.put(entity));
	}

	@PatchMapping(CommonAPIConstants.ID_PATH_VARIABLE)
	public ResponseEntity<D> patch(@PathVariable I id,
			@ApiParam(value = "entityMap", required = true, defaultValue = "{\"key\": \"value\"}") @RequestBody Map<String, Object> entityMap) {
		D patchedEntity = dbService.update(id, entityMap);
		return ResponseEntity.ok(patchedEntity);
	}

	@DeleteMapping(CommonAPIConstants.ID_PATH_VARIABLE)
	@ResponseStatus(value = HttpStatus.NO_CONTENT)
	public void delete(@PathVariable(CommonAPIConstants.ID) final I id) {
		this.dbService.delete(id);
	}

}
