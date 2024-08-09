package com.unilog.prime.commons.service;

import java.io.Serializable;
import java.util.Map;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import com.unilog.prime.commons.model.AbstractDomainModel;

public interface IBaseDomainService<D extends AbstractDomainModel<I>, I extends Serializable> {

	Page<D> list(Pageable pageable);

	Page<D> list(Pageable pageable, Map<String, Object> searchQuery);

	D create(D entity);

	D put(D entity);

	D update(I id, Map<String, Object> updateFields);

	void delete(I id);

	D getById(I id);

	boolean exists(I id);
}
