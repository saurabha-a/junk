package com.unilog.prime.commons.service.impl;

import java.io.Serializable;

import org.springframework.core.GenericTypeResolver;

import com.unilog.prime.commons.model.AbstractDomainModel;
import com.unilog.prime.commons.service.IBaseDomainService;

public abstract class AbstractDomainServiceImpl<P extends AbstractDomainModel<I>, 
				I extends Serializable>  
				extends AbstractCoreServiceImpl
				implements IBaseDomainService<P, I> {
	
	protected final Class<P> pojoClass;
	
	@SuppressWarnings("unchecked")
	public AbstractDomainServiceImpl() {
		Class<?>[] classArray = GenericTypeResolver.resolveTypeArguments(getClass(), AbstractDomainServiceImpl.class);
		this.pojoClass = (Class<P>) classArray[0];		
	}
}


