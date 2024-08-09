package com.unilog.prime.dbcore.domain;

import java.io.Serializable;

import com.unilog.prime.commons.model.AbstractDomainModel;

public abstract class AbstractDatabaseDomainModel<I extends Serializable> extends AbstractDomainModel<I> {


	private static final long serialVersionUID = 8554574268654780678L;

	protected String statusCode;


	public String getStatusCode() {
		return statusCode;
	}
	
	public void setStatusCode(String statusCode) {
		this.statusCode = statusCode;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((statusCode == null) ? 0 : statusCode.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (!(obj instanceof AbstractDatabaseDomainModel))
			return false;
		@SuppressWarnings("unchecked")
		AbstractDatabaseDomainModel<I> other = (AbstractDatabaseDomainModel<I>) obj;
		if (statusCode == null) {
			if (other.statusCode != null)
				return false;
		} else if (!statusCode.equals(other.statusCode))
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "AbstractDatabaseDomainModel [statusCode=" + statusCode + ", getId()=" + getId() + ", getCreatedBy()="
				+ getCreatedBy() + ", getUpdatedBy()=" + getUpdatedBy() + ", getCreatedAt()=" + getCreatedAt()
				+ ", getUpdatedAt()=" + getUpdatedAt() + "]";
	}
}
