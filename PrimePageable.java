package com.unilog.prime.commons.util;

import java.util.List;

import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;

public class PrimePageable<T> extends PageImpl<T>{

	private static final long serialVersionUID = 745894089918005859L;
	
	private Long totalItemCount;
	
	public PrimePageable(List<T> content, Pageable pageable, long total) {
		super(content, pageable, total);
	}
	
	public PrimePageable(List<T> content, Pageable pageable, long total, Long totalItemCount) {
		this(content, pageable, total);
		this.totalItemCount = totalItemCount;
	}

	public Long getTotalItemCount() {
		return totalItemCount;
	}

	public void setTotalItemCount(Long totalItemCount) {
		this.totalItemCount = totalItemCount;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((totalItemCount == null) ? 0 : totalItemCount.hashCode());
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		PrimePageable other = (PrimePageable) obj;
		if (totalItemCount == null) {
			if (other.totalItemCount != null)
				return false;
		} else if (!totalItemCount.equals(other.totalItemCount))
			return false;
		return true;
	}
}
