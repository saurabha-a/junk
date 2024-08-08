package com.unilog.cx1.pim.commons.model;

import java.util.Map;

import org.jooq.types.ULong;

import com.unilog.prime.dbcore.domain.AbstractDatabaseDomainModel;
import com.unilog.prime.misc.jooq.enums.MiscJobJobCategory;
import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode(callSuper=true)
public class MiscJobModel extends AbstractDatabaseDomainModel<ULong> {

	private static final long serialVersionUID = 1775266344230645438L;
	
	private ULong clientId;
	private ULong subsetId;
	private String jobName;
	private String jobKey;
	private Map<String, Object> jobParameters; // NOSONAR
	private MiscJobJobCategory jobCategory;
	private Map<String, Object> schedule;
}
