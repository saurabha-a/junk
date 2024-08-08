package com.unilog.cx1.pim.commons.model;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jooq.types.UInteger;
import org.jooq.types.ULong;

import com.unilog.prime.dbcore.domain.AbstractDatabaseDomainModel;
import com.unilog.prime.misc.jooq.enums.MiscJobExecutionFtpStatus;
import com.unilog.prime.misc.jooq.enums.MiscJobExecutionStatus;

import lombok.Data;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper=true)
public @Data class MiscJobExecutionModel extends AbstractDatabaseDomainModel<ULong> {

	private static final long serialVersionUID = 7106556855320438371L;
	
	private ULong miscJobId;
	private MiscJobExecutionStatus status;
	private UInteger counter0 = UInteger.valueOf(0);
	private UInteger counter1 = UInteger.valueOf(0);
	private UInteger counter2 = UInteger.valueOf(0);
	private UInteger counter3 = UInteger.valueOf(0);
	private UInteger counter4 = UInteger.valueOf(0);
	private UInteger counter5 = UInteger.valueOf(0);
	private UInteger counter6 = UInteger.valueOf(0);
	private UInteger counter7 = UInteger.valueOf(0);
	private UInteger totalSteps = UInteger.valueOf(0);
	private Map<String, Object> executionParameters = new HashMap<>(); // NOSONAR
	private MiscJobExecutionFtpStatus ftpStatus;
	private Byte readingEnd = (byte) 0;
	private List<String> files;

	private transient MiscJobModel miscJob;

}
