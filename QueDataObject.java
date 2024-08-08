package com.unilog.prime.etl2.model;

import com.unilog.cx1.pim.commons.model.PimDataObject;
import lombok.Data;
import lombok.experimental.Accessors;
import org.jooq.types.ULong;

import java.util.List;

import static com.unilog.prime.commons.util.StringUtil.safeValueOf;

@Data
@Accessors(chain = true)
public class QueDataObject extends PimDataObject {

	private static final long serialVersionUID = -1197772135930774320L;

	private ULong etlId;
	private Integer stepNumber;
	private String nextExchangeName;
	private List<String> nextRoutingKey;
	private Boolean dummyPill;

	public static QueDataObject createDummyPill(ULong executionId2, String exchange, List<String> nextRoutingKey2,
                                                int stepNumber, ULong clientId2, ULong userId) {

		QueDataObject queDataObject = new QueDataObject();
		queDataObject.setDummyPill(true).setNextExchangeName(exchange)
				.setNextRoutingKey(nextRoutingKey2).setStepNumber(stepNumber).setExecutionId(executionId2)
				.setUserId(userId);
		queDataObject.setClientId(clientId2);
		return queDataObject;
	}

	public static QueDataObject createDummyPillFromQDO(QueDataObject qdo) {

		return QueDataObject.createDummyPill(qdo.getExecutionId(), qdo.getNextExchangeName(), qdo.getNextRoutingKey(),
				qdo.getStepNumber() + 1, qdo.getClientId(), qdo.getUserId());
	}

	public Boolean getDummyPill() {
		if (dummyPill == null)
			return Boolean.FALSE;
		return dummyPill;
	}

	public String getValue(String name) {
		return safeValueOf(data.get(name));
	}
}
