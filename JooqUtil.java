package com.unilog.cx1.pim.commons.util;

import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ASSET_TYPE;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ID;
import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.ITEM_ID;
import static com.unilog.prime.jooq.Tables.SI_ITEM_KEYWORD;
import static com.unilog.prime.jooq.Tables.SI_ITEM_PARTNUMBER;
import static org.jooq.impl.DSL.falseCondition;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.trueCondition;

import java.util.List;

import org.jooq.Condition;
import org.jooq.impl.DSL;

public class JooqUtil {

    public final static Condition getItemDeleteCondition(List<String> requiredServices, String itemId) {
        Condition condition = trueCondition();
        condition = condition.and(field(ID).eq(itemId));
        if (requiredServices.contains("item"))
            return condition;
        return falseCondition();
    }

    public final static Condition getItemXFieldDeleteCondition(List<String> requiredServices, String itemId) {
        Condition condition = trueCondition();
        condition = condition.and(field(ITEM_ID).eq(itemId));
        if (requiredServices.contains("xField"))
            return condition;
        return falseCondition();
    }

    public final static Condition getItemAttributeDeleteCondition(List<String> requiredServices, String itemId) {
        Condition condition = trueCondition();
        condition = condition.and(field(ITEM_ID).eq(itemId));
        if (requiredServices.contains("attribute"))
            return condition;
        return falseCondition();
    }

    public final static Condition getItemProductDeleteCondition(List<String> requiredServices, String itemId) {
        Condition condition = trueCondition();
        condition = condition.and(field(ITEM_ID).eq(itemId));
        if (requiredServices.contains("product"))
            return condition;
        return falseCondition();
    }

    public final static Condition getItemAssetDeleteCondition(List<String> requiredServices, String itemId) {
        Condition condition = trueCondition();
        condition = condition.and(field(ITEM_ID).eq(itemId));
        if (requiredServices.contains("asset")) {
            if (requiredServices.contains("asset.image") && requiredServices.contains("asset.document"))
                return condition;
            else if (requiredServices.contains("asset.document") || requiredServices.contains("asset.image")) {
                if (requiredServices.contains("asset.document"))
                    condition = condition.and(field(ASSET_TYPE).eq("DOC"));
                if (requiredServices.contains("asset.image"))
                    condition = condition.and(field(ASSET_TYPE).eq("IMG"));
                return condition;
            }
        }
        return falseCondition();
    }

    public final static Condition getItemPartnumberDeleteCondition(List<String> requiredServices, String itemId) {
        Condition condition = trueCondition();
        condition = condition.and(field(ITEM_ID).eq(itemId));
        if (requiredServices.contains("partnumber")) {
            Condition partnumberCondition = DSL.trueCondition();
            if (requiredServices.contains("partnumber.apn1"))
                partnumberCondition = partnumberCondition.or(field(SI_ITEM_PARTNUMBER.PARTNUMBER_TYPE).eq("ALT1"));
            if (requiredServices.contains("partnumber.apn2"))
                partnumberCondition = partnumberCondition.or(field(SI_ITEM_PARTNUMBER.PARTNUMBER_TYPE).eq("ALT2"));
            if (requiredServices.contains("partnumber.dpn"))
                partnumberCondition = partnumberCondition.or(field(SI_ITEM_PARTNUMBER.PARTNUMBER_TYPE).eq("DPN"));
            return condition.and(partnumberCondition);
        }
        return falseCondition();
    }

	public final static Condition getItemKeywordDeleteCondition(List<String> requiredServices, String itemId) {
		Condition condition = trueCondition();
		condition = condition.and(field(ITEM_ID).eq(itemId));
		if (requiredServices.contains("keyword")) {
			if (requiredServices.contains("keyword.ckw") && requiredServices.contains("keyword.pkw"))
				return condition;
			else if (requiredServices.contains("keyword.ckw") || requiredServices.contains("keyword.pkw")) {
				if (requiredServices.contains("keyword.ckw"))
					condition = condition.and(field(SI_ITEM_KEYWORD.KEYWORD_TYPE).eq("CKW"));
				if (requiredServices.contains("keyword.pkw"))
					condition = condition.and(field(SI_ITEM_KEYWORD.KEYWORD_TYPE).eq("PKW"));
				return condition;
			}
		}
		return falseCondition();

	}

    public final static Condition getItemCategoryDeleteCondition(List<String> requiredServices, String itemId) {
        Condition condition = trueCondition();
        condition = condition.and(field(ITEM_ID).eq(itemId));
        if (requiredServices.contains("category"))
            return condition;
        return falseCondition();
    }

}
