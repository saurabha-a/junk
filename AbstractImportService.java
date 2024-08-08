package com.unilog.cx1.pim.commons.service.impl;

import static com.unilog.cx1.pim.commons.constants.ItemHeaderConstants.PART_NUMBER;
import static org.springframework.transaction.annotation.Isolation.READ_COMMITTED;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;

import com.unilog.cx1.pim.commons.model.PimDataObject;
import com.unilog.cx1.pim.commons.model.importtype.Import;
import com.unilog.cx1.pim.commons.service.IAttributeService;
import com.unilog.cx1.pim.commons.service.ICategoryService;
import com.unilog.cx1.pim.commons.service.IItemAssetService;
import com.unilog.cx1.pim.commons.service.IKeywordService;
import com.unilog.cx1.pim.commons.service.IPartnumberService;
import com.unilog.cx1.pim.commons.service.IPimSubsetService;
import com.unilog.cx1.pim.commons.service.IProductService;
import com.unilog.prime.commons.model.Tuple;

public abstract class AbstractImportService {

    protected final Logger logger;

    public AbstractImportService() {
        logger = LoggerFactory.getLogger(this.getClass());
    }

    @Autowired
    protected DSLContext dslContext;

    @Autowired
    protected IPimSubsetService subsetService;

    @Autowired
    protected ItemServiceImpl itemService;

    @Autowired
    protected IAttributeService attributeService;

    @Autowired
    protected ICategoryService categoryService;

    @Autowired
    protected IKeywordService keywordService;

    @Autowired
    protected IPartnumberService partnumberService;

    @Autowired
    protected IProductService productService;

    @Autowired
    protected IItemAssetService assetService;

    public void doImportInItemSubTable(Import anImport, PimDataObject qdo, Tuple<List<Tuple<Map<String, Object>, Map<String, Object>>>, Condition> tuple,
                                       Table table, Set<String> tableColumns, final List<String> stringTypeDeletableFields,
                                       final List<String> numericTypeDeletableFields, Set<String> notNullColumns) {
        filterValues(anImport, stringTypeDeletableFields, numericTypeDeletableFields, tuple.getFirstValue());
        persist(anImport, qdo, table, tableColumns, tuple.getFirstValue(), tuple.getSecondValue(), notNullColumns);
    }

    public void doImportInItemSubTable(Import anImport, PimDataObject qdo, Tuple<List<Tuple<Map<String, Object>, Map<String, Object>>>, Condition> tuple,
                                       Table table, Set<String> tableColumns, final List<String> stringTypeDeletableFields,
                                       final List<String> numericTypeDeletableFields) {
        filterValues(anImport, stringTypeDeletableFields, numericTypeDeletableFields, tuple.getFirstValue());
        persist(anImport, qdo, table, tableColumns, tuple.getFirstValue(), tuple.getSecondValue());
    }

    @Transactional(isolation = READ_COMMITTED)
    public void persist(Import anImport, PimDataObject qdo, Table table, Set<String> tableColumns, List<Tuple<Map<String, Object>,
            Map<String, Object>>> recs, Condition conditionToDeleteOldRecord, Set<String> notNullColumns) {
        anImport.persistToDb(recs, conditionToDeleteOldRecord, table, tableColumns, qdo, true, notNullColumns);
    }

    @Transactional(isolation = READ_COMMITTED)
    public void persist(Import anImport, PimDataObject qdo, Table table, Set<String> tableColumns, List<Tuple<Map<String, Object>,
            Map<String, Object>>> recs, Condition conditionToDeleteOldRecord) {
            anImport.persistToDb(recs, conditionToDeleteOldRecord, table, tableColumns, qdo, true);
    }

    public void filterValues(Import anImport, List<String> stringTypeDeletableFields, List<String> numericTypeDeletableFields, List<Tuple<Map<String, Object>, Map<String, Object>>> recs) {
        anImport.filterRecordValues(recs, stringTypeDeletableFields, numericTypeDeletableFields);
    }

}
