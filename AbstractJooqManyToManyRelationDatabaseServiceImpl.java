package com.unilog.prime.dbcore.service.impl;

import java.util.List;
import java.util.Set;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.UpdatableRecord;
import org.springframework.beans.factory.annotation.Autowired;

import com.unilog.prime.commons.service.impl.AbstractCoreServiceImpl;
import com.unilog.prime.dbcore.service.IBaseManyToManyDatabaseService;

public abstract class AbstractJooqManyToManyRelationDatabaseServiceImpl<R extends UpdatableRecord<R>>
		extends AbstractCoreServiceImpl 
		implements IBaseManyToManyDatabaseService<R> {
	
	@Autowired
	protected DSLContext dslContext;
	
	private final Table<R> table;

	public AbstractJooqManyToManyRelationDatabaseServiceImpl( Table<R> table) {
		this.table = table;
	}

	@Override
	public <T1, T2> List<T2> getRelated(Field<T1> parentIdField, Field<T2> childIdField, T1 id) {
		return dslContext.select(childIdField).from(table).where(parentIdField.eq(id)).fetch(childIdField);
	}

	@Override
	public <T1, T2> R create(Field<T1> field1, Field<T2> field2, T1 id1, T2 id2) {
		R record = dslContext.newRecord(table);
		record.setValue(field1, id1);
		record.setValue(field2, id2);
		record.store();
		return record;
	}

	@Override
	public <T1, T2> void delete(Field<T1> field1, Field<T2> field2, T1 id1, T2 id2) {
		dslContext.deleteFrom(table).where(field1.eq(id1).and(field2.eq(id2))).execute();
	}

	@Override
	public <T> void deleteAll(Field<T> field, T id) {
		dslContext.deleteFrom(table).where(field.eq(id)).execute();
	}

	@Override
	public <T1, T2> void merge(Field<T1> parentField, Field<T2> childField, T1 parentId, Set<T2> newChildIds) {
		if (newChildIds != null) {
			List<T2> oldChildIds = getRelated(parentField, childField, parentId);
			oldChildIds.stream().filter(childId -> !newChildIds.contains(childId))
					.forEach(childId -> delete(parentField, childField, parentId, childId));
			newChildIds.stream().filter(childId -> !oldChildIds.contains(childId))
					.forEach(childId -> create(parentField, childField, parentId, childId));
		}
	}
}
