package com.unilog.prime.dbcore.service;

import java.util.List;
import java.util.Set;

import org.jooq.Field;
import org.jooq.UpdatableRecord;


public interface IBaseManyToManyDatabaseService <R extends UpdatableRecord<R>> {

	<T1, T2> List<T2> getRelated(Field<T1> parentIdField, Field<T2> childIdField, T1 id);

	<T1, T2> R create(Field<T1> field1, Field<T2> field2, T1 id1, T2 id2);

	<T1, T2> void delete(Field<T1> field1, Field<T2> field2, T1 id1, T2 id2);

	<T> void deleteAll(Field<T> field, T id);

	<T1, T2> void merge(Field<T1> parentField, Field<T2> childField, T1 parentId, Set<T2> newChildIds);
}
