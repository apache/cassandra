/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.cql3.statements;

import java.util.Collections;
import java.util.List;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.CompactTables;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkContainsNoDuplicates;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.checkTrue;

/**
 * An <code>UPDATE</code> statement parsed from a CQL query statement.
 *
 */
public class UpdateStatement extends ModificationStatement
{
    private static final Constants.Value EMPTY = new Constants.Value(ByteBufferUtil.EMPTY_BYTE_BUFFER);

    private UpdateStatement(StatementType type,
                            int boundTerms,
                            CFMetaData cfm,
                            Operations operations,
                            StatementRestrictions restrictions,
                            Conditions conditions,
                            Attributes attrs)
    {
        super(type, boundTerms, cfm, operations, restrictions, conditions, attrs);
    }

    public boolean requireFullClusteringKey()
    {
        return true;
    }

    @Override
    public void addUpdateForKey(PartitionUpdate update, Clustering clustering, UpdateParameters params)
    {
        if (updatesRegularRows())
        {
            params.newRow(clustering);

            // We update the row timestamp (ex-row marker) only on INSERT (#6782)
            // Further, COMPACT tables semantic differs from "CQL3" ones in that a row exists only if it has
            // a non-null column, so we don't want to set the row timestamp for them.
            if (type.isInsert() && cfm.isCQLTable())
                params.addPrimaryKeyLivenessInfo();

            List<Operation> updates = getRegularOperations();

            // For compact table, when we translate it to thrift, we don't have a row marker. So we don't accept an insert/update
            // that only sets the PK unless the is no declared non-PK columns (in the latter we just set the value empty).

            // For a dense layout, when we translate it to thrift, we don't have a row marker. So we don't accept an insert/update
            // that only sets the PK unless the is no declared non-PK columns (which we recognize because in that case the compact
            // value is of type "EmptyType").
            if ((cfm.isCompactTable() && !cfm.isSuper()) && updates.isEmpty())
            {
                checkTrue(CompactTables.hasEmptyCompactValue(cfm),
                          "Column %s is mandatory for this COMPACT STORAGE table",
                          cfm.compactValueColumn().name);

                updates = Collections.<Operation>singletonList(new Constants.Setter(cfm.compactValueColumn(), EMPTY));
            }

            for (Operation op : updates)
                op.execute(update.partitionKey(), params);

            update.add(params.buildRow());
        }

        if (updatesStaticRow())
        {
            params.newRow(Clustering.STATIC_CLUSTERING);
            for (Operation op : getStaticOperations())
                op.execute(update.partitionKey(), params);
            update.add(params.buildRow());
        }
    }

    @Override
    public void addUpdateForKey(PartitionUpdate update, Slice slice, UpdateParameters params)
    {
        throw new UnsupportedOperationException();
    }

    public static class ParsedInsert extends ModificationStatement.Parsed
    {
        private final List<ColumnDefinition.Raw> columnNames;
        private final List<Term.Raw> columnValues;

        /**
         * A parsed <code>INSERT</code> statement.
         *
         * @param name column family being operated on
         * @param attrs additional attributes for statement (CL, timestamp, timeToLive)
         * @param columnNames list of column names
         * @param columnValues list of column values (corresponds to names)
         * @param ifNotExists true if an IF NOT EXISTS condition was specified, false otherwise
         */
        public ParsedInsert(CFName name,
                            Attributes.Raw attrs,
                            List<ColumnDefinition.Raw> columnNames,
                            List<Term.Raw> columnValues,
                            boolean ifNotExists)
        {
            super(name, StatementType.INSERT, attrs, null, ifNotExists, false);
            this.columnNames = columnNames;
            this.columnValues = columnValues;
        }

        @Override
        protected ModificationStatement prepareInternal(CFMetaData cfm,
                                                        VariableSpecifications boundNames,
                                                        Conditions conditions,
                                                        Attributes attrs)
        {

            // Created from an INSERT
            checkFalse(cfm.isCounter(), "INSERT statements are not allowed on counter tables, use UPDATE instead");

            checkFalse(columnNames == null, "Column names for INSERT must be provided when using VALUES");
            checkFalse(columnNames.isEmpty(), "No columns provided to INSERT");
            checkFalse(columnNames.size() != columnValues.size(), "Unmatched column names/values");
            checkContainsNoDuplicates(columnNames, "The column names contains duplicates");

            WhereClause.Builder whereClause = new WhereClause.Builder();
            Operations operations = new Operations(type);
            boolean hasClusteringColumnsSet = false;

            if (cfm.isSuper() && cfm.isDense())
            {
                // SuperColumn familiy updates are always row-level
                hasClusteringColumnsSet = true;
                SuperColumnCompatibility.prepareInsertOperations(cfm, columnNames, whereClause, columnValues, boundNames, operations);
            }
            else
            {
                for (int i = 0; i < columnNames.size(); i++)
                {
                    ColumnDefinition def = getColumnDefinition(cfm, columnNames.get(i));

                    if (def.isClusteringColumn())
                        hasClusteringColumnsSet = true;

                    Term.Raw value = columnValues.get(i);

                    if (def.isPrimaryKeyColumn())
                    {
                        whereClause.add(new SingleColumnRelation(columnNames.get(i), Operator.EQ, value));
                    }
                    else
                    {
                        Operation operation = new Operation.SetValue(value).prepare(cfm, def);
                        operation.collectMarkerSpecification(boundNames);
                        operations.add(operation);
                    }
                }
            }

            boolean applyOnlyToStaticColumns = !hasClusteringColumnsSet && appliesOnlyToStaticColumns(operations, conditions);

            StatementRestrictions restrictions = new StatementRestrictions(type,
                                                                           cfm,
                                                                           whereClause.build(),
                                                                           boundNames,
                                                                           applyOnlyToStaticColumns,
                                                                           false,
                                                                           false,
                                                                           false);

            return new UpdateStatement(type,
                                       boundNames.size(),
                                       cfm,
                                       operations,
                                       restrictions,
                                       conditions,
                                       attrs);
        }
    }

    /**
     * A parsed INSERT JSON statement.
     */
    public static class ParsedInsertJson extends ModificationStatement.Parsed
    {
        private final Json.Raw jsonValue;
        private final boolean defaultUnset;

        public ParsedInsertJson(CFName name, Attributes.Raw attrs, Json.Raw jsonValue, boolean defaultUnset, boolean ifNotExists)
        {
            super(name, StatementType.INSERT, attrs, null, ifNotExists, false);
            this.jsonValue = jsonValue;
            this.defaultUnset = defaultUnset;
        }

        @Override
        protected ModificationStatement prepareInternal(CFMetaData cfm,
                                                        VariableSpecifications boundNames,
                                                        Conditions conditions,
                                                        Attributes attrs)
        {
            checkFalse(cfm.isCounter(), "INSERT statements are not allowed on counter tables, use UPDATE instead");

            List<ColumnDefinition> defs = newArrayList(cfm.allColumnsInSelectOrder());
            Json.Prepared prepared = jsonValue.prepareAndCollectMarkers(cfm, defs, boundNames);

            WhereClause.Builder whereClause = new WhereClause.Builder();
            Operations operations = new Operations(type);
            boolean hasClusteringColumnsSet = false;

            if (cfm.isSuper() && cfm.isDense())
            {
                hasClusteringColumnsSet = true;
                SuperColumnCompatibility.prepareInsertJSONOperations(cfm, defs, boundNames, prepared, whereClause, operations);
            }
            else
            {



// TODO: indent

            for (ColumnDefinition def : defs)
            {
                if (def.isClusteringColumn())
                    hasClusteringColumnsSet = true;

                Term.Raw raw = prepared.getRawTermForColumn(def, defaultUnset);
                if (def.isPrimaryKeyColumn())
                {
                    whereClause.add(new SingleColumnRelation(ColumnDefinition.Raw.forColumn(def), Operator.EQ, raw));
                }
                else
                {
                    Operation operation = new Operation.SetValue(raw).prepare(cfm, def);
                    operation.collectMarkerSpecification(boundNames);
                    operations.add(operation);
                }
            }




            }

            boolean applyOnlyToStaticColumns = !hasClusteringColumnsSet && appliesOnlyToStaticColumns(operations, conditions);

            StatementRestrictions restrictions = new StatementRestrictions(type,
                                                                           cfm,
                                                                           whereClause.build(),
                                                                           boundNames,
                                                                           applyOnlyToStaticColumns,
                                                                           false,
                                                                           false,
                                                                           false);

            return new UpdateStatement(type,
                                       boundNames.size(),
                                       cfm,
                                       operations,
                                       restrictions,
                                       conditions,
                                       attrs);
        }
    }

    public static class ParsedUpdate extends ModificationStatement.Parsed
    {
        // Provided for an UPDATE
        private final List<Pair<ColumnDefinition.Raw, Operation.RawUpdate>> updates;
        private WhereClause whereClause;

        /**
         * Creates a new UpdateStatement from a column family name, columns map, consistency
         * level, and key term.
         *
         * @param name column family being operated on
         * @param attrs additional attributes for statement (timestamp, timeToLive)
         * @param updates a map of column operations to perform
         * @param whereClause the where clause
         * @param ifExists flag to check if row exists
         * */
        public ParsedUpdate(CFName name,
                            Attributes.Raw attrs,
                            List<Pair<ColumnDefinition.Raw, Operation.RawUpdate>> updates,
                            WhereClause whereClause,
                            List<Pair<ColumnDefinition.Raw, ColumnCondition.Raw>> conditions,
                            boolean ifExists)
        {
            super(name, StatementType.UPDATE, attrs, conditions, false, ifExists);
            this.updates = updates;
            this.whereClause = whereClause;
        }

        @Override
        protected ModificationStatement prepareInternal(CFMetaData cfm,
                                                        VariableSpecifications boundNames,
                                                        Conditions conditions,
                                                        Attributes attrs)
        {
            Operations operations = new Operations(type);

            if (cfm.isSuper() && cfm.isDense())
            {
                conditions = SuperColumnCompatibility.rebuildLWTColumnConditions(conditions, cfm, whereClause);
                whereClause = SuperColumnCompatibility.prepareUpdateOperations(cfm, whereClause, updates, boundNames, operations);
            }
            else
            {
                for (Pair<ColumnDefinition.Raw, Operation.RawUpdate> entry : updates)
                {
                    ColumnDefinition def = getColumnDefinition(cfm, entry.left);

                    checkFalse(def.isPrimaryKeyColumn(), "PRIMARY KEY part %s found in SET part", def.name);

                    Operation operation = entry.right.prepare(cfm, def);
                    operation.collectMarkerSpecification(boundNames);
                    operations.add(operation);
                }
            }
            
            StatementRestrictions restrictions = newRestrictions(cfm,
                                                                 boundNames,
                                                                 operations,
                                                                 whereClause,
                                                                 conditions);

            return new UpdateStatement(type,
                                       boundNames.size(),
                                       cfm,
                                       operations,
                                       restrictions,
                                       conditions,
                                       attrs);
        }
    }
}
