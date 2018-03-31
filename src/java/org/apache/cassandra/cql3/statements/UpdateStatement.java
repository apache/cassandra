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

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cql3.*;
import org.apache.cassandra.cql3.conditions.ColumnCondition;
import org.apache.cassandra.cql3.conditions.Conditions;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.CompactTables;
import org.apache.cassandra.db.Slice;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.lang.builder.ToStringStyle;

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
                            TableMetadata metadata,
                            Operations operations,
                            StatementRestrictions restrictions,
                            Conditions conditions,
                            Attributes attrs)
    {
        super(type, boundTerms, metadata, operations, restrictions, conditions, attrs);
    }

    public boolean requireFullClusteringKey()
    {
        return true;
    }

    @Override
    public void addUpdateForKey(PartitionUpdate.Builder updateBuilder, Clustering clustering, UpdateParameters params)
    {
        if (updatesRegularRows())
        {
            params.newRow(clustering);

            // We update the row timestamp (ex-row marker) only on INSERT (#6782)
            // Further, COMPACT tables semantic differs from "CQL3" ones in that a row exists only if it has
            // a non-null column, so we don't want to set the row timestamp for them.
            if (type.isInsert() && metadata().isCQLTable())
                params.addPrimaryKeyLivenessInfo();

            List<Operation> updates = getRegularOperations();

            // For compact table, we don't accept an insert/update that only sets the PK unless the is no
            // declared non-PK columns (which we recognize because in that case
            // the compact value is of type "EmptyType").
            if (metadata().isCompactTable() && updates.isEmpty())
            {
                checkTrue(CompactTables.hasEmptyCompactValue(metadata),
                          "Column %s is mandatory for this COMPACT STORAGE table",
                          metadata().compactValueColumn.name);

                updates = Collections.<Operation>singletonList(new Constants.Setter(metadata().compactValueColumn, EMPTY));
            }

            for (Operation op : updates)
                op.execute(updateBuilder.partitionKey(), params);

            updateBuilder.add(params.buildRow());
        }

        if (updatesStaticRow())
        {
            params.newRow(Clustering.STATIC_CLUSTERING);
            for (Operation op : getStaticOperations())
                op.execute(updateBuilder.partitionKey(), params);
            updateBuilder.add(params.buildRow());
        }
    }

    @Override
    public void addUpdateForKey(PartitionUpdate.Builder update, Slice slice, UpdateParameters params)
    {
        throw new UnsupportedOperationException();
    }

    public static class ParsedInsert extends ModificationStatement.Parsed
    {
        private final List<ColumnMetadata.Raw> columnNames;
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
                            List<ColumnMetadata.Raw> columnNames,
                            List<Term.Raw> columnValues,
                            boolean ifNotExists)
        {
            super(name, StatementType.INSERT, attrs, null, ifNotExists, false);
            this.columnNames = columnNames;
            this.columnValues = columnValues;
        }

        @Override
        protected ModificationStatement prepareInternal(TableMetadata metadata,
                                                        VariableSpecifications boundNames,
                                                        Conditions conditions,
                                                        Attributes attrs)
        {

            // Created from an INSERT
            checkFalse(metadata.isCounter(), "INSERT statements are not allowed on counter tables, use UPDATE instead");

            checkFalse(columnNames == null, "Column names for INSERT must be provided when using VALUES");
            checkFalse(columnNames.isEmpty(), "No columns provided to INSERT");
            checkFalse(columnNames.size() != columnValues.size(), "Unmatched column names/values");
            checkContainsNoDuplicates(columnNames, "The column names contains duplicates");

            WhereClause.Builder whereClause = new WhereClause.Builder();
            Operations operations = new Operations(type);
            boolean hasClusteringColumnsSet = false;

            for (int i = 0; i < columnNames.size(); i++)
            {
                ColumnMetadata def = getColumnDefinition(metadata, columnNames.get(i));

                if (def.isClusteringColumn())
                    hasClusteringColumnsSet = true;

                Term.Raw value = columnValues.get(i);

                if (def.isPrimaryKeyColumn())
                {
                    whereClause.add(new SingleColumnRelation(columnNames.get(i), Operator.EQ, value));
                }
                else
                {
                    Operation operation = new Operation.SetValue(value).prepare(metadata, def);
                    operation.collectMarkerSpecification(boundNames);
                    operations.add(operation);
                }
            }

            boolean applyOnlyToStaticColumns = !hasClusteringColumnsSet && appliesOnlyToStaticColumns(operations, conditions);

            StatementRestrictions restrictions = new StatementRestrictions(type,
                                                                           metadata,
                                                                           whereClause.build(),
                                                                           boundNames,
                                                                           applyOnlyToStaticColumns,
                                                                           false,
                                                                           false);

            return new UpdateStatement(type,
                                       boundNames.size(),
                                       metadata,
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
        protected ModificationStatement prepareInternal(TableMetadata metadata,
                                                        VariableSpecifications boundNames,
                                                        Conditions conditions,
                                                        Attributes attrs)
        {
            checkFalse(metadata.isCounter(), "INSERT statements are not allowed on counter tables, use UPDATE instead");

            Collection<ColumnMetadata> defs = metadata.columns();
            Json.Prepared prepared = jsonValue.prepareAndCollectMarkers(metadata, defs, boundNames);

            WhereClause.Builder whereClause = new WhereClause.Builder();
            Operations operations = new Operations(type);
            boolean hasClusteringColumnsSet = false;

            for (ColumnMetadata def : defs)
            {
                if (def.isClusteringColumn())
                    hasClusteringColumnsSet = true;

                Term.Raw raw = prepared.getRawTermForColumn(def, defaultUnset);
                if (def.isPrimaryKeyColumn())
                {
                    whereClause.add(new SingleColumnRelation(ColumnMetadata.Raw.forColumn(def), Operator.EQ, raw));
                }
                else
                {
                    Operation operation = new Operation.SetValue(raw).prepare(metadata, def);
                    operation.collectMarkerSpecification(boundNames);
                    operations.add(operation);
                }
            }

            boolean applyOnlyToStaticColumns = !hasClusteringColumnsSet && appliesOnlyToStaticColumns(operations, conditions);

            StatementRestrictions restrictions = new StatementRestrictions(type,
                                                                           metadata,
                                                                           whereClause.build(),
                                                                           boundNames,
                                                                           applyOnlyToStaticColumns,
                                                                           false,
                                                                           false);

            return new UpdateStatement(type,
                                       boundNames.size(),
                                       metadata,
                                       operations,
                                       restrictions,
                                       conditions,
                                       attrs);
        }
    }

    public static class ParsedUpdate extends ModificationStatement.Parsed
    {
        // Provided for an UPDATE
        private final List<Pair<ColumnMetadata.Raw, Operation.RawUpdate>> updates;
        private final WhereClause whereClause;

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
                            List<Pair<ColumnMetadata.Raw, Operation.RawUpdate>> updates,
                            WhereClause whereClause,
                            List<Pair<ColumnMetadata.Raw, ColumnCondition.Raw>> conditions,
                            boolean ifExists)
        {
            super(name, StatementType.UPDATE, attrs, conditions, false, ifExists);
            this.updates = updates;
            this.whereClause = whereClause;
        }

        @Override
        protected ModificationStatement prepareInternal(TableMetadata metadata,
                                                        VariableSpecifications boundNames,
                                                        Conditions conditions,
                                                        Attributes attrs)
        {
            Operations operations = new Operations(type);

            for (Pair<ColumnMetadata.Raw, Operation.RawUpdate> entry : updates)
            {
                ColumnMetadata def = getColumnDefinition(metadata, entry.left);

                checkFalse(def.isPrimaryKeyColumn(), "PRIMARY KEY part %s found in SET part", def.name);

                Operation operation = entry.right.prepare(metadata, def);
                operation.collectMarkerSpecification(boundNames);
                operations.add(operation);
            }

            StatementRestrictions restrictions = newRestrictions(metadata,
                                                                 boundNames,
                                                                 operations,
                                                                 whereClause,
                                                                 conditions);

            return new UpdateStatement(type,
                                       boundNames.size(),
                                       metadata,
                                       operations,
                                       restrictions,
                                       conditions,
                                       attrs);
        }
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
}
