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
package org.apache.cassandra.cql3.statements.schema;

import java.util.*;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.statements.schema.IndexTarget.Type;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sasi.SASIIndex;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.schema.Keyspaces.KeyspacesDiff;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Event.SchemaChange;
import org.apache.cassandra.transport.Event.SchemaChange.Change;
import org.apache.cassandra.transport.Event.SchemaChange.Target;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Iterables.tryFind;

public final class CreateIndexStatement extends AlterSchemaStatement
{
    private final String indexName;
    private final String tableName;
    private final List<IndexTarget.Raw> rawIndexTargets;
    private final IndexAttributes attrs;
    private final boolean ifNotExists;

    private static final String DSE_INDEX_WARNING = "Index %s was not created. DSE custom index (%s) is not " +
                                                    "supported. Consult the docs on alternatives (SAI indexes, " +
                                                    "Secondary Indexes).";

    @VisibleForTesting
    public static final Set<String> DSE_INDEXES = ImmutableSet.of(
        "com.datastax.bdp.cassandra.index.solr.SolrSecondaryIndex",
        "com.datastax.bdp.cassandra.index.solr.ThriftSolrSecondaryIndex",
        "com.datastax.bdp.cassandra.index.solr.Cql3SolrSecondaryIndex",
        "com.datastax.bdp.search.solr.ThriftSolrSecondaryIndex",
        "com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex"
    );

    public CreateIndexStatement(String keyspaceName,
                                String tableName,
                                String indexName,
                                List<IndexTarget.Raw> rawIndexTargets,
                                IndexAttributes attrs,
                                boolean ifNotExists)
    {
        super(keyspaceName);
        this.tableName = tableName;
        this.indexName = indexName;
        this.rawIndexTargets = rawIndexTargets;
        this.attrs = attrs;
        this.ifNotExists = ifNotExists;
    }

    public Keyspaces apply(Keyspaces schema)
    {
        if (isDseIndexCreateStatement())
        {
            // DSE indexes are not supported. The index is not created, the attempt is ignored (doesn't cause error),
            // a meaningfull warning is returned instead.
            return schema;
        }

        attrs.validate();

        if (attrs.isCustom && attrs.customClass.equals(SASIIndex.class.getName()) && !DatabaseDescriptor.getEnableSASIIndexes())
            throw new InvalidRequestException("SASI indexes are disabled. Enable in cassandra.yaml to use.");

        KeyspaceMetadata keyspace = schema.getNullable(keyspaceName);
        if (null == keyspace)
            throw ire("Keyspace '%s' doesn't exist", keyspaceName);

        TableMetadata table = keyspace.getTableOrViewNullable(tableName);
        if (null == table)
            throw ire("Table '%s' doesn't exist", tableName);

        if (null != indexName && keyspace.hasIndex(indexName))
        {
            if (ifNotExists)
                return schema;

            throw ire("Index '%s' already exists", indexName);
        }

        if (table.isCounter())
            throw ire("Secondary indexes on counter tables aren't supported");

        if (table.isView())
            throw ire("Secondary indexes on materialized views aren't supported");

        if (Keyspace.open(table.keyspace).getReplicationStrategy().hasTransientReplicas())
            throw new InvalidRequestException("Secondary indexes are not supported on transiently replicated keyspaces");

        List<IndexTarget> indexTargets = Lists.newArrayList(transform(rawIndexTargets, t -> t.prepare(table)));

        if (indexTargets.isEmpty() && !attrs.isCustom)
            throw ire("Only CUSTOM indexes can be created without specifying a target column");

        if (indexTargets.size() > 1)
        {
            if (!attrs.isCustom)
                throw ire("Only CUSTOM indexes support multiple columns");

            Set<ColumnIdentifier> columns = new HashSet<>();
            for (IndexTarget target : indexTargets)
                if (!columns.add(target.column))
                    throw ire("Duplicate column '%s' in index target list", target.column);
        }

        IndexMetadata.Kind kind = attrs.isCustom ? IndexMetadata.Kind.CUSTOM : IndexMetadata.Kind.COMPOSITES;

        indexTargets.forEach(t -> validateIndexTarget(table, kind, t));

        String name = null == indexName ? generateIndexName(keyspace, indexTargets) : indexName;

        Map<String, String> options = attrs.isCustom ? attrs.getOptions() : Collections.emptyMap();

        IndexMetadata index = IndexMetadata.fromIndexTargets(indexTargets, name, kind, options);

        // check to disallow creation of an index which duplicates an existing one in all but name
        IndexMetadata equalIndex = tryFind(table.indexes, i -> i.equalsWithoutName(index)).orNull();
        if (null != equalIndex)
        {
            if (ifNotExists)
                return schema;

            throw ire("Index %s is a duplicate of existing index %s", index.name, equalIndex.name);
        }

        TableMetadata newTable = table.withSwapped(table.indexes.with(index));
        newTable.validate();

        return schema.withAddedOrUpdated(keyspace.withSwapped(keyspace.tables.withSwapped(newTable)));
    }

    @Override
    Set<String> clientWarnings(KeyspacesDiff diff)
    {
        if (attrs.isCustom && attrs.customClass.equals(SASIIndex.class.getName()))
            return ImmutableSet.of(SASIIndex.USAGE_WARNING);

        if (isDseIndexCreateStatement())
            return ImmutableSet.of(String.format(DSE_INDEX_WARNING, indexName, attrs.customClass));

        return ImmutableSet.of();
    }

    private boolean isDseIndexCreateStatement()
    {
        return DSE_INDEXES.contains(attrs.customClass);
    }

    private void validateIndexTarget(TableMetadata table, IndexMetadata.Kind kind, IndexTarget target)
    {
        ColumnMetadata column = table.getColumn(target.column);

        if (null == column)
            throw ire("Column '%s' doesn't exist", target.column);

        if ((kind == IndexMetadata.Kind.CUSTOM) && !SchemaConstants.isValidName(target.column.toString()))
            throw ire("Column '%s' is longer than the permissible name length of %d characters or" +
                      " contains non-alphanumeric-underscore characters", target.column, SchemaConstants.NAME_LENGTH);

        if (column.type.referencesDuration())
        {
            if (column.type.isCollection())
                throw ire("Secondary indexes are not supported on collections containing durations");

            if (column.type.isTuple())
                throw ire("Secondary indexes are not supported on tuples containing durations");

            if (column.type.isUDT())
                throw  ire("Secondary indexes are not supported on UDTs containing durations");

            throw ire("Secondary indexes are not supported on duration columns");
        }

        if (table.isCompactTable())
        {
            TableMetadata.CompactTableMetadata compactTable = (TableMetadata.CompactTableMetadata) table;
            if (column.isPrimaryKeyColumn())
                throw new InvalidRequestException("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables");
            if (compactTable.compactValueColumn.equals(column))
                throw new InvalidRequestException("Secondary indexes are not supported on compact value column of COMPACT STORAGE tables");
        }

        if (column.isPartitionKey() && table.partitionKeyColumns().size() == 1)
            throw ire("Cannot create secondary index on the only partition key column %s", column);

        if (column.type.isFrozenCollection() && target.type != Type.FULL)
            throw ire("Cannot create %s() index on frozen column %s. Frozen collections are immutable and must be fully " +
                      "indexed by using the 'full(%s)' modifier", target.type, column, column);

        if (!column.type.isFrozenCollection() && target.type == Type.FULL)
            throw ire("full() indexes can only be created on frozen collections");

        if (!column.type.isCollection() && target.type != Type.SIMPLE)
            throw ire("Cannot create %s() index on %s. Non-collection columns only support simple indexes", target.type, column);

        if (!(column.type instanceof MapType && column.type.isMultiCell()) && (target.type == Type.KEYS || target.type == Type.KEYS_AND_VALUES))
            throw ire("Cannot create index on %s of column %s with non-map type", target.type, column);

        if (column.type.isUDT() && column.type.isMultiCell())
            throw ire("Cannot create index on non-frozen UDT column %s", column);
    }

    private String generateIndexName(KeyspaceMetadata keyspace, List<IndexTarget> targets)
    {
        String baseName = targets.size() == 1
                        ? IndexMetadata.generateDefaultIndexName(tableName, targets.get(0).column)
                        : IndexMetadata.generateDefaultIndexName(tableName);
        return keyspace.findAvailableIndexName(baseName);
    }

    SchemaChange schemaChangeEvent(KeyspacesDiff diff)
    {
        return new SchemaChange(Change.UPDATED, Target.TABLE, keyspaceName, tableName);
    }

    public void authorize(ClientState client)
    {
        client.ensureTablePermission(keyspaceName, tableName, Permission.ALTER);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_INDEX, keyspaceName, indexName);
    }

    public String toString()
    {
        return String.format("%s (%s, %s)", getClass().getSimpleName(), keyspaceName, indexName);
    }

    public static final class Raw extends CQLStatement.Raw
    {
        private final QualifiedName tableName;
        private final QualifiedName indexName;
        private final List<IndexTarget.Raw> rawIndexTargets;
        private final IndexAttributes attrs;
        private final boolean ifNotExists;

        public Raw(QualifiedName tableName,
                   QualifiedName indexName,
                   List<IndexTarget.Raw> rawIndexTargets,
                   IndexAttributes attrs,
                   boolean ifNotExists)
        {
            this.tableName = tableName;
            this.indexName = indexName;
            this.rawIndexTargets = rawIndexTargets;
            this.attrs = attrs;
            this.ifNotExists = ifNotExists;
        }

        public CreateIndexStatement prepare(ClientState state)
        {
            String keyspaceName = tableName.hasKeyspace()
                                ? tableName.getKeyspace()
                                : indexName.hasKeyspace() ? indexName.getKeyspace() : state.getKeyspace();

            if (tableName.hasKeyspace() && !keyspaceName.equals(tableName.getKeyspace()))
                throw ire("Keyspace name '%s' doesn't match table name '%s'", keyspaceName, tableName);

            if (indexName.hasKeyspace() && !keyspaceName.equals(indexName.getKeyspace()))
                throw ire("Keyspace name '%s' doesn't match index name '%s'", keyspaceName, tableName);

            return new CreateIndexStatement(keyspaceName, tableName.getName(), indexName.getName(), rawIndexTargets, attrs, ifNotExists);
        }
    }
}
