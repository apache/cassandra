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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.marshal.VectorType;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.MemtableParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.schema.UserFunctions;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Event.SchemaChange;

/**
 * {@code CREATE TABLE [IF NOT EXISTS] <newtable> LIKE <oldtable> WITH <property> = <value>}
 */
public final class CopyTableStatement extends AlterSchemaStatement
{
    private final String sourceKeyspace;
    private final String sourceTableName;
    private final String targetKeyspace;
    private final String targetTableName;
    private final boolean ifNotExists;
    private final TableAttributes attrs;
    private final CreateLikeOption createLikeOption;

    public CopyTableStatement(String sourceKeyspace,
                              String targetKeyspace,
                              String sourceTableName,
                              String targetTableName,
                              boolean ifNotExists,
                              CreateLikeOption createLikeOption,
                              TableAttributes attrs)
    {
        super(targetKeyspace);
        this.sourceKeyspace = sourceKeyspace;
        this.targetKeyspace = targetKeyspace;
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.ifNotExists = ifNotExists;
        this.createLikeOption = createLikeOption;
        this.attrs = attrs;
    }

    @Override
    SchemaChange schemaChangeEvent(Keyspaces.KeyspacesDiff diff)
    {
        return new SchemaChange(SchemaChange.Change.CREATED, SchemaChange.Target.TABLE, targetKeyspace, targetTableName);
    }

    @Override
    public void authorize(ClientState client)
    {
        client.ensureTablePermission(sourceKeyspace, sourceTableName, Permission.SELECT);
        client.ensureAllTablesPermission(targetKeyspace, Permission.CREATE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_TABLE_LIKE, targetKeyspace, targetTableName);
    }

    @Override
    public Keyspaces apply(ClusterMetadata metadata)
    {
        Keyspaces schema = metadata.schema.getKeyspaces();
        KeyspaceMetadata sourceKeyspaceMeta = schema.getNullable(sourceKeyspace);

        if (null == sourceKeyspaceMeta)
            throw ire("Source Keyspace '%s' doesn't exist", sourceKeyspace);

        TableMetadata sourceTableMeta = sourceKeyspaceMeta.getTableNullable(sourceTableName);

        if (null == sourceTableMeta)
            throw ire("Souce Table '%s.%s' doesn't exist", sourceKeyspace, sourceTableName);

        if (sourceTableMeta.isIndex())
            throw ire("Cannot use CREATE TABLE LIKE on an index table '%s.%s'.", sourceKeyspace, sourceTableName);

        if (sourceTableMeta.isView())
            throw ire("Cannot use CREATE TABLE LIKE on a materialized view '%s.%s'.", sourceKeyspace, sourceTableName);

        KeyspaceMetadata targetKeyspaceMeta = schema.getNullable(targetKeyspace);
        if (null == targetKeyspaceMeta)
            throw ire("Target Keyspace '%s' doesn't exist", targetKeyspace);

        if (targetKeyspaceMeta.hasTable(targetTableName))
        {
            if (ifNotExists)
                return schema;

            throw new AlreadyExistsException(targetKeyspace, targetTableName);
        }

        if (!sourceKeyspace.equalsIgnoreCase(targetKeyspace))
        {
            Set<String> missingUserTypes = Sets.newHashSet();
            // for different keyspace, if source table used some udts and the target table also need them
            for (ByteBuffer sourceUserTypeName : sourceTableMeta.getReferencedUserTypes())
            {
                Optional<UserType> targetUserType = targetKeyspaceMeta.types.get(sourceUserTypeName);
                Optional<UserType> sourceUserType = sourceKeyspaceMeta.types.get(sourceUserTypeName);
                if (targetUserType.isPresent() && sourceUserType.isPresent())
                {
                    if (!sourceUserType.get().equalsWithOutKs(targetUserType.get()))
                        throw ire("Target keyspace '%s' has same UDT name '%s' as source keyspace '%s' but with different structure.",
                                  targetKeyspace,
                                  UTF8Type.instance.getString(targetUserType.get().name),
                                  sourceKeyspace);
                }
                else
                {
                    missingUserTypes.add(UTF8Type.instance.compose(sourceUserTypeName));
                }
            }

            if (!missingUserTypes.isEmpty())
                throw ire("UDTs %s do not exist in target keyspace '%s'.",
                          missingUserTypes.stream().sorted().collect(Collectors.joining(", ")),
                          targetKeyspace);
        }

        // Guardrail on columns per table
        Guardrails.columnsPerTable.guard(sourceTableMeta.columns().size(), targetTableName, false, state);

        sourceTableMeta.columns().forEach(columnMetadata -> {
            if (columnMetadata.type.isVector())
            {
                Guardrails.vectorTypeEnabled.ensureEnabled(columnMetadata.name.toString(), state);
                int dimensions = ((VectorType) columnMetadata.type).dimension;
                Guardrails.vectorDimensions.guard(dimensions, columnMetadata.name.toString(), false, state);
            }
        });

        // Guardrail to check whether creation of new COMPACT STORAGE tables is allowed
        if (sourceTableMeta.isCompactTable())
            Guardrails.compactTablesEnabled.ensureEnabled(state);

        if (sourceKeyspaceMeta.replicationStrategy.hasTransientReplicas()
            && sourceTableMeta.params.readRepair != ReadRepairStrategy.NONE)
        {
            throw ire("read_repair must be set to 'NONE' for transiently replicated keyspaces");
        }

        if (!sourceTableMeta.params.compression.isEnabled())
            Guardrails.uncompressedTablesEnabled.ensureEnabled(state);

        // withInternals can be set to false as it is only used for souce table id, which is not need for target table and the table
        // id can be set through create table like cql using WITH ID
        String sourceCQLString = sourceTableMeta.toCqlString(false, false, false, false);

        TableMetadata.Builder targetBuilder = CreateTableStatement.parse(sourceCQLString,
                                                                         targetKeyspace,
                                                                         targetTableName,
                                                                         sourceKeyspaceMeta.types,
                                                                         UserFunctions.none())
                                                                  .indexes(Indexes.none())
                                                                  .triggers(Triggers.none());

        TableParams originalParams = targetBuilder.build().params;
        TableParams newTableParams = attrs.asAlteredTableParams(originalParams);
        maybeCopyIndexes(targetBuilder, sourceTableMeta, targetKeyspaceMeta);

        TableMetadata table = targetBuilder.params(newTableParams)
                                           .id(TableId.get(metadata))
                                           .build();
        table.validate();

        return schema.withAddedOrUpdated(targetKeyspaceMeta.withSwapped(targetKeyspaceMeta.tables.with(table)));
    }

    @Override
    public void validate(ClientState state)
    {
        super.validate(state);

        // If a memtable configuration is specified, validate it against config
        if (attrs.hasOption(TableParams.Option.MEMTABLE))
            MemtableParams.get(attrs.getString(TableParams.Option.MEMTABLE.toString()));

        // Guardrail on table properties
        Guardrails.tableProperties.guard(attrs.updatedProperties(), attrs::removeProperty, state);

        // Guardrail on number of tables
        if (Guardrails.tables.enabled(state))
        {
            int totalUserTables = Schema.instance.getUserKeyspaces()
                                                 .stream()
                                                 .mapToInt(ksm -> ksm.tables.size())
                                                 .sum();
            Guardrails.tables.guard(totalUserTables + 1, targetTableName, false, state);
        }
        validateDefaultTimeToLive(attrs.asNewTableParams());
    }

    private void maybeCopyIndexes(TableMetadata.Builder builder, TableMetadata sourceTableMeta, KeyspaceMetadata targetKeyspaceMeta)
    {
        if (createLikeOption != CreateLikeOption.INDEXES || sourceTableMeta.indexes.isEmpty())
            return;

        Set<String> customIndexes = Sets.newTreeSet();
        List<IndexMetadata> indexesToCopy = new ArrayList<>();
        for (IndexMetadata indexMetadata : sourceTableMeta.indexes)
        {
            // only sai and legacy secondary index is supported
            if (indexMetadata.isCustom() && !StorageAttachedIndex.class.getCanonicalName().equals(indexMetadata.getIndexClassName()))
            {
                customIndexes.add(indexMetadata.name);
                continue;
            }

            ColumnMetadata targetColumn = sourceTableMeta.getColumn(UTF8Type.instance.decompose(indexMetadata.options.get("target")));
            String indexName;
            // The rules for generating the index names of the target table are:
            // (1) If the source table's index names follow the pattern sourcetablename_columnname_idx_number, the index names are considered to be generated by the system,
            //     then we directly replace the name of source table with the name of target table, and increment the number after idx to avoid index name conflicts.
            // (2) Index names that do not follow the above pattern are considered user-defined, so the index names are retained and increment the number after idx to avoid conflicts.
            if (indexMetadata.name.startsWith(sourceTableName + "_" + targetColumn.name + "_idx"))
            {
                String baseName = IndexMetadata.generateDefaultIndexName(targetTableName, targetColumn.name);
                indexName = targetKeyspaceMeta.findAvailableIndexName(baseName, indexesToCopy, targetKeyspaceMeta);
            }
            else
            {
                indexName = targetKeyspaceMeta.findAvailableIndexName(indexMetadata.name, indexesToCopy, targetKeyspaceMeta);
            }
            indexesToCopy.add(IndexMetadata.fromSchemaMetadata(indexName, indexMetadata.kind, indexMetadata.options));
        }

        if (!indexesToCopy.isEmpty())
            builder.indexes(Indexes.builder().add(indexesToCopy).build());

        if (!customIndexes.isEmpty())
            ClientWarn.instance.warn(String.format("Source table %s.%s to copy indexes from to %s.%s has custom indexes. These indexes were not copied: %s",
                                                   sourceKeyspace,
                                                   sourceTableName,
                                                   targetKeyspace,
                                                   targetTableName,
                                                   customIndexes));
    }

    public final static class Raw extends CQLStatement.Raw
    {
        private final QualifiedName oldName;
        private final QualifiedName newName;
        private final boolean ifNotExists;
        public final TableAttributes attrs = new TableAttributes();
        private CreateLikeOption createLikeOption = null;

        public Raw(QualifiedName newName, QualifiedName oldName, boolean ifNotExists)
        {
            this.newName = newName;
            this.oldName = oldName;
            this.ifNotExists = ifNotExists;
        }

        @Override
        public CQLStatement prepare(ClientState state)
        {
            String oldKeyspace = oldName.hasKeyspace() ? oldName.getKeyspace() : state.getKeyspace();
            String newKeyspace = newName.hasKeyspace() ? newName.getKeyspace() : state.getKeyspace();
            return new CopyTableStatement(oldKeyspace, newKeyspace, oldName.getName(), newName.getName(), ifNotExists, createLikeOption, attrs);
        }

        public void withLikeOption(CreateLikeOption option)
        {
            this.createLikeOption = option;
        }
    }

    public enum CreateLikeOption
    {
        INDEXES;
    }
}
