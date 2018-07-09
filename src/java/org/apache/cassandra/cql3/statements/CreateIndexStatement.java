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

import java.util.*;

import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.MigrationManager;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Event;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static org.apache.cassandra.cql3.statements.RequestValidations.checkFalse;
import static org.apache.cassandra.cql3.statements.RequestValidations.invalidRequest;

/** A <code>CREATE INDEX</code> statement parsed from a CQL query. */
public class CreateIndexStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateIndexStatement.class);

    private final String indexName;
    private final List<IndexTarget.Raw> rawTargets;
    private final IndexPropDefs properties;
    private final boolean ifNotExists;

    public CreateIndexStatement(CFName name,
                                IndexName indexName,
                                List<IndexTarget.Raw> targets,
                                IndexPropDefs properties,
                                boolean ifNotExists)
    {
        super(name);
        this.indexName = indexName.getIdx();
        this.rawTargets = targets;
        this.properties = properties;
        this.ifNotExists = ifNotExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.hasColumnFamilyAccess(keyspace(), columnFamily(), Permission.ALTER);
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        TableMetadata table = Schema.instance.validateTable(keyspace(), columnFamily());

        if (table.isVirtual())
            throw new InvalidRequestException("Secondary indexes are not supported on virtual tables");

        if (table.isCounter())
            throw new InvalidRequestException("Secondary indexes are not supported on counter tables");

        if (table.isView())
            throw new InvalidRequestException("Secondary indexes are not supported on materialized views");

        if (table.isCompactTable() && !table.isStaticCompactTable())
            throw new InvalidRequestException("Secondary indexes are not supported on COMPACT STORAGE tables that have clustering columns");

        List<IndexTarget> targets = new ArrayList<>(rawTargets.size());
        for (IndexTarget.Raw rawTarget : rawTargets)
            targets.add(rawTarget.prepare(table));

        if (targets.isEmpty() && !properties.isCustom)
            throw new InvalidRequestException("Only CUSTOM indexes can be created without specifying a target column");

        if (targets.size() > 1)
            validateTargetsForMultiColumnIndex(targets);

        for (IndexTarget target : targets)
        {
            ColumnMetadata cd = table.getColumn(target.column);

            if (cd == null)
                throw new InvalidRequestException("No column definition found for column " + target.column);

            if (cd.type.referencesDuration())
            {
                checkFalse(cd.type.isCollection(), "Secondary indexes are not supported on collections containing durations");
                checkFalse(cd.type.isTuple(), "Secondary indexes are not supported on tuples containing durations");
                checkFalse(cd.type.isUDT(), "Secondary indexes are not supported on UDTs containing durations");
                throw invalidRequest("Secondary indexes are not supported on duration columns");
            }

            // TODO: we could lift that limitation
            if (table.isCompactTable() && cd.isPrimaryKeyColumn())
                throw new InvalidRequestException("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables");

            if (cd.kind == ColumnMetadata.Kind.PARTITION_KEY && table.partitionKeyColumns().size() == 1)
                throw new InvalidRequestException(String.format("Cannot create secondary index on partition key column %s", target.column));

            boolean isMap = cd.type instanceof MapType;
            boolean isFrozenCollection = cd.type.isCollection() && !cd.type.isMultiCell();
            if (isFrozenCollection)
            {
                validateForFrozenCollection(target);
            }
            else
            {
                validateNotFullIndex(target);
                validateIsSimpleIndexIfTargetColumnNotCollection(cd, target);
                validateTargetColumnIsMapIfIndexInvolvesKeys(isMap, target);
            }

            checkFalse(cd.type.isUDT() && cd.type.isMultiCell(), "Secondary indexes are not supported on non-frozen UDTs");
        }

        if (!Strings.isNullOrEmpty(indexName))
        {
            if (Schema.instance.getKeyspaceMetadata(keyspace()).existingIndexNames(null).contains(indexName))
            {
                if (ifNotExists)
                    return;
                else
                    throw new InvalidRequestException(String.format("Index %s already exists", indexName));
            }
        }

        properties.validate();
    }

    private void validateForFrozenCollection(IndexTarget target) throws InvalidRequestException
    {
        if (target.type != IndexTarget.Type.FULL)
            throw new InvalidRequestException(String.format("Cannot create %s() index on frozen column %s. " +
                                                            "Frozen collections only support full() indexes",
                                                            target.type, target.column));
    }

    private void validateNotFullIndex(IndexTarget target) throws InvalidRequestException
    {
        if (target.type == IndexTarget.Type.FULL)
            throw new InvalidRequestException("full() indexes can only be created on frozen collections");
    }

    private void validateIsSimpleIndexIfTargetColumnNotCollection(ColumnMetadata cd, IndexTarget target) throws InvalidRequestException
    {
        if (!cd.type.isCollection() && target.type != IndexTarget.Type.SIMPLE)
            throw new InvalidRequestException(String.format("Cannot create %s() index on %s. " +
                                                            "Non-collection columns support only simple indexes",
                                                            target.type.toString(), target.column));
    }

    private void validateTargetColumnIsMapIfIndexInvolvesKeys(boolean isMap, IndexTarget target) throws InvalidRequestException
    {
        if (target.type == IndexTarget.Type.KEYS || target.type == IndexTarget.Type.KEYS_AND_VALUES)
        {
            if (!isMap)
                throw new InvalidRequestException(String.format("Cannot create index on %s of column %s with non-map type",
                                                                target.type, target.column));
        }
    }

    private void validateTargetsForMultiColumnIndex(List<IndexTarget> targets)
    {
        if (!properties.isCustom)
            throw new InvalidRequestException("Only CUSTOM indexes support multiple columns");

        Set<ColumnIdentifier> columns = Sets.newHashSetWithExpectedSize(targets.size());
        for (IndexTarget target : targets)
            if (!columns.add(target.column))
                throw new InvalidRequestException("Duplicate column " + target.column + " in index target list");
    }

    public Event.SchemaChange announceMigration(QueryState queryState, boolean isLocalOnly) throws RequestValidationException
    {
        TableMetadata current = Schema.instance.getTableMetadata(keyspace(), columnFamily());
        List<IndexTarget> targets = new ArrayList<>(rawTargets.size());
        for (IndexTarget.Raw rawTarget : rawTargets)
            targets.add(rawTarget.prepare(current));

        String acceptedName = indexName;
        if (Strings.isNullOrEmpty(acceptedName))
        {
            acceptedName = Indexes.getAvailableIndexName(keyspace(),
                                                         columnFamily(),
                                                         targets.size() == 1 ? targets.get(0).column.toString() : null);
        }

        if (Schema.instance.getKeyspaceMetadata(keyspace()).existingIndexNames(null).contains(acceptedName))
        {
            if (ifNotExists)
                return null;
            else
                throw new InvalidRequestException(String.format("Index %s already exists", acceptedName));
        }

        IndexMetadata.Kind kind;
        Map<String, String> indexOptions;
        if (properties.isCustom)
        {
            kind = IndexMetadata.Kind.CUSTOM;
            indexOptions = properties.getOptions();
        }
        else
        {
            indexOptions = Collections.emptyMap();
            kind = current.isCompound() ? IndexMetadata.Kind.COMPOSITES : IndexMetadata.Kind.KEYS;
        }

        IndexMetadata index = IndexMetadata.fromIndexTargets(targets, acceptedName, kind, indexOptions);

        // check to disallow creation of an index which duplicates an existing one in all but name
        Optional<IndexMetadata> existingIndex = Iterables.tryFind(current.indexes, existing -> existing.equalsWithoutName(index));
        if (existingIndex.isPresent())
        {
            if (ifNotExists)
                return null;
            else
                throw new InvalidRequestException(String.format("Index %s is a duplicate of existing index %s",
                                                                index.name,
                                                                existingIndex.get().name));
        }

        TableMetadata updated =
            current.unbuild()
                   .indexes(current.indexes.with(index))
                   .build();

        logger.trace("Updating index definition for {}", indexName);

        MigrationManager.announceTableUpdate(updated, isLocalOnly);

        // Creating an index is akin to updating the CF
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_INDEX, keyspace(), indexName);
    }
}
