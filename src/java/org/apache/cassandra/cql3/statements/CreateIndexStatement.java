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

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.CFName;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.IndexName;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;

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
        CFMetaData cfm = ThriftValidation.validateColumnFamily(keyspace(), columnFamily());

        if (cfm.isCounter())
            throw new InvalidRequestException("Secondary indexes are not supported on counter tables");

        if (cfm.isView())
            throw new InvalidRequestException("Secondary indexes are not supported on materialized views");

        if (cfm.isCompactTable() && !cfm.isStaticCompactTable())
            throw new InvalidRequestException("Secondary indexes are not supported on COMPACT STORAGE tables that have clustering columns");

        List<IndexTarget> targets = new ArrayList<>(rawTargets.size());
        for (IndexTarget.Raw rawTarget : rawTargets)
            targets.add(rawTarget.prepare(cfm));

        if (targets.isEmpty() && !properties.isCustom)
            throw new InvalidRequestException("Only CUSTOM indexes can be created without specifying a target column");

        if (targets.size() > 1)
            validateTargetsForMultiColumnIndex(targets);

        for (IndexTarget target : targets)
        {
            ColumnDefinition cd = cfm.getColumnDefinition(target.column);

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
            if (cfm.isCompactTable())
            {
                if (cd.isPrimaryKeyColumn())
                    throw new InvalidRequestException("Secondary indexes are not supported on PRIMARY KEY columns in COMPACT STORAGE tables");
                if (cfm.compactValueColumn().equals(cd))
                    throw new InvalidRequestException("Secondary indexes are not supported on compact value column of COMPACT STORAGE tables");
            }

            if (cd.kind == ColumnDefinition.Kind.PARTITION_KEY && cfm.getKeyValidatorAsClusteringComparator().size() == 1)
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
            if (Schema.instance.getKSMetaData(keyspace()).existingIndexNames(null).contains(indexName))
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

    private void validateIsSimpleIndexIfTargetColumnNotCollection(ColumnDefinition cd, IndexTarget target) throws InvalidRequestException
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
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily()).copy();
        List<IndexTarget> targets = new ArrayList<>(rawTargets.size());
        for (IndexTarget.Raw rawTarget : rawTargets)
            targets.add(rawTarget.prepare(cfm));

        String acceptedName = indexName;
        if (Strings.isNullOrEmpty(acceptedName))
        {
            acceptedName = Indexes.getAvailableIndexName(keyspace(),
                                                         columnFamily(),
                                                         targets.size() == 1 ? targets.get(0).column.toString() : null);
        }

        if (Schema.instance.getKSMetaData(keyspace()).existingIndexNames(null).contains(acceptedName))
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
            kind = cfm.isCompound() ? IndexMetadata.Kind.COMPOSITES : IndexMetadata.Kind.KEYS;
        }

        IndexMetadata index = IndexMetadata.fromIndexTargets(cfm, targets, acceptedName, kind, indexOptions);

        // check to disallow creation of an index which duplicates an existing one in all but name
        Optional<IndexMetadata> existingIndex = Iterables.tryFind(cfm.getIndexes(), existing -> existing.equalsWithoutName(index));
        if (existingIndex.isPresent())
        {
            if (ifNotExists)
                return null;
            else
                throw new InvalidRequestException(String.format("Index %s is a duplicate of existing index %s",
                                                                index.name,
                                                                existingIndex.get().name));
        }

        logger.trace("Updating index definition for {}", indexName);
        cfm.indexes(cfm.getIndexes().with(index));

        MigrationManager.announceColumnFamilyUpdate(cfm, isLocalOnly);

        // Creating an index is akin to updating the CF
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }
}
