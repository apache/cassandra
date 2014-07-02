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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.config.IndexType;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.transport.Event;

/** A <code>CREATE INDEX</code> statement parsed from a CQL query. */
public class CreateIndexStatement extends SchemaAlteringStatement
{
    private static final Logger logger = LoggerFactory.getLogger(CreateIndexStatement.class);

    private final String indexName;
    private final IndexTarget target;
    private final IndexPropDefs properties;
    private final boolean ifNotExists;

    public CreateIndexStatement(CFName name,
                                String indexName,
                                IndexTarget target,
                                IndexPropDefs properties,
                                boolean ifNotExists)
    {
        super(name);
        this.indexName = indexName;
        this.target = target;
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

        ColumnDefinition cd = cfm.getColumnDefinition(target.column);

        if (cd == null)
            throw new InvalidRequestException("No column definition found for column " + target.column);

        boolean isMap = cd.type instanceof MapType;
        if (target.isCollectionKeys && !isMap)
            throw new InvalidRequestException("Cannot create index on keys of column " + target + " with non map type");

        if (cd.getIndexType() != null)
        {
            boolean previousIsKeys = cd.getIndexOptions().containsKey("index_keys");
            if (isMap && target.isCollectionKeys != previousIsKeys)
            {
                String msg = "Cannot create index on %s %s, an index on %s %s already exists and indexing "
                           + "a map on both keys and values at the same time is not currently supported";
                throw new InvalidRequestException(String.format(msg,
                                                                target.column, target.isCollectionKeys ? "keys" : "values",
                                                                target.column, previousIsKeys ? "keys" : "values"));
            }

            if (ifNotExists)
                return;
            else
                throw new InvalidRequestException("Index already exists");
        }

        properties.validate();

        // TODO: we could lift that limitation
        if (cfm.comparator.isDense() && cd.kind != ColumnDefinition.Kind.REGULAR)
            throw new InvalidRequestException(String.format("Secondary index on %s column %s is not yet supported for compact table", cd.kind, target.column));

        // It would be possible to support 2ndary index on static columns (but not without modifications of at least ExtendedFilter and
        // CompositesIndex) and maybe we should, but that means a query like:
        //     SELECT * FROM foo WHERE static_column = 'bar'
        // would pull the full partition every time the static column of partition is 'bar', which sounds like offering a
        // fair potential for foot-shooting, so I prefer leaving that to a follow up ticket once we have identified cases where
        // such indexing is actually useful.
        if (cd.isStatic())
            throw new InvalidRequestException("Secondary indexes are not allowed on static columns");

        if (cd.kind == ColumnDefinition.Kind.PARTITION_KEY && cd.isOnAllComponents())
            throw new InvalidRequestException(String.format("Cannot add secondary index to already primarily indexed column %s", target.column));
    }

    public void announceMigration(boolean isLocalOnly) throws RequestValidationException
    {
        logger.debug("Updating column {} definition for index {}", target.column, indexName);
        CFMetaData cfm = Schema.instance.getCFMetaData(keyspace(), columnFamily()).copy();
        ColumnDefinition cd = cfm.getColumnDefinition(target.column);

        if (cd.getIndexType() != null && ifNotExists)
            return;

        if (properties.isCustom)
        {
            cd.setIndexType(IndexType.CUSTOM, properties.getOptions());
        }
        else if (cfm.comparator.isCompound())
        {
            Map<String, String> options = Collections.emptyMap();
            // For now, we only allow indexing values for collections, but we could later allow
            // to also index map keys, so we record that this is the values we index to make our
            // lives easier then.
            if (cd.type.isCollection())
                options = ImmutableMap.of(target.isCollectionKeys ? "index_keys" : "index_values", "");
            cd.setIndexType(IndexType.COMPOSITES, options);
        }
        else
        {
            cd.setIndexType(IndexType.KEYS, Collections.<String, String>emptyMap());
        }

        cd.setIndexName(indexName);
        cfm.addDefaultIndexNames();
        MigrationManager.announceColumnFamilyUpdate(cfm, false, isLocalOnly);
    }

    public Event.SchemaChange changeEvent()
    {
        // Creating an index is akin to updating the CF
        return new Event.SchemaChange(Event.SchemaChange.Change.UPDATED, Event.SchemaChange.Target.TABLE, keyspace(), columnFamily());
    }
}
