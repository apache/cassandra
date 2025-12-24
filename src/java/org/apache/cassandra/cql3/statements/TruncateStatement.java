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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.Iterables;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static java.util.stream.Collectors.joining;

public class TruncateStatement extends QualifiedStatement implements CQLStatement
{
    public TruncateStatement(QualifiedName name)
    {
        super(name);
    }

    public TruncateStatement prepare(ClientState state)
    {
        return this;
    }

    public void authorize(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        if (name() == null)
            state.ensureKeyspacePermission(keyspace(), Permission.DROP);
        else
            state.ensureTablePermission(keyspace(), name(), Permission.MODIFY);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        if (name() == null)
            Schema.instance.validateKeyspace(keyspace());
        else
            Schema.instance.validateTable(keyspace(), name());

        Guardrails.dropTruncateTableEnabled.ensureEnabled(state);
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime) throws InvalidRequestException, TruncateException
    {
        if (name() == null)
        {
            Iterable<TableMetadata> tablesIterable = Iterables.filter(Schema.instance.getTablesAndViews(keyspace()), tmd -> !tmd.isView());
            List<TableMetadata> tablesToTruncate = new ArrayList<>();
            for (TableMetadata t : tablesIterable)
                tablesToTruncate.add(t);

            for (TableMetadata tmd : tablesIterable)
            {
                try
                {
                    truncateOne(tmd.keyspace, tmd.name);
                    tablesToTruncate.remove(tmd);
                }
                catch (TruncateException ex)
                {
                    throw new TruncateException(ex, "Tables not truncated: " + tablesToTruncate.stream()
                                                                                               .map(TableMetadata::toString)
                                                                                               .collect(joining(",")));
                }
            }
        }
        else
            truncateOne(keyspace(), name());

        return null;
    }

    private void truncateOne(String keyspace, String table)
    {
        try
        {
            TableMetadata metaData = Schema.instance.getTableMetadata(keyspace, table);

            if (metaData == null)
                throw new InvalidRequestException(String.format("Cannot TRUNCATE %s.%s as it does not exist.", keyspace(), name()));

            if (metaData.isView())
                throw new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead");

            if (metaData.isVirtual())
                executeForVirtualTable(metaData.id);
            else
                StorageProxy.truncateBlocking(keyspace, table);
        }
        catch (UnavailableException | TimeoutException e)
        {
            throw new TruncateException(e);
        }
    }

    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        try
        {
            TableMetadata metaData = Schema.instance.getTableMetadata(keyspace(), name());

            if (metaData == null)
                throw new InvalidRequestException(String.format("Cannot TRUNCATE %s.%s as it does not exist.", keyspace(), name()));

            if (metaData.isView())
                throw new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead");

            if (metaData.isVirtual())
            {
                executeForVirtualTable(metaData.id);
            }
            else
            {
                ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(name());
                cfs.truncateBlocking();
            }
        }
        catch (Exception e)
        {
            throw new TruncateException(e);
        }
        return null;
    }

    private void executeForVirtualTable(TableId id)
    {
        VirtualTable maybeVTable = VirtualKeyspaceRegistry.instance.getTableNullable(id);
        if (maybeVTable != null)
            maybeVTable.truncate();
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        if (name() == null)
            return new AuditLogContext(AuditLogEntryType.TRUNCATE, keyspace());
        else
            return new AuditLogContext(AuditLogEntryType.TRUNCATE, keyspace(), name());
    }
}
