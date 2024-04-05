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

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.TableTruncation;
import org.apache.cassandra.transport.messages.ResultMessage;

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
        state.ensureTablePermission(keyspace(), name(), Permission.MODIFY);
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
        Schema.instance.validateTable(keyspace(), name());
        Guardrails.dropTruncateTableEnabled.ensureEnabled(state);
    }

    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws InvalidRequestException, TruncateException
    {
        executeInternal(() -> {
            ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(name());
            long timestampMillis = queryStartNanoTime / 1000 / 1000;
            ClusterMetadataService.instance().commit(new TableTruncation(cfs.metadata.id, timestampMillis));
        });

        return null;
    }

    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        executeInternal(() -> {
            ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(name());

            // we need some timestamp too for local executions
            // as that one does not come as the param as execute() method has it
            // let's take it from state where is it stored in microseconds so we convert
            long timestampMillis = state.getTimestamp() / 1000;
            cfs.truncateBlocking(timestampMillis);
        });

        return null;
    }

    private void executeInternal(Runnable runnable)
    {
        try
        {
            TableMetadata tableMetadata = Schema.instance.getTableMetadata(keyspace(), name());
            assert tableMetadata != null : String.format("No table %s.%s found", keyspace(), name());

            if (tableMetadata.isView())
                throw new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead");

            if (tableMetadata.isVirtual())
            {
                VirtualTable tableNullable = VirtualKeyspaceRegistry.instance.getTableNullable(tableMetadata.id);
                assert tableNullable != null : "no virtual table of id " + tableMetadata.id;
                tableNullable.truncate();
            }
            else
            {
                runnable.run();
            }
        }
        catch (Exception e)
        {
            throw new TruncateException(e);
        }
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.TRUNCATE, keyspace(), name());
    }
}