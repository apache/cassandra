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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.TruncateException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
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
    }

    public void resolveTimeout(QueryOptions options, QueryState state)
    {
        state.setTimeoutInNanos(options.calculateTimeout(DatabaseDescriptor::getTruncateRpcTimeout, TimeUnit.NANOSECONDS));
    }

    public ResultMessage execute(QueryState state, QueryOptions options) throws InvalidRequestException, TruncateException
    {
        try
        {
            TableMetadata metaData = Schema.instance.getTableMetadata(keyspace(), name());
            if (metaData.isView())
                throw new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead");

            if (metaData.isVirtual())
                throw new InvalidRequestException("Cannot truncate virtual tables");

            StorageProxy.truncateBlocking(keyspace(), name(), state);
        }
        catch (UnavailableException | TimeoutException e)
        {
            throw new TruncateException(e);
        }
        return null;
    }

    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        try
        {
            TableMetadata metaData = Schema.instance.getTableMetadata(keyspace(), name());
            if (metaData.isView())
                throw new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead");

            if (metaData.isVirtual())
                throw new InvalidRequestException("Cannot truncate virtual tables");

            ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(name());
            cfs.truncateBlocking();
        }
        catch (Exception e)
        {
            throw new TruncateException(e);
        }
        return null;
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
