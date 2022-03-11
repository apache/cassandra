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

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeoutException;
import java.util.function.UnaryOperator;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.guardrails.Guardrails;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.FBUtilities;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import static org.apache.cassandra.config.CassandraRelevantProperties.TRUNCATE_STATEMENT_PROVIDER;

public class TruncateStatement implements CQLStatement, CQLStatement.SingleKeyspaceCqlStatement
{
    private final String rawCQLStatement;
    private final QualifiedName qualifiedName;

    public TruncateStatement(String queryString, QualifiedName name)
    {
        this.rawCQLStatement = queryString;
        this.qualifiedName = name;
    }

    @Override
    public String getRawCQLStatement()
    {
        return rawCQLStatement;
    }

    @Override
    public String keyspace()
    {
        return qualifiedName.getKeyspace();
    }

    public String name()
    {
        return qualifiedName.getName();
    }

    public void authorize(ClientState state) throws InvalidRequestException, UnauthorizedException
    {
        state.ensureTablePermission(keyspace(), name(), Permission.MODIFY);
    }

    @Override
    public void validate(QueryState state) throws InvalidRequestException
    {
        Guardrails.truncateTableEnabled.ensureEnabled(state);

        Schema.instance.validateTable(keyspace(), name());
    }

    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws InvalidRequestException, TruncateException
    {
        try
        {
            TableMetadata metaData = Schema.instance.getTableMetadata(keyspace(), name());
            if (metaData.isView())
                throw new InvalidRequestException("Cannot TRUNCATE materialized view directly; must truncate base table instead");

            if (metaData.isVirtual())
                throw new InvalidRequestException("Cannot truncate virtual tables");

            StorageProxy.instance.truncateBlocking(keyspace(), name());
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

            doTruncateBlocking();
        }
        catch (Exception e)
        {
            throw new TruncateException(e);
        }
        return null;
    }

    protected void doTruncateBlocking()
    {
        ColumnFamilyStore cfs = Keyspace.open(keyspace()).getColumnFamilyStore(name());
        cfs.truncateBlocking();
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

    public static final class Raw extends QualifiedStatement<TruncateStatement>
    {
        public Raw(QualifiedName name)
        {
            super(name);
        }

        @Override
        public TruncateStatement prepare(ClientState state, UnaryOperator<String> keyspaceMapper)
        {
            setKeyspace(state);
            String ks = keyspaceMapper.apply(keyspace());
            QualifiedName qual = qualifiedName;
            if (!ks.equals(qual.getKeyspace()))
                qual = new QualifiedName(ks, qual.getName());
            return provider.createTruncateStatement(rawCQLStatement, qual);
        }
    }

    private static TruncateStatementProvider getProviderFromProperty()
    {
        try
        {
            return (TruncateStatementProvider)FBUtilities.classForName(TRUNCATE_STATEMENT_PROVIDER.getString(),
                                                                       "Truncate statement provider")
                                                         .getConstructor().newInstance();
        }
        catch (NoSuchMethodException | IllegalAccessException | InstantiationException |
               InvocationTargetException e)
        {
            throw new RuntimeException("Unable to find a truncate statement provider with name " +
                                       TRUNCATE_STATEMENT_PROVIDER.getString(), e);
        }
    }

    private static final TruncateStatementProvider provider = TRUNCATE_STATEMENT_PROVIDER.isPresent() ?
                                                              getProviderFromProperty() :
                                                              new DefaultTruncateStatementProvider();

    public static interface TruncateStatementProvider
    {
        public TruncateStatement createTruncateStatement(String rawCQLStatement, QualifiedName qual);
    }

    public static final class DefaultTruncateStatementProvider implements TruncateStatementProvider
    {
        @Override
        public TruncateStatement createTruncateStatement(String rawCQLStatement, QualifiedName qual)
        {
            return new TruncateStatement(rawCQLStatement, qual);
        }
    }
}
