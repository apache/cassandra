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

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class UseStatement extends CQLStatement.Raw implements CQLStatement
{
    private final String keyspace;

    public UseStatement(String keyspace)
    {
        this.keyspace = keyspace;
    }

    public UseStatement prepare(ClientState state)
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13426
        return this;
    }

    public void authorize(ClientState state) throws UnauthorizedException
    {
        state.validateLogin();
    }

    public void validate(ClientState state) throws InvalidRequestException
    {
    }

    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime) throws InvalidRequestException
    {
        state.getClientState().setKeyspace(keyspace);
        return new ResultMessage.SetKeyspace(keyspace);
    }

    public ResultMessage executeLocally(QueryState state, QueryOptions options) throws InvalidRequestException
    {
        // In production, internal queries are exclusively on the system keyspace and 'use' is thus useless
        // but for some unit tests we need to set the keyspace (e.g. for tests with DROP INDEX)
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-12256
        return execute(state, options, System.nanoTime());
    }
    
    @Override
    public String toString()
    {
//IC see: https://issues.apache.org/jira/browse/CASSANDRA-13653
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.USE_KEYSPACE, keyspace);
    }
}
