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

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;

import static java.lang.String.format;
import static org.apache.cassandra.auth.AuthKeyspace.ROLES;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;

public abstract class AuthenticationStatement extends CQLStatement.Raw implements CQLStatement
{
    public AuthenticationStatement prepare(ClientState state)
    {
        return this;
    }

    public ResultMessage execute(QueryState state, QueryOptions options, long queryStartNanoTime)
    throws RequestExecutionException, RequestValidationException
    {
        return execute(state.getClientState());
    }

    public abstract ResultMessage execute(ClientState state) throws RequestExecutionException, RequestValidationException;

    public ResultMessage executeLocally(QueryState state, QueryOptions options)
    {
        // executeLocally is for local query only, thus altering users doesn't make sense and is not supported
        throw new UnsupportedOperationException();
    }

    public void checkPermission(ClientState state, Permission required, RoleResource resource) throws UnauthorizedException
    {
        try
        {
            state.ensurePermission(required, resource);
        }
        catch (UnauthorizedException e)
        {
            // Catch and rethrow with a more friendly message
            throw new UnauthorizedException(String.format("User %s does not have sufficient privileges " +
                                                          "to perform the requested operation",
                                                          state.getUser().getName()));
        }
    }

    public String obfuscatePassword(String query)
    {
        return query;
    }

    protected static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    protected String getSaltedHash(String roleName)
    {
        UntypedResultSet rows = executeInternal(format("SELECT salted_hash FROM %s.%s WHERE role = ?",
                                                       AUTH_KEYSPACE_NAME, ROLES),
                                                roleName);
        if (rows != null)
        {
            UntypedResultSet.Row row = rows.one();
            if (row.has("salted_hash"))
            {
                return row.getString("salted_hash");
            }
        }

        return null;
    }
}

