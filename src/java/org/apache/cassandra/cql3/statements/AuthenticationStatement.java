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

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleOptions;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.messages.ResultMessage;

import static com.google.common.collect.ImmutableList.of;
import static java.lang.String.format;
import static org.apache.cassandra.auth.AuthKeyspace.ROLES;
import static org.apache.cassandra.cql3.QueryProcessor.executeInternal;
import static org.apache.cassandra.schema.SchemaConstants.AUTH_KEYSPACE_NAME;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public abstract class AuthenticationStatement extends CQLStatement.Raw implements CQLStatement
{
    private static final List<ColumnSpecification> GENERATED_PASSWORD_METADATA =
    of(new ColumnSpecification(SchemaConstants.AUTH_KEYSPACE_NAME,
                               "generated_password",
                               new ColumnIdentifier("generated_password", true),
                               UTF8Type.instance));

    public AuthenticationStatement prepare(ClientState state)
    {
        return this;
    }

    @Override
    public ResultMessage execute(QueryState state, QueryOptions options, Dispatcher.RequestTime requestTime)
    throws RequestExecutionException, RequestValidationException
    {
        return execute(state.getClientState());
    }

    public abstract ResultMessage execute(ClientState state) throws RequestExecutionException, RequestValidationException;

    @Override
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
        if (rows != null && !rows.isEmpty())
        {
            UntypedResultSet.Row row = rows.one();
            if (row.has("salted_hash"))
                return row.getString("salted_hash");
        }

        return null;
    }

    protected ResultMessage getResultMessage(RoleOptions opts)
    {
        if (!(opts.isGeneratedPassword() && opts.getPassword().isPresent()))
            return null;

        ResultSet.ResultMetadata resultMetadata = new ResultSet.ResultMetadata(GENERATED_PASSWORD_METADATA);
        ResultSet result = new ResultSet(resultMetadata);

        result.addColumnValue(bytes(opts.getPassword().get()));

        return new ResultMessage.Rows(result);
    }
}

