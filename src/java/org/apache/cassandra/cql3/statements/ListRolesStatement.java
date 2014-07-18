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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.commons.lang.StringUtils;

public class ListRolesStatement extends AuthenticationStatement
{
    private static final String KS = Auth.AUTH_KS;
    private static final String CF = Auth.ROLES_CF;

    private static final List<ColumnSpecification> metadata;

    static
    {
        List<ColumnSpecification> columns = new ArrayList<ColumnSpecification>(4);
        columns.add(new ColumnSpecification(KS, CF, new ColumnIdentifier("role", true), UTF8Type.instance));
        metadata = Collections.unmodifiableList(columns);
    }

    private final String username;

    public ListRolesStatement(String username)
    {
        this.username = username;
    }

    public void validate(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.ensureNotAnonymous();

        if (!state.getUser().isSuper() && (username != null))
            throw new UnauthorizedException("Only superusers are allowed to LIST ROLES for another user or role");

        if ((username != null) && !Auth.isExistingUser(username))
            throw new InvalidRequestException(String.format("User %s doesn't exist", username));
    }

    public void checkAccess(ClientState state)
    {
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        if (state.getUser().isSuper())
        {
            if (StringUtils.isEmpty(username))
                return resultMessage(Auth.getRoles());
            else
                return resultMessage(DatabaseDescriptor.getAuthenticator().listRoles(username));
        }
        else
            return resultMessage(DatabaseDescriptor.getAuthenticator().listRoles(state.getUser().getName()));
    }

    private ResultMessage resultMessage(Set<String> roles)
    {
        if (roles.isEmpty())
            return new ResultMessage.Void();

        ResultSet result = new ResultSet(metadata);
        for (String rolename : roles)
        {
            result.addColumnValue(UTF8Type.instance.decompose(rolename));
        }
        return new ResultMessage.Rows(result);
    }
}
