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
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.SchemaConstants;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class ListRolesStatement extends AuthorizationStatement
{
    // pseudo-virtual cf as the actual datasource is dependent on the IRoleManager impl
    private static final String KS = SchemaConstants.AUTH_KEYSPACE_NAME;
    private static final String CF = AuthKeyspace.ROLES;

    private static final MapType optionsType = MapType.getInstance(UTF8Type.instance, UTF8Type.instance, false);
    private static final List<ColumnSpecification> metadata =
        ImmutableList.of(new ColumnSpecification(KS, CF, new ColumnIdentifier("role", true), UTF8Type.instance),
                         new ColumnSpecification(KS, CF, new ColumnIdentifier("super", true), BooleanType.instance),
                         new ColumnSpecification(KS, CF, new ColumnIdentifier("login", true), BooleanType.instance),
                         new ColumnSpecification(KS, CF, new ColumnIdentifier("options", true), optionsType));

    private final RoleResource grantee;
    private final boolean recursive;

    public ListRolesStatement()
    {
        this(new RoleName(), false);
    }

    public ListRolesStatement(RoleName grantee, boolean recursive)
    {
        this.grantee = grantee.hasName() ? RoleResource.role(grantee.getName()) : null;
        this.recursive = recursive;
    }

    public void validate(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.ensureNotAnonymous();

        if ((grantee != null) && !DatabaseDescriptor.getRoleManager().isExistingRole(grantee))
            throw new InvalidRequestException(String.format("%s doesn't exist", grantee));
    }

    public void checkAccess(ClientState state) throws InvalidRequestException
    {
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        // If the executing user has DESCRIBE permission on the root roles resource, let them list any and all roles
        boolean hasRootLevelSelect = DatabaseDescriptor.getAuthorizer()
                                                       .authorize(state.getUser(), RoleResource.root())
                                                       .contains(Permission.DESCRIBE);
        if (hasRootLevelSelect)
        {
            if (grantee == null)
                return resultMessage(DatabaseDescriptor.getRoleManager().getAllRoles());
            else
                return resultMessage(DatabaseDescriptor.getRoleManager().getRoles(grantee, recursive));
        }
        else
        {
            RoleResource currentUser = RoleResource.role(state.getUser().getName());
            if (grantee == null)
                return resultMessage(DatabaseDescriptor.getRoleManager().getRoles(currentUser, recursive));
            if (DatabaseDescriptor.getRoleManager().getRoles(currentUser, true).contains(grantee))
                return resultMessage(DatabaseDescriptor.getRoleManager().getRoles(grantee, recursive));
            else
                throw new UnauthorizedException(String.format("You are not authorized to view roles granted to %s ", grantee.getRoleName()));
        }
    }

    private ResultMessage resultMessage(Set<RoleResource> roles)
    {
        if (roles.isEmpty())
            return new ResultMessage.Void();

        List<RoleResource> sorted = Lists.newArrayList(roles);
        Collections.sort(sorted);
        return formatResults(sorted);
    }

    // overridden in ListUsersStatement to include legacy metadata
    protected ResultMessage formatResults(List<RoleResource> sortedRoles)
    {
        ResultSet result = new ResultSet(metadata);

        IRoleManager roleManager = DatabaseDescriptor.getRoleManager();
        for (RoleResource role : sortedRoles)
        {
            result.addColumnValue(UTF8Type.instance.decompose(role.getRoleName()));
            result.addColumnValue(BooleanType.instance.decompose(roleManager.isSuper(role)));
            result.addColumnValue(BooleanType.instance.decompose(roleManager.canLogin(role)));
            result.addColumnValue(optionsType.decompose(roleManager.getCustomOptions(role)));
        }
        return new ResultMessage.Rows(result);
    }
}
