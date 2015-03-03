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

import org.apache.cassandra.auth.*;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class DropRoleStatement extends AuthenticationStatement
{
    private final RoleResource role;
    private final boolean ifExists;

    public DropRoleStatement(RoleName name, boolean ifExists)
    {
        this.role = RoleResource.role(name.getName());
        this.ifExists = ifExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        super.checkPermission(state, Permission.DROP, role);
        if (Roles.hasSuperuserStatus(role) && !state.getUser().isSuper())
            throw new UnauthorizedException("Only superusers can drop a role with superuser status");
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        // validate login here before checkAccess to avoid leaking user existence to anonymous users.
        state.ensureNotAnonymous();

        if (!ifExists && !DatabaseDescriptor.getRoleManager().isExistingRole(role))
            throw new InvalidRequestException(String.format("%s doesn't exist", role.getRoleName()));

        AuthenticatedUser user = state.getUser();
        if (user != null && user.getName().equals(role.getRoleName()))
            throw new InvalidRequestException("Cannot DROP primary role for current login");
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        // not rejected in validate()
        if (ifExists && !DatabaseDescriptor.getRoleManager().isExistingRole(role))
            return null;

        // clean up grants and permissions of/on the dropped role.
        DatabaseDescriptor.getRoleManager().dropRole(state.getUser(), role);
        DatabaseDescriptor.getAuthorizer().revokeAllFrom(role);
        DatabaseDescriptor.getAuthorizer().revokeAllOn(role);
        return null;
    }
}
