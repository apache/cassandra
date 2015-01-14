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

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.IRoleManager.Option;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.cql3.RoleOptions;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class AlterRoleStatement extends AuthenticationStatement
{
    private final String role;
    private final RoleOptions opts;

    public AlterRoleStatement(RoleName name, RoleOptions opts)
    {
        this.role = name.getName();
        this.opts = opts;
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        opts.validate();

        if (opts.isEmpty())
            throw new InvalidRequestException("ALTER [ROLE|USER] can't be empty");

        // validate login here before checkAccess to avoid leaking user existence to anonymous users.
        state.ensureNotAnonymous();
        if (!DatabaseDescriptor.getRoleManager().isExistingRole(role))
            throw new InvalidRequestException(String.format("%s doesn't exist", role));
    }

    public void checkAccess(ClientState state) throws UnauthorizedException
    {
        AuthenticatedUser user = state.getUser();
        boolean isSuper = user.isSuper();

        if (opts.getOptions().containsKey(Option.SUPERUSER) && user.getRoles().contains(role))
            throw new UnauthorizedException("You aren't allowed to alter your own superuser " +
                                            "status or that of a role granted to you");

        if (opts.getOptions().containsKey(Option.SUPERUSER) && !isSuper)
            throw new UnauthorizedException("Only superusers are allowed to alter superuser status");

        if (!user.isSuper() && !user.getName().equals(role))
            throw new UnauthorizedException("You aren't allowed to alter this role");

        if (!isSuper)
        {
            for (Option option : opts.getOptions().keySet())
            {
                if (!DatabaseDescriptor.getRoleManager().alterableOptions().contains(option))
                    throw new UnauthorizedException(String.format("You aren't allowed to alter %s", option));
            }
        }
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        if (!opts.isEmpty())
            DatabaseDescriptor.getRoleManager().alterRole(state.getUser(), role, opts.getOptions());
        return null;
    }
}
