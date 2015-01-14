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

import org.apache.cassandra.auth.IRoleManager.Option;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.cql3.RoleOptions;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class CreateRoleStatement extends AuthorizationStatement
{
    private final String role;
    private final RoleOptions opts;
    private final boolean ifNotExists;

    public CreateRoleStatement(RoleName name, RoleOptions options, boolean ifNotExists)
    {
        this.role = name.getName();
        this.opts = options;
        this.ifNotExists = ifNotExists;
    }

    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        if (!state.getUser().isSuper())
            throw new UnauthorizedException("Only superusers are allowed to perform CREATE [ROLE|USER] queries");
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        opts.validate();

        if (role.isEmpty())
            throw new InvalidRequestException("Role name can't be an empty string");

        // validate login here before checkAccess to avoid leaking role existence to anonymous users.
        state.ensureNotAnonymous();

        if (!ifNotExists && DatabaseDescriptor.getRoleManager().isExistingRole(role))
            throw new InvalidRequestException(String.format("%s already exists", role));

        for (Option option : opts.getOptions().keySet())
        {
            if (!DatabaseDescriptor.getRoleManager().supportedOptions().contains(option))
                throw new UnauthorizedException(String.format("You aren't allowed to alter %s", option));
        }
    }

    public ResultMessage execute(ClientState state) throws RequestExecutionException, RequestValidationException
    {
        // not rejected in validate()
        if (ifNotExists && DatabaseDescriptor.getRoleManager().isExistingRole(role))
            return null;

        DatabaseDescriptor.getRoleManager().createRole(state.getUser(), role, opts.getOptions());
        return null;
    }
}
