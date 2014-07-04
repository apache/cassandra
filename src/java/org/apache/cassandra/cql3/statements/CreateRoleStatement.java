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

import org.apache.cassandra.auth.Auth;
import org.apache.cassandra.auth.Role;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class CreateRoleStatement extends AuthenticationStatement
{
    private final Role role;
    private final boolean ifNotExists;

    public CreateRoleStatement(Role role, boolean ifNotExists)
    {
        this.role = role;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        if (!state.getUser().isSuper())
            throw new UnauthorizedException("Only superusers are allowed to perform CREATE ROLE queries");
    }

    @Override
    public void validate(ClientState state) throws RequestValidationException
    {
        if (role.getName().isEmpty())
            throw new InvalidRequestException("Rolename can't be an empty string");

        // validate login here before checkAccess to avoid leaking role existence to anonymous users.
        state.ensureNotAnonymous();

        if (!ifNotExists && role.isExisting())
            throw new InvalidRequestException(String.format("Role %s already exists", role.getName()));
    }

    @Override
    public ResultMessage execute(ClientState state) throws RequestExecutionException, RequestValidationException
    {
        // not rejected in validate()
        if (ifNotExists && role.isExisting())
            return null;

        Auth.insertRole(role);
        return null;
    }
}
