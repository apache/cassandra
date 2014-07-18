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
import org.apache.commons.lang.StringUtils;

public class CreateRoleStatement extends AuthenticationStatement
{
    private final String rolename;
    private final boolean ifNotExists;

    public CreateRoleStatement(String rolename, boolean ifNotExists)
    {
        this.rolename = rolename;
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
        if (StringUtils.isEmpty(rolename))
            throw new InvalidRequestException("Rolename can't be an empty string");

        // validate login here before checkAccess to avoid leaking role existence to anonymous users.
        state.ensureNotAnonymous();

        if (!ifNotExists && Auth.isExistingRole(rolename))
            throw new InvalidRequestException(String.format("Role %s already exists", rolename));
    }

    @Override
    public ResultMessage execute(ClientState state) throws RequestExecutionException, RequestValidationException
    {
        // not rejected in validate()
        if (ifNotExists && Auth.isExistingRole(rolename))
            return null;

        Auth.insertRole(rolename);
        return null;
    }
}
