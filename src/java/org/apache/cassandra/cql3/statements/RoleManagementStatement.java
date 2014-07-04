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

import org.apache.cassandra.auth.IGrantee;
import org.apache.cassandra.auth.Role;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;

public abstract class RoleManagementStatement extends AuthorizationStatement
{
    protected final Role role;
    protected final IGrantee grantee;

    public RoleManagementStatement(Role role, IGrantee grantee)
    {
        this.role = role;
        this.grantee = grantee;
    }

    @Override
    public void checkAccess(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        if (!state.getUser().isSuper())
            throw new UnauthorizedException("Only superusers are allowed to perform role management queries");
    }

    @Override
    public void validate(ClientState state) throws RequestValidationException
    {
        state.ensureNotAnonymous();

        if (!role.isExisting())
            throw new InvalidRequestException(String.format("%s %s doesn't exist", role.getType(), role.getName()));

        if (!grantee.isExisting())
            throw new InvalidRequestException(String.format("%s %s doesn't exist", grantee.getType(), grantee.getName()));

        if (role.equals(grantee))
            throw new InvalidRequestException("A role cannot operate on itself");
    }
}
