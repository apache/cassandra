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
import org.apache.cassandra.auth.IGrantee;
import org.apache.cassandra.auth.Role;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class GrantRoleStatement extends RoleManagementStatement
{
    public GrantRoleStatement(Role role, IGrantee grantee)
    {
        super(role, grantee);
    }

    @Override
    public void validate(ClientState state) throws RequestValidationException
    {
        super.validate(state);
        if (Auth.getRoles(grantee, true).contains(role))
            throw new InvalidRequestException(String.format("%s %s is already granted %s %s",
                                                            grantee.getType(), grantee.getName(),
                                                            role.getType(), role.getName()));
        if (Auth.getRoles(role, true).contains(grantee))
            throw new InvalidRequestException("Grant will create circular dependency");
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        Auth.grantRole(role, grantee);
        return null;
    }
}
