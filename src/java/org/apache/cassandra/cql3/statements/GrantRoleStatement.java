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
import org.apache.cassandra.auth.User;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

public class GrantRoleStatement extends RoleManagementStatement
{
    public GrantRoleStatement(String username, String rolename)
    {
        super(username, rolename);
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        if (DatabaseDescriptor.getAuthenticator().listRoles(username).contains(rolename))
            throw new InvalidRequestException(String.format("User %s is already granted role %s", username, rolename));

        DatabaseDescriptor.getAuthenticator().grantRole(username, rolename);
        return null;
    }
}
