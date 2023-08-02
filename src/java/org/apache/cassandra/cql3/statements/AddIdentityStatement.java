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

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * Cqlsh statement to add identity into roles_to_identity table for storing authorized identities for mTLS connections.
 * Performs some checks before adding the identity to roles table.
 *
 * EX: ADD IDENTITY 'testIdentity' TO ROLE 'testRole'
 */
public class AddIdentityStatement extends AuthenticationStatement
{
    final String identity;
    final String role;
    final boolean ifNotExists;

    public AddIdentityStatement(String identity, String role, boolean ifNotExists)
    {
        this.role = role;
        this.identity = identity;
        this.ifNotExists = ifNotExists;
    }

    @Override
    public void authorize(ClientState state)
    {
        checkPermission(state, Permission.CREATE, state.getUser().getPrimaryRole());
    }

    @Override
    public void validate(ClientState state)
    {
        state.ensureNotAnonymous();

        if (!DatabaseDescriptor.getRoleManager().isExistingRole(RoleResource.role(role)))
        {
            throw new InvalidRequestException(String.format("Can not add identity for non-existent role '%s'", role));
        }

        if (!ifNotExists && DatabaseDescriptor.getRoleManager().isExistingIdentity(identity))
            throw new InvalidRequestException(String.format("%s already exists", identity));
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_IDENTITY);
    }

    @Override
    public ResultMessage execute(ClientState state) throws RequestExecutionException, RequestValidationException
    {
        if(!ifNotExists || !DatabaseDescriptor.getRoleManager().isExistingIdentity(identity))
        {
            DatabaseDescriptor.getRoleManager().addIdentity(identity, role);
        }
        return null;
    }
}
