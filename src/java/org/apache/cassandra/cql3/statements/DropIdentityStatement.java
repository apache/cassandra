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
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * Cqlsh statement to remove identity from identity_to_roles table.
 * Ex: DROP IDENTITY 'testIdentity'
 */
public class DropIdentityStatement extends AuthenticationStatement
{
    final String identity;
    final boolean ifExists;

    public DropIdentityStatement(String identity, boolean ifExists)
    {
        this.identity = identity;
        this.ifExists = ifExists;
    }

    @Override
    public void authorize(ClientState state)
    {
        checkPermission(state, Permission.DROP, state.getUser().getPrimaryRole());
    }

    @Override
    public void validate(ClientState state)
    {
        state.ensureNotAnonymous();

        if (!ifExists && !DatabaseDescriptor.getRoleManager().isExistingIdentity(identity))
        {
            throw new InvalidRequestException(String.format("identity '%s' doesn't exist", identity));
        }
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.DROP_IDENTITY);
    }

    @Override
    public ResultMessage execute(ClientState state) throws RequestExecutionException, RequestValidationException
    {
        // not rejected in validate()
        if(!ifExists || DatabaseDescriptor.getRoleManager().isExistingIdentity(identity))
        {
            DatabaseDescriptor.getRoleManager().dropIdentity(identity);
        }
        return null;
    }
}
