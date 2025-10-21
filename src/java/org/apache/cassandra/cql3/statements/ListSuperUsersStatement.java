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

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.Roles;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.ResultSet;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

/**
 * LIST SUPERUSERS cql command returns list of roles with superuser privileges
 * This includes superusers and all roles who have superuser role granted in the roles hierarchy
 */
public class ListSuperUsersStatement extends AuthorizationStatement
{
    private static final List<ColumnSpecification> metadata =
    List.of(new ColumnSpecification(SchemaConstants.AUTH_KEYSPACE_NAME, AuthKeyspace.ROLES,
                                    new ColumnIdentifier("role", true), UTF8Type.instance));

    public ListSuperUsersStatement()
    {
        // nothing to do
    }

    public void validate(ClientState state) throws UnauthorizedException, InvalidRequestException
    {
        state.ensureNotAnonymous();
    }

    public void authorize(ClientState state) throws InvalidRequestException
    {
        // Allow listing superuser privileged users only if the caller has DESCRIBE permission on 'all roles'
        if (!DatabaseDescriptor.getAuthorizer()
                               .authorize(state.getUser(), RoleResource.root())
                               .contains(Permission.DESCRIBE))
        {
            throw new UnauthorizedException("You are not authorized to view superuser details");
        }
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        Set<RoleResource> superUsers = Roles.getAllRoles(Roles::hasSuperuserStatus);
        if (superUsers == null || superUsers.isEmpty())
            return new ResultMessage.Void();

        ResultSet result = new ResultSet(new ResultSet.ResultMetadata(metadata));

        superUsers.stream()
                  .sorted(RoleResource::compareTo)
                  .forEach(role -> result.addColumnValue(UTF8Type.instance.decompose(role.getRoleName())));

        return new ResultMessage.Rows(result);
    }

    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.LIST_SUPERUSERS);
    }
}
