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

import java.util.Set;
import java.util.stream.Collectors;

import org.apache.cassandra.audit.AuditLogContext;
import org.apache.cassandra.audit.AuditLogEntryType;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class RevokePermissionsStatement extends PermissionsManagementStatement
{
    public RevokePermissionsStatement(Set<Permission> permissions, IResource resource, RoleName grantee)
    {
        super(permissions, resource, grantee);
    }

    public ResultMessage execute(ClientState state) throws RequestValidationException, RequestExecutionException
    {
        IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
        Set<Permission> revoked = authorizer.revoke(state.getUser(), permissions, resource, grantee);

        // We want to warn the client if all the specified permissions have not been revoked and the client did
        // not specify ALL in the query.
        if (!revoked.equals(permissions) && !permissions.equals(Permission.ALL))
        {
            String permissionsStr = permissions.stream()
                                               .filter(permission -> !revoked.contains(permission))
                                               .sorted(Permission::compareTo) // guarantee the order for testing
                                               .map(Permission::name)
                                               .collect(Collectors.joining(", "));

            ClientWarn.instance.warn(String.format("Role '%s' was not granted %s on %s",
                                                   grantee.getRoleName(),
                                                   permissionsStr,
                                                   resource));
        }

        return null;
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }

    @Override
    public AuditLogContext getAuditLogContext()
    {
        String keyspace = resource.hasParent() ? resource.getParent().getName() : resource.getName();
        return new AuditLogContext(AuditLogEntryType.REVOKE, keyspace, resource.getName());
    }
}
