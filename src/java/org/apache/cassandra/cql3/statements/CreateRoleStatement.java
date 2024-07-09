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

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CIDRPermissions;
import org.apache.cassandra.auth.DCPermissions;
import org.apache.cassandra.auth.IRoleManager;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleOptions;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.PasswordObfuscator;
import org.apache.cassandra.cql3.RoleName;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.messages.ResultMessage;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class CreateRoleStatement extends AuthenticationStatement
{
    private final RoleResource role;
    private final RoleOptions opts;
    final DCPermissions dcPermissions;
    final CIDRPermissions cidrPermissions;
    private final boolean ifNotExists;
    private Boolean roleExists;

    public CreateRoleStatement(RoleName name, RoleOptions options, DCPermissions dcPermissions,
                               CIDRPermissions cidrPermissions, boolean ifNotExists)
    {
        this.role = RoleResource.role(name.getName());
        this.opts = options;
        this.dcPermissions = dcPermissions;
        this.cidrPermissions = cidrPermissions;
        this.ifNotExists = ifNotExists;
    }

    public void authorize(ClientState state) throws UnauthorizedException
    {
        super.checkPermission(state, Permission.CREATE, RoleResource.root());
        if (opts.getSuperuser().isPresent())
        {
            if (opts.getSuperuser().get() && !state.getUser().isSuper())
                throw new UnauthorizedException("Only superusers can create a role with superuser status");
        }
    }

    public void validate(ClientState state) throws RequestValidationException
    {
        opts.validate();

        if (role.getRoleName().isEmpty())
            throw new InvalidRequestException("Role name can't be an empty string");

        if (dcPermissions != null)
        {
            dcPermissions.validate();
        }

        if (cidrPermissions != null)
        {
            cidrPermissions.validate();
        }

        // validate login here before authorize to avoid leaking role existence to anonymous users.
        state.ensureNotAnonymous();

        roleExists = DatabaseDescriptor.getRoleManager().isExistingRole(role);

        if (!ifNotExists && roleExists)
            throw new InvalidRequestException(String.format("%s already exists", role.getRoleName()));
    }

    public ResultMessage execute(ClientState state) throws RequestExecutionException, RequestValidationException
    {
        if (roleExists == null)
            roleExists = DatabaseDescriptor.getRoleManager().isExistingRole(role);

        // not rejected in validate()
        if (ifNotExists && roleExists)
            return null;

        if (opts.isGeneratedPassword())
        {
            String generatedPassword = Guardrails.password.generate();
            if (generatedPassword != null)
                opts.setOption(IRoleManager.Option.PASSWORD, generatedPassword);
            else
                throw new InvalidRequestException("You have to enable password_validator and it's generator_class_name property " +
                                                  "in cassandra.yaml to be able to generate passwords.");
        }

        opts.getPassword().ifPresent(password -> Guardrails.password.guard(password, state));

        DatabaseDescriptor.getRoleManager().createRole(state.getUser(), role, opts);
        if (DatabaseDescriptor.getNetworkAuthorizer().requireAuthorization())
        {
            DatabaseDescriptor.getNetworkAuthorizer().setRoleDatacenters(role, dcPermissions);
        }

        if (cidrPermissions != null)
            DatabaseDescriptor.getCIDRAuthorizer().setCidrGroupsForRole(role, cidrPermissions);

        grantPermissionsToCreator(state);

        return getResultMessage(opts);
    }

    /**
     * Grant all applicable permissions on the newly created role to the user performing the request
     * see also: AlterTableStatement#createdResources() and the overridden implementations
     * of it in subclasses CreateKeyspaceStatement & CreateTableStatement.
     * @param state client state
     */
    private void grantPermissionsToCreator(ClientState state)
    {
        // The creator of a Role automatically gets ALTER/DROP/AUTHORIZE/DESCRIBE permissions on it if:
        // * the user is not anonymous
        // * the configured IAuthorizer supports granting of permissions (not all do, AllowAllAuthorizer doesn't and
        //   custom external implementations may not)
        if (!state.getUser().isAnonymous())
        {
            try
            {
                DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER,
                                                         role.applicablePermissions(),
                                                         role,
                                                         RoleResource.role(state.getUser().getName()));
            }
            catch (UnsupportedOperationException e)
            {
                // not a problem, grant is an optional method on IAuthorizer
            }
        }
    }
    
    @Override
    public String toString()
    {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.SHORT_PREFIX_STYLE);
    }
    @Override
    public AuditLogContext getAuditLogContext()
    {
        return new AuditLogContext(AuditLogEntryType.CREATE_ROLE);
    }

    @Override
    public String obfuscatePassword(String query)
    {
        return PasswordObfuscator.obfuscate(query, opts);
    }
}
