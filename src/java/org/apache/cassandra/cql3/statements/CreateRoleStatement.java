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

import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.security.Permissions;

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

public class CreateRoleStatement extends AuthenticationStatement
{
    private final RoleResource role;
    private final RoleOptions opts;
    final DCPermissions dcPermissions;
    final CIDRPermissions cidrPermissions;
    private final boolean ifNotExists;

    public CreateRoleStatement(RoleName name, RoleOptions options, DCPermissions dcPermissions,
                               CIDRPermissions cidrPermissions, boolean ifNotExists)
    {
        if (options.isGeneratedName())
            this.role = RoleResource.GENERATED_ROLE;
        else
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

        if (!ifNotExists && role != RoleResource.GENERATED_ROLE && DatabaseDescriptor.getRoleManager().isExistingRole(role))
            throw new InvalidRequestException(String.format("%s already exists", role.getRoleName()));
    }

    public ResultMessage execute(ClientState state) throws RequestExecutionException, RequestValidationException
    {
        // not rejected in validate()
        if (ifNotExists && role != RoleResource.GENERATED_ROLE && DatabaseDescriptor.getRoleManager().isExistingRole(role))
            return null;

        RoleResource roleResource;
        if (opts.isGeneratedName())
        {
            Map<String, Object> options = (Map<String, Object>) opts.getOptions().get(IRoleManager.Option.OPTIONS);
            String generatedName = Guardrails.roleNamePolicy.generate(state, options);
            if (generatedName != null)
                roleResource = RoleResource.role(generatedName);
            else
                throw new InvalidRequestException("You have to enable role_name_policy and its generator_class_name property " +
                                                  "in cassandra.yaml to be able to generate role names.");
        }
        else
        {
            roleResource = role;
        }

        if (opts.isGeneratedPassword())
        {
            String generatedPassword = Guardrails.passwordPolicy.generate(state);
            if (generatedPassword != null)
                opts.setOption(IRoleManager.Option.PASSWORD, generatedPassword);
            else
                throw new InvalidRequestException("You have to enable password_policy and its generator_class_name property " +
                                                  "in cassandra.yaml to be able to generate passwords.");
        }

        opts.getPassword().ifPresent(password -> Guardrails.passwordPolicy.validate(password, state));
        Guardrails.roleNamePolicy.validate(roleResource.getRoleName(), state);

        ResultMessage resultMessage = DatabaseDescriptor.getRoleManager().createRoleWithResult(state.getUser(), roleResource, opts);

        if (DatabaseDescriptor.getNetworkAuthorizer().requireAuthorization())
        {
            DatabaseDescriptor.getNetworkAuthorizer().setRoleDatacenters(roleResource, dcPermissions);
        }

        if (cidrPermissions != null)
            DatabaseDescriptor.getCIDRAuthorizer().setCidrGroupsForRole(roleResource, cidrPermissions);

        grantPermissionsToCreator(roleResource, state);

        return resultMessage;
    }

    /**
     * Grant all applicable permissions on the newly created role to the user performing the request
     * see also: AlterTableStatement#createdResources() and the overridden implementations
     * of it in subclasses CreateKeyspaceStatement & CreateTableStatement.
     *
     * @param roleResource role resource to grant permissions on
     * @param state        client state
     */
    private void grantPermissionsToCreator(RoleResource roleResource, ClientState state)
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
                                                         roleResource.applicablePermissions(),
                                                         roleResource,
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
