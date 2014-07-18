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
package org.apache.cassandra.auth;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.auth.IGrantee.Type;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

/**
 * An implementation of the IAuthorizer interface that supports role based access control.
 * It keeps permissions internally in C* in the system_auth.rbac_permissions CQL3 table.
 *
 * In this implementation permissions are granted to either roles or users. When authorization is
 * sought for a resource, the user is checked for any granted permissions and all the roles granted
 * to the user are checked for any granted permissions as well.
 */
public class RoleAwareAuthorizer extends RoleUnawareAuthorizer
{
    private static final Logger logger = LoggerFactory.getLogger(RoleAwareAuthorizer.class);

    private static final String ROLE_PERMISSIONS_CF = "role_permissions";
    private static final String ROLE_PERMISSIONS_CF_SCHEMA = String.format("CREATE TABLE %s.%s ("
                                                                           + "role text,"
                                                                           + "resource text,"
                                                                           + "permissions set<text>,"
                                                                           + "PRIMARY KEY(role, resource)"
                                                                           + ") WITH gc_grace_seconds=%d",
                                                                           Auth.AUTH_KS,
                                                                           ROLE_PERMISSIONS_CF,
                                                                           Auth.THREE_MONTHS);
    private SelectStatement authorizeStatement;

    /**
     * Returns every permission on the resource granted to the user and granted to
     * any roles that have been granted to the user
     */
    public Set<Permission> authorize(AuthenticatedUser user, IResource resource)
    {
        if (user.isSuper())
            return Permission.ALL;

        Set<Permission> permissions = super.authorize(user, resource);

        try
        {
            for (String rolename: user.getRoles())
                addPermissionsForRole(permissions, resource, rolename);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            return Permission.NONE;
        }

        if (permissions.isEmpty())
            return Permission.NONE;

        return permissions;
    }

    private void addPermissionsForRole(Set<Permission> permissions, IResource resource, String rolename)
    throws RequestExecutionException, RequestValidationException
    {
        ResultMessage.Rows rows = authorizeStatement.execute(QueryState.forInternalCalls(),
                                                             QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE,
                                                                                           Lists.newArrayList(ByteBufferUtil.bytes(rolename),
                                                                                                   ByteBufferUtil.bytes(resource.getName()))));
        UntypedResultSet result = UntypedResultSet.create(rows.result);

        if (result.isEmpty() || !result.one().has(PERMISSIONS))
            return;

        for (String perm : result.one().getSet(PERMISSIONS, UTF8Type.instance))
        {
            permissions.add(Permission.valueOf(perm));
        }
    }

    // Called after a resource is removed (DROP KEYSPACE, DROP TABLE, etc.).
    public void revokeAll(IResource droppedResource)
    {
        UntypedResultSet rows;
        try
        {
            rows = process(String.format("SELECT role FROM %s.%s WHERE resource = '%s' ALLOW FILTERING",
                                         Auth.AUTH_KS,
                                         ROLE_PERMISSIONS_CF,
                                         escape(droppedResource.getName())));
        }
        catch (Throwable e)
        {
            logger.warn("RoleAwareAuthorizer failed to revoke all permissions on {}: {}", droppedResource, e);
            return;
        }

        for (UntypedResultSet.Row row : rows)
        {
            try
            {
                process(String.format("DELETE FROM %s.%s WHERE role = '%s' AND resource = '%s'",
                                      Auth.AUTH_KS,
                                      ROLE_PERMISSIONS_CF,
                                      escape(row.getString("role")),
                                      escape(droppedResource.getName())));
            }
            catch (Throwable e)
            {
                logger.warn("RoleAwareAuthorizer failed to revoke all permissions on {}: {}", droppedResource, e);
            }
        }
    }

    public Set<DataResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.columnFamily(Auth.AUTH_KS, ROLE_PERMISSIONS_CF));
    }

    public void setup()
    {
        Auth.setupTable(ROLE_PERMISSIONS_CF, ROLE_PERMISSIONS_CF_SCHEMA);
        try
        {
            String query = String.format("SELECT permissions FROM %s.%s WHERE role = ? AND resource = ?", Auth.AUTH_KS, ROLE_PERMISSIONS_CF);
            authorizeStatement = (SelectStatement) QueryProcessor.parseStatement(query).prepare().statement;
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
    }

    public void grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, IGrantee grantee)
    throws RequestValidationException, RequestExecutionException
    {
        if (grantee.getType() == Type.User)
            super.grant(performer, permissions, resource, grantee.getName());
        else
            modify(permissions, resource, grantee.getName(), "+");
    }

    public void revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, IGrantee grantee)
    throws RequestValidationException, RequestExecutionException
    {
        if (grantee.getType() == Type.User)
            super.revoke(performer, permissions, resource, grantee.getName());
        else
            modify(permissions, resource, grantee.getName(), "-");
    }

    // Adds or removes permissions from a permissions table (adds if op is "+", removes if op is "-")
    private void modify(Set<Permission> permissions, IResource resource, String rolename, String op) throws RequestExecutionException
    {
        process(String.format("UPDATE %s.%s SET permissions = permissions %s {%s} WHERE role = '%s' AND resource = '%s'",
                Auth.AUTH_KS,
                ROLE_PERMISSIONS_CF,
                op,
                "'" + StringUtils.join(permissions, "','") + "'",
                escape(rolename),
                escape(resource.getName())));
    }

    public Set<PermissionDetails> list(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, IGrantee grantee)
    throws RequestValidationException, RequestExecutionException
    {
        if (!performer.isSuper() && (grantee != null) && ((grantee.getType() == Type.Role) ||
            ((grantee.getType() == Type.User) && !performer.getName().equals(grantee.getName()))))
            throw new UnauthorizedException(String.format("You are not authorized to view %s permissions",
                                                          grantee == null ? "all roles and users" :
                                                          grantee.getType() + " - " + grantee.getName() + "'"));

        Set<PermissionDetails> details;
        if (grantee == null)
        {
            details = super.list(performer, permissions, resource, (String)null);
            for (String rolename : performer.getRoles())
            {
                addPermissionDetailsForRole(details, permissions, resource, rolename);
            }
        }
        else if (grantee.getType() == Type.User)
        {
            details = super.list(performer, permissions, resource, grantee.getName());
            for (String rolename : Auth.getRoles(grantee.getName()))
            {
                addPermissionDetailsForRole(details, permissions, resource, rolename);
            }
        }
        else
        {
            details = new HashSet<>();
            addPermissionDetailsForRole(details, permissions, resource, grantee.getName());
        }

        return details;
    }

    private static void addPermissionDetailsForRole(Set<PermissionDetails> details, Set<Permission> permissions, IResource resource, String rolename)
    throws RequestValidationException, RequestExecutionException
    {
        for (UntypedResultSet.Row row : process(buildListQuery(resource, rolename)))
        {
            if (row.has(PERMISSIONS))
            {
                for (String p : row.getSet(PERMISSIONS, UTF8Type.instance))
                {
                    Permission permission = Permission.valueOf(p);
                    if (permissions.contains(permission))
                        details.add(new PermissionDetails(Grantee.asRole(row.getString("role")),
                                                          DataResource.fromName(row.getString(RESOURCE)),
                                                          permission));
                }
            }
        }
    }

    private static String buildListQuery(IResource resource, String rolename)
    {
        List<String> vars = Lists.newArrayList(Auth.AUTH_KS, ROLE_PERMISSIONS_CF);
        List<String> conditions = new ArrayList<String>();

        if (resource != null)
        {
            conditions.add("resource = '%s'");
            vars.add(escape(resource.getName()));
        }

        if (rolename != null)
        {
            conditions.add("role = '%s'");
            vars.add(escape(rolename));
        }

        String query = "SELECT grantee, resource, permissions FROM %s.%s";

        if (!conditions.isEmpty())
            query += " WHERE " + StringUtils.join(conditions, " AND ");

        if (resource != null && rolename == null)
            query += " ALLOW FILTERING";

        return String.format(query, vars.toArray());
    }

    public void revokeAll(IGrantee droppedGrantee)
    {
        if (droppedGrantee.getType() == Type.User)
            super.revokeAll(droppedGrantee.getName());
        else
        {
            try
            {
                process(String.format("DELETE FROM %s.%s WHERE role = '%s'",
                                      Auth.AUTH_KS,
                                      ROLE_PERMISSIONS_CF,
                                      escape(droppedGrantee.getName())));
            }
            catch (Throwable e)
            {
                logger.warn("RoleAwareAuthorizer failed to revoke all permissions of {} {}: {}",
                            droppedGrantee.getType(), droppedGrantee.getName(), e);
            }
        }
    }
}
