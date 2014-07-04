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
public class CassandraRoleAwareAuthorizer extends AbstractCassandraAuthorizer
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraRoleAwareAuthorizer.class);

    private static final String GRANTEE = "grantee";
    private static final String RBAC_PERMISSIONS_CF = "rbac_permissions";
    private static final String RBAC_PERMISSIONS_CF_SCHEMA = String.format("CREATE TABLE %s.%s ("
                                                                      + "grantee text,"
                                                                      + "resource text,"
                                                                      + "permissions set<text>,"
                                                                      + "PRIMARY KEY(grantee, resource)"
                                                                      + ") WITH gc_grace_seconds=%d",
                                                                      Auth.AUTH_KS,
                                                                      RBAC_PERMISSIONS_CF,
                                                                      90 * 24 * 60 * 60); // 3 months.
    private static final ColumnIdentifier RESOURCE_COLUMN = new ColumnIdentifier("resource", false);
    private SelectStatement authorizeStatement;

    /**
     * Returns every permission on the resource granted to the user and granted to
     * any roles that have been granted to the user
     */
    public Set<Permission> authorize(AuthenticatedUser user, IResource resource)
    {
        if (user.isSuper())
            return Permission.ALL;

        Set<Permission> permissions = EnumSet.noneOf(Permission.class);
        try
        {
            IGrantee grantee = Grantee.asUser(user.getName());
            addPermissionsForGrantee(permissions, resource, grantee);
            for (Role role: user.getRoles())
                addPermissionsForGrantee(permissions, resource, role);
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

    private void addPermissionsForGrantee(Set<Permission> permissions, IResource resource, IGrantee grantee)
    throws RequestExecutionException, RequestValidationException
    {
        ResultMessage.Rows rows = authorizeStatement.execute(QueryState.forInternalCalls(),
                                                             QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE,
                                                                                           Lists.newArrayList(ByteBufferUtil.bytes(grantee.getId()),
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
            rows = process(String.format("SELECT grantee FROM %s.%s WHERE resource = '%s'",
                                         Auth.AUTH_KS,
                                         RBAC_PERMISSIONS_CF,
                                         escape(droppedResource.getName())));
        }
        catch (Throwable e)
        {
            logger.warn("CassandraRoleAwareAuthorizer failed to revoke all permissions on {}: {}", droppedResource, e);
            return;
        }

        for (UntypedResultSet.Row row : rows)
        {
            try
            {
                process(String.format("DELETE FROM %s.%s WHERE grantee = '%s' AND resource = '%s'",
                                      Auth.AUTH_KS,
                                      RBAC_PERMISSIONS_CF,
                                      escape(row.getString(GRANTEE)),
                                      escape(droppedResource.getName())));
            }
            catch (Throwable e)
            {
                logger.warn("CassandraRoleAwareAuthorizer failed to revoke all permissions on {}: {}", droppedResource, e);
            }
        }
    }

    public Set<DataResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.columnFamily(Auth.AUTH_KS, RBAC_PERMISSIONS_CF));
    }

    public void setup()
    {
        Auth.setupTable(RBAC_PERMISSIONS_CF, RBAC_PERMISSIONS_CF_SCHEMA);
        Auth.setupIndex(RBAC_PERMISSIONS_CF, RESOURCE_COLUMN);
        try
        {
            String query = String.format("SELECT permissions FROM %s.%s WHERE grantee = ? AND resource = ?", Auth.AUTH_KS, RBAC_PERMISSIONS_CF);
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
        modify(permissions, resource, grantee, "+");
    }

    public void revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, IGrantee grantee)
    throws RequestValidationException, RequestExecutionException
    {
        modify(permissions, resource, grantee, "-");
    }

    // Adds or removes permissions from a permissions table (adds if op is "+", removes if op is "-")
    private void modify(Set<Permission> permissions, IResource resource, IGrantee grantee, String op) throws RequestExecutionException
    {
        process(String.format("UPDATE %s.%s SET permissions = permissions %s {%s} WHERE grantee = '%s' AND resource = '%s'",
                              Auth.AUTH_KS,
                              RBAC_PERMISSIONS_CF,
                              op,
                              "'" + StringUtils.join(permissions, "','") + "'",
                              grantee.getId(),
                              escape(resource.getName())));
    }

    public Set<PermissionDetails> list(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, IGrantee grantee)
    throws RequestValidationException, RequestExecutionException
    {
        if (!performer.isSuper() && (grantee != null) && (grantee.getType() == Type.User) &&
            !performer.getName().equals(grantee.getName()))
            throw new UnauthorizedException(String.format("You are not authorized to view %s permissions",
                                                          grantee == null ? "all roles and users" :
                                                                            grantee.getType() + " - " + grantee.getName() + "'"));

        Set<PermissionDetails> details = new HashSet<PermissionDetails>();
        addPermissionsForGrantee(details, permissions, resource, grantee);
        if (grantee != null)
        {
            for (Role role : performer.getRoles())
            {
                addPermissionsForGrantee(details, permissions, resource, role);
            }
        }

        return details;
    }

    private static void addPermissionsForGrantee(Set<PermissionDetails> details, Set<Permission> permissions, IResource resource, IGrantee grantee)
    throws RequestValidationException, RequestExecutionException
    {
        for (UntypedResultSet.Row row : process(buildListQuery(resource, grantee)))
        {
            if (row.has(PERMISSIONS))
            {
                for (String p : row.getSet(PERMISSIONS, UTF8Type.instance))
                {
                    Permission permission = Permission.valueOf(p);
                    if (permissions.contains(permission))
                        details.add(new PermissionDetails(Grantee.fromId(row.getString(GRANTEE)),
                                                          DataResource.fromName(row.getString(RESOURCE)),
                                                          permission));
                }
            }
        }
    }

    private static String buildListQuery(IResource resource, IGrantee grantee)
    {
        List<String> vars = Lists.newArrayList(Auth.AUTH_KS, RBAC_PERMISSIONS_CF);
        List<String> conditions = new ArrayList<String>();

        if (resource != null)
        {
            conditions.add("resource = '%s'");
            vars.add(escape(resource.getName()));
        }

        if (grantee != null)
        {
            conditions.add("grantee = '%s'");
            vars.add(grantee.getId());
        }

        String query = "SELECT grantee, resource, permissions FROM %s.%s";

        if (!conditions.isEmpty())
            query += " WHERE " + StringUtils.join(conditions, " AND ");

        return String.format(query, vars.toArray());
    }

    public void revokeAll(IGrantee droppedGrantee)
    {
        try
        {
            process(String.format("DELETE FROM %s.%s WHERE grantee = '%s'",
                                  Auth.AUTH_KS,
                                  RBAC_PERMISSIONS_CF,
                                  droppedGrantee.getId()));
        }
        catch (Throwable e)
        {
            logger.warn("CassandraRoleBasedAuthorizer failed to revoke all permissions of {} {}: {}",
                        droppedGrantee.getType(), droppedGrantee.getName(), e);
        }
    }
}
