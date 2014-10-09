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

import java.util.*;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * CassandraAuthorizer is an IAuthorizer implementation that keeps
 * permissions internally in C* - in system_auth.permissions CQL3 table.
 */
public class CassandraAuthorizer implements IAuthorizer
{
    private static final Logger logger = LoggerFactory.getLogger(CassandraAuthorizer.class);

    private static final String USERNAME = "username";
    private static final String RESOURCE = "resource";
    private static final String PERMISSIONS = "permissions";

    private static final String PERMISSIONS_CF = "permissions";
    private static final String PERMISSIONS_CF_SCHEMA = String.format("CREATE TABLE %s.%s ("
                                                                      + "username text,"
                                                                      + "resource text,"
                                                                      + "permissions set<text>,"
                                                                      + "PRIMARY KEY(username, resource)"
                                                                      + ") WITH gc_grace_seconds=%d",
                                                                      Auth.AUTH_KS,
                                                                      PERMISSIONS_CF,
                                                                      90 * 24 * 60 * 60); // 3 months.

    private SelectStatement authorizeStatement;

    // Returns every permission on the resource granted to the user.
    public Set<Permission> authorize(AuthenticatedUser user, IResource resource)
    {
        if (user.isSuper())
            return Permission.ALL;

        UntypedResultSet result;
        try
        {
            ResultMessage.Rows rows = authorizeStatement.execute(QueryState.forInternalCalls(),
                                                                 QueryOptions.forInternalCalls(ConsistencyLevel.LOCAL_ONE,
                                                                                               Lists.newArrayList(ByteBufferUtil.bytes(user.getName()),
                                                                                                                  ByteBufferUtil.bytes(resource.getName()))));
            result = UntypedResultSet.create(rows.result);
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
        catch (RequestExecutionException e)
        {
            logger.warn("CassandraAuthorizer failed to authorize {} for {}", user, resource);
            return Permission.NONE;
        }

        if (result.isEmpty() || !result.one().has(PERMISSIONS))
            return Permission.NONE;

        Set<Permission> permissions = EnumSet.noneOf(Permission.class);
        for (String perm : result.one().getSet(PERMISSIONS, UTF8Type.instance))
            permissions.add(Permission.valueOf(perm));
        return permissions;
    }

    public void grant(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, String to)
    throws RequestExecutionException
    {
        modify(permissions, resource, to, "+");
    }

    public void revoke(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, String from)
    throws RequestExecutionException
    {
        modify(permissions, resource, from, "-");
    }

    // Adds or removes permissions from user's 'permissions' set (adds if op is "+", removes if op is "-")
    private void modify(Set<Permission> permissions, IResource resource, String user, String op) throws RequestExecutionException
    {
        process(String.format("UPDATE %s.%s SET permissions = permissions %s {%s} WHERE username = '%s' AND resource = '%s'",
                              Auth.AUTH_KS,
                              PERMISSIONS_CF,
                              op,
                              "'" + StringUtils.join(permissions, "','") + "'",
                              escape(user),
                              escape(resource.getName())));
    }

    // 'of' can be null - in that case everyone's permissions have been requested. Otherwise only single user's.
    // If the user requesting 'LIST PERMISSIONS' is not a superuser OR his username doesn't match 'of', we
    // throw UnauthorizedException. So only a superuser can view everybody's permissions. Regular users are only
    // allowed to see their own permissions.
    public Set<PermissionDetails> list(AuthenticatedUser performer, Set<Permission> permissions, IResource resource, String of)
    throws RequestValidationException, RequestExecutionException
    {
        if (!performer.isSuper() && !performer.getName().equals(of))
            throw new UnauthorizedException(String.format("You are not authorized to view %s's permissions",
                                                          of == null ? "everyone" : of));

        Set<PermissionDetails> details = new HashSet<PermissionDetails>();

        for (UntypedResultSet.Row row : process(buildListQuery(resource, of)))
        {
            if (row.has(PERMISSIONS))
            {
                for (String p : row.getSet(PERMISSIONS, UTF8Type.instance))
                {
                    Permission permission = Permission.valueOf(p);
                    if (permissions.contains(permission))
                        details.add(new PermissionDetails(row.getString(USERNAME),
                                                          DataResource.fromName(row.getString(RESOURCE)),
                                                          permission));
                }
            }
        }

        return details;
    }

    private static String buildListQuery(IResource resource, String of)
    {
        List<String> vars = Lists.newArrayList(Auth.AUTH_KS, PERMISSIONS_CF);
        List<String> conditions = new ArrayList<String>();

        if (resource != null)
        {
            conditions.add("resource = '%s'");
            vars.add(escape(resource.getName()));
        }

        if (of != null)
        {
            conditions.add("username = '%s'");
            vars.add(escape(of));
        }

        String query = "SELECT username, resource, permissions FROM %s.%s";

        if (!conditions.isEmpty())
            query += " WHERE " + StringUtils.join(conditions, " AND ");

        if (resource != null && of == null)
            query += " ALLOW FILTERING";

        return String.format(query, vars.toArray());
    }

    // Called prior to deleting the user with DROP USER query. Internal hook, so no permission checks are needed here.
    public void revokeAll(String droppedUser)
    {
        try
        {
            process(String.format("DELETE FROM %s.%s WHERE username = '%s'", Auth.AUTH_KS, PERMISSIONS_CF, escape(droppedUser)));
        }
        catch (RequestExecutionException e)
        {
            logger.warn("CassandraAuthorizer failed to revoke all permissions of {}: {}", droppedUser, e);
        }
    }

    // Called after a resource is removed (DROP KEYSPACE, DROP TABLE, etc.).
    public void revokeAll(IResource droppedResource)
    {

        UntypedResultSet rows;
        try
        {
            // TODO: switch to secondary index on 'resource' once https://issues.apache.org/jira/browse/CASSANDRA-5125 is resolved.
            rows = process(String.format("SELECT username FROM %s.%s WHERE resource = '%s' ALLOW FILTERING",
                                         Auth.AUTH_KS,
                                         PERMISSIONS_CF,
                                         escape(droppedResource.getName())));
        }
        catch (RequestExecutionException e)
        {
            logger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", droppedResource, e);
            return;
        }

        for (UntypedResultSet.Row row : rows)
        {
            try
            {
                process(String.format("DELETE FROM %s.%s WHERE username = '%s' AND resource = '%s'",
                                      Auth.AUTH_KS,
                                      PERMISSIONS_CF,
                                      escape(row.getString(USERNAME)),
                                      escape(droppedResource.getName())));
            }
            catch (RequestExecutionException e)
            {
                logger.warn("CassandraAuthorizer failed to revoke all permissions on {}: {}", droppedResource, e);
            }
        }
    }

    public Set<DataResource> protectedResources()
    {
        return ImmutableSet.of(DataResource.columnFamily(Auth.AUTH_KS, PERMISSIONS_CF));
    }

    public void validateConfiguration() throws ConfigurationException
    {
    }

    public void setup()
    {
        Auth.setupTable(PERMISSIONS_CF, PERMISSIONS_CF_SCHEMA);

        try
        {
            String query = String.format("SELECT permissions FROM %s.%s WHERE username = ? AND resource = ?", Auth.AUTH_KS, PERMISSIONS_CF);
            authorizeStatement = (SelectStatement) QueryProcessor.parseStatement(query).prepare().statement;
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
    }

    // We only worry about one character ('). Make sure it's properly escaped.
    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }

    private static UntypedResultSet process(String query) throws RequestExecutionException
    {
        return QueryProcessor.process(query, ConsistencyLevel.ONE);
    }
}
