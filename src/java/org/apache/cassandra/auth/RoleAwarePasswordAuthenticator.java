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

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An implementation of the PasswordAuthenticator that adds any roles, that have
 * been granted to the user, to the returned AuthenticatedUser. This should be
 * used with the RoleAwareAuthorizer to enable role based access control
 */
public class RoleAwarePasswordAuthenticator extends RoleUnawarePasswordAuthenticator
{
    private static final Logger logger = LoggerFactory.getLogger(RoleAwarePasswordAuthenticator.class);

    private static final String GRANTS_CF = "grants";

    private static final String GRANTS_CF_SCHEMA = String.format("CREATE TABLE %s.%s ("
                                                                 + "user text,"
                                                                 + "role text,"
                                                                 + "PRIMARY KEY(user, role)"
                                                                 + ") WITH gc_grace_seconds=%d",
                                                                 Auth.AUTH_KS,
                                                                 GRANTS_CF,
                                                                 Auth.THREE_MONTHS);

    private static SelectStatement selectRolesStatement;

    public AuthenticatedUser authenticate(Map<String, String> credentials) throws AuthenticationException
    {
        AuthenticatedUser user = super.authenticate(credentials);
        return new AuthenticatedUser(user.getName(), Auth.getRoles(user.getName()));
    }

    public void create(String username, Map<Option, Object> options) throws InvalidRequestException, RequestExecutionException
    {
        super.create(username, options);
    }

    public void alter(String username, Map<Option, Object> options) throws RequestExecutionException
    {
        super.alter(username, options);
        dropAllRolesForUser(username);
    }

    public void grantRole(String username, String rolename) throws RequestValidationException, RequestExecutionException
    {
        QueryProcessor.process(String.format("INSERT INTO %s.%s (user, role) values ('%s','%s')",
                Auth.AUTH_KS,
                GRANTS_CF,
                escape(username),
                escape(rolename)),
                ConsistencyLevel.QUORUM);
    }

    public void revokeRole(String username, String rolename) throws RequestValidationException, RequestExecutionException
    {
        QueryProcessor.process(String.format("DELETE FROM %s.%s WHERE user = '%s' AND role = '%s'",
                Auth.AUTH_KS,
                GRANTS_CF,
                escape(username),
                escape(rolename)),
                ConsistencyLevel.QUORUM);
    }

    public void revokeAll(String rolename) throws RequestValidationException, RequestExecutionException
    {
        QueryProcessor.process(String.format("DELETE FROM %s.%s WHERE role = '%s' ALLOW FILTERING",
                Auth.AUTH_KS,
                GRANTS_CF,
                escape(rolename)),
                ConsistencyLevel.QUORUM);
    }

    public Set<String> listRoles(String username) throws RequestValidationException, RequestExecutionException
    {
        Set<String> roles = new HashSet<>();

        ResultMessage.Rows rows = selectRolesStatement.execute(QueryState.forInternalCalls(),
                                                               QueryOptions.forInternalCalls(ConsistencyLevel.QUORUM,
                                                                       Lists.newArrayList(ByteBufferUtil.bytes(username))));

        for (UntypedResultSet.Row row: UntypedResultSet.create(rows.result))
            roles.add(row.getString("role"));

        return roles;
    }

    public void setup()
    {
        super.setup();
        Auth.setupTable(GRANTS_CF, GRANTS_CF_SCHEMA);
        try
        {
            String query = String.format("SELECT * FROM %s.%s WHERE name = ?", Auth.AUTH_KS, GRANTS_CF);
            selectRolesStatement = (SelectStatement) QueryProcessor.parseStatement(query).prepare().statement;
        }
        catch (RequestValidationException e)
        {
            throw new AssertionError(e); // not supposed to happen
        }
    }

    private void dropAllRolesForUser(String username) throws RequestExecutionException
    {
        QueryProcessor.process(String.format("DELETE FROM %s.%s WHERE user = '%s'",
                                             Auth.AUTH_KS,
                                             GRANTS_CF,
                                             escape(username)),
                               ConsistencyLevel.QUORUM);
    }

    // we only worry about one character ('). Make sure it's properly escaped.
    private static String escape(String name)
    {
        return StringUtils.replace(name, "'", "''");
    }
}
