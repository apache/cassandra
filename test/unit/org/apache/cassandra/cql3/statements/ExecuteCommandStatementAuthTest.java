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

import java.lang.reflect.Field;
import java.util.Collections;

import com.google.common.collect.ImmutableSet;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CommandResource;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.StubAuthorizer;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.service.ClientState;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class ExecuteCommandStatementAuthTest extends CQLTester
{
    private static final String ROLE_NAME = "test_command_auth_role";

    private ClientState clientState;
    private RoleResource role;

    @BeforeClass
    public static void setupAuthorizer() throws Exception
    {
        IAuthorizer authorizer = new StubAuthorizer();
        Field authorizerField = DatabaseDescriptor.class.getDeclaredField("authorizer");
        authorizerField.setAccessible(true);
        authorizerField.set(null, authorizer);
        DatabaseDescriptor.setPermissionsValidity(0);
    }

    @Before
    public void setup() throws Exception
    {
        ((StubAuthorizer) DatabaseDescriptor.getAuthorizer()).clear();

        role = RoleResource.role(ROLE_NAME);
        AuthenticatedUser user = new AuthenticatedUser(ROLE_NAME);
        clientState = ClientState.forInternalCalls();
        Field userField = ClientState.class.getDeclaredField("user");
        userField.setAccessible(true);
        userField.set(clientState, user);
    }

    @Test
    public void deniesCommandExecutionForOrdinaryRoleWithNoGrant()
    {
        ExecuteCommandStatement.Raw statement = new ExecuteCommandStatement.Raw("version", Collections.emptyMap());

        assertThatThrownBy(() -> statement.authorize(clientState))
            .isInstanceOf(UnauthorizedException.class)
            .hasMessageContaining("EXECUTE");
    }

    @Test
    public void allowsCommandExecutionForRoleGrantedExecuteOnCommandResource()
    {
        DatabaseDescriptor.getAuthorizer().grant(AuthenticatedUser.SYSTEM_USER,
                                                 ImmutableSet.of(Permission.EXECUTE),
                                                 CommandResource.command("version"),
                                                 role);

        ExecuteCommandStatement.Raw statement = new ExecuteCommandStatement.Raw("version", Collections.emptyMap());
        statement.authorize(clientState);
    }
}
