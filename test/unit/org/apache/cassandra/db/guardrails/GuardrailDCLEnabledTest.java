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

package org.apache.cassandra.db.guardrails;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.exceptions.InvalidQueryException;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.AuthenticationStatement;
import org.apache.cassandra.cql3.statements.AuthorizationStatement;
import org.apache.cassandra.service.QueryState;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GuardrailDCLEnabledTest extends GuardrailTester
{
    private static final String DCL_ERROR_MSG = "DCL statement is not allowed";
    private static final String DCL_TEST_NEW_USER = "dcltest";

    private void setGuardrail(boolean enabled)
    {
        Guardrails.instance.setDCLEnabled(enabled);
    }

    @Before
    public void beforeGuardrailTest() throws Throwable
    {
        super.beforeGuardrailTest();
        useSuperUser();
        // need permission on roles to test dcl queries
        executeNet(format("GRANT ALL ON ALL ROLES TO %s", USERNAME));
        useUser(USERNAME, PASSWORD);
        createTable(KEYSPACE, "CREATE TABLE IF NOT EXISTS %s (k INT, c INT, v TEXT, PRIMARY KEY(k, c))");
    }

    @After
    public void afterTest()
    {
        setGuardrail(true);
        executeNet(dropRole(DCL_TEST_NEW_USER));
        dropTable("DROP TABLE IF EXISTS %s");
    }

    @Test
    public void testCannotCreateRoleWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        shouldFailWithDCLErrorMsg(getCreateRoleCQL(DCL_TEST_NEW_USER));
        // no role is created
        assertEmpty(execute(String.format("SELECT * FROM system_auth.roles WHERE role='%s'",
                                          DCL_TEST_NEW_USER)));

        setGuardrail(true);
        executeNet(getCreateRoleCQL(DCL_TEST_NEW_USER));
        // role is created
        assertRowCount(execute(String.format("SELECT * FROM system_auth.roles WHERE role='%s'",
                                             DCL_TEST_NEW_USER)),
                       1);
    }

    @Test
    public void testCannotDropRoleWhileFeatureDisabled() throws Throwable
    {
        executeNet(getCreateRoleCQL(DCL_TEST_NEW_USER));
        setGuardrail(false);
        shouldFailWithDCLErrorMsg(dropRole(DCL_TEST_NEW_USER));
        // role is not dropped
        assertRowCount(execute(String.format("SELECT * FROM system_auth.roles WHERE role='%s'",
                                             DCL_TEST_NEW_USER)),
                       1);

        setGuardrail(true);
        executeNet(dropRole(DCL_TEST_NEW_USER));
        // role is dropped
        assertEmpty(execute(String.format("SELECT * FROM system_auth.roles WHERE role='%s'",
                                          DCL_TEST_NEW_USER)));
    }

    @Test
    public void testCannotListRoleWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        shouldFailWithDCLErrorMsg("LIST ROLES");
        shouldFailWithDCLErrorMsg(String.format("LIST ALL PERMISSIONS OF %s", USERNAME));
        shouldFailWithDCLErrorMsg("LIST SUPERUSERS");
        shouldFailWithDCLErrorMsg("LIST USERS");

        setGuardrail(true);
        executeNet("LIST ROLES");
        executeNet(String.format("LIST ALL PERMISSIONS OF %s", USERNAME));
        executeNet("LIST SUPERUSERS");
        executeNet("LIST USERS");
    }

    @Test
    public void testCannotGrantPermissionWhileFeatureDisabled() throws Throwable
    {
        executeNet(getCreateRoleCQL(DCL_TEST_NEW_USER));

        setGuardrail(false);
        shouldFailWithDCLErrorMsg(getGrantPermissionCQL(DCL_TEST_NEW_USER, KEYSPACE, currentTable()));
        // DCL_TEST_NEW_USER doesn't get permission
        assertEmpty(execute(String.format("SELECT * FROM system_auth.role_permissions WHERE role='%s' AND resource='data/%s/%s'",
                                          DCL_TEST_NEW_USER, KEYSPACE, currentTable())));

        setGuardrail(true);
        executeNet(getGrantPermissionCQL(DCL_TEST_NEW_USER, KEYSPACE, currentTable()));
        assertRowCount(execute(String.format("SELECT * FROM system_auth.role_permissions WHERE role='%s' AND resource='data/%s/%s'",
                                             DCL_TEST_NEW_USER, KEYSPACE, currentTable())),
                       1);
    }

    @Test
    public void testCannotRevokePermissionWhileFeatureDisabled() throws Throwable
    {
        executeNet(getCreateRoleCQL(DCL_TEST_NEW_USER));
        executeNet(getGrantPermissionCQL(DCL_TEST_NEW_USER, KEYSPACE, currentTable()));

        setGuardrail(false);
        shouldFailWithDCLErrorMsg(getRevokePermissionCQL(DCL_TEST_NEW_USER, KEYSPACE, currentTable()));
        // DCL_TEST_NEW_USER permission wasn't revoked on KEYSPACE
        assertRowCount(execute(String.format("SELECT * FROM system_auth.role_permissions WHERE role='%s' AND resource='data/%s/%s'",
                                             DCL_TEST_NEW_USER, KEYSPACE, currentTable())),
                       1);

        setGuardrail(true);
        executeNet(getRevokePermissionCQL(DCL_TEST_NEW_USER, KEYSPACE, currentTable()));
        assertEmpty(execute(String.format("SELECT * FROM system_auth.role_permissions WHERE role='%s' AND resource='data/%s/%s'",
                                          DCL_TEST_NEW_USER, KEYSPACE, currentTable())));
    }

    @Test
    public void testMockDCLStatementsWhileFeaturesDisabled()
    {
        AuthorizationStatement authorizationStatement = mock(AuthorizationStatement.class);
        AuthenticationStatement authenticationStatement = mock(AuthenticationStatement.class);
        QueryState queryState = mock(QueryState.class);

        doReturn(userClientState).when(queryState).getClientState();
        doCallRealMethod().when(authorizationStatement).isDCLStatement();
        doCallRealMethod().when(authenticationStatement).isDCLStatement();
        QueryProcessor qp = QueryProcessor.instance;

        setGuardrail(false);

        assertThatThrownBy(() -> qp.processStatement(authorizationStatement, queryState, QueryOptions.DEFAULT, 0L))
        .isInstanceOf(GuardrailViolatedException.class);

        verify(authorizationStatement).isDCLStatement();

        assertThatThrownBy(() -> qp.processStatement(authenticationStatement, queryState, QueryOptions.DEFAULT, 0L))
        .isInstanceOf(GuardrailViolatedException.class);

        verify(authenticationStatement).isDCLStatement();

        setGuardrail(true);
        qp.processStatement(authorizationStatement, queryState, QueryOptions.DEFAULT, 0L);
        qp.processStatement(authenticationStatement, queryState, QueryOptions.DEFAULT, 0L);
    }

    private void shouldFailWithDCLErrorMsg(String query)
    {
        assertThatThrownBy(() -> executeNet(query))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining(DCL_ERROR_MSG);
    }

    private static String getCreateRoleCQL(String role)
    {
        return String.format("CREATE ROLE IF NOT EXISTS %s WITH PASSWORD = 'test'",
                             role);
    }

    private static String getGrantPermissionCQL(String role, String ks, String tbl)
    {
        return String.format("GRANT ALL PERMISSIONS ON %s.%s TO %s;", ks, tbl, role);
    }

    private static String getRevokePermissionCQL(String role, String ks, String tbl)
    {
        return String.format("REVOKE ALL ON %s.%s FROM %s;", ks, tbl, role);
    }

    private static String dropRole(String role)
    {
        return String.format("DROP ROLE IF EXISTS %s", role);
    }
}
