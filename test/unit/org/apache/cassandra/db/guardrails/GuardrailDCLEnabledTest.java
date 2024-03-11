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

import java.net.InetSocketAddress;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.service.ClientState;

import static java.lang.String.format;

public class GuardrailDCLEnabledTest extends GuardrailTester
{
    private static final String TEST_USER = "testuser";
    private static final String TEST_PW = "testpassword";
    private static final String TEST_USER1 = "testuser1";
    private static final String TEST_PW1 = "testpassword1";
    private static final String TEST_KS = "dclks";
    private static final String TEST_TABLE = "dcltbl";
    private static final String DCL_ERROR_MSG = "DCL statement is not allowed";
    private ClientState loginUserClientState;
    private void setGuardrail(boolean enabled)
    {
        Guardrails.instance.setDCLEnabled(enabled);
    }

    @Before
    public void beforeGuardrailTest() throws Throwable
    {
        super.beforeGuardrailTest();
        // create user in login state
        useSuperUser();
        executeNet(getCreateRoleCQL(TEST_USER, true, false, TEST_PW));
        executeNet(format("GRANT ALL ON KEYSPACE %s TO %s", KEYSPACE, TEST_USER));
        useUser(TEST_USER, TEST_PW);

        loginUserClientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 1234));
        loginUserClientState.login(new AuthenticatedUser(TEST_USER));
        execute(loginUserClientState, "USE " + keyspace());

        execute(superClientState, String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", TEST_KS));
        execute(superClientState, String.format("CREATE TABLE IF NOT EXISTS %s.%s (key text PRIMARY KEY, col1 int, col2 int)", TEST_KS, TEST_TABLE));
    }

    @After
    public void afterTest()
    {
        setGuardrail(true);
    }

    @Test
    public void testCannotCreateRoleWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        assertFails(() -> execute(loginUserClientState,
                                  getCreateRoleCQL(TEST_USER1, true, false, TEST_PW1)),
                                  DCL_ERROR_MSG);
        // no role is created
        assertEmpty(execute(String.format("SELECT * FROM system_auth.roles WHERE role='%s'", TEST_USER1)));
    }

    @Test
    public void testCannotGrantPermissionWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        assertFails(() -> execute(loginUserClientState,
                                  getGrantPermissionCQL(TEST_USER, TEST_KS, TEST_TABLE)),
                                  DCL_ERROR_MSG);
        // TEST_USER don't get permission on TEST_KS.TEST_TABLE
        assertEmpty(execute(String.format("SELECT * FROM system_auth.role_permissions WHERE role='%s' AND resource='data/%s/%s'",
                                          TEST_USER, TEST_KS, TEST_TABLE)));
    }

    @Test
    public void testCannotRevokePermissionWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        assertFails(() -> execute(loginUserClientState,
                                  String.format("REVOKE ALL ON KEYSPACE %s FROM %s", KEYSPACE, TEST_USER)),
                                  DCL_ERROR_MSG);
        // TEST_USER permission wasn't revoked on KEYSPACE
        assertRowCount(execute(String.format("SELECT * FROM system_auth.role_permissions WHERE role='%s' AND resource='data/%s'", TEST_USER, KEYSPACE)),
                       1);
    }

    private static String getCreateRoleCQL(String role, boolean login, boolean superUser, String password)
    {
        return String.format("CREATE ROLE IF NOT EXISTS %s WITH LOGIN = %s AND SUPERUSER = %s AND PASSWORD = '%s'",
                             role, login, superUser, password);
    }

    private static String getGrantPermissionCQL(String role, String ks, String tbl) {
        return String.format("GRANT ALL PERMISSIONS ON %s.%s TO %s;", ks, tbl, role);
    }
}
