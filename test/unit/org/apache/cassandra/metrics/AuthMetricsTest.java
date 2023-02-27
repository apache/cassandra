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

package org.apache.cassandra.metrics;

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;
import static org.apache.cassandra.auth.PasswordAuthenticator.*;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import org.apache.cassandra.auth.PasswordAuthenticator;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.service.EmbeddedCassandraService;

/** AuthMetricsTest class is responsible for covering test cases for AuthMetrics */

@RunWith(Parameterized.class)
public class AuthMetricsTest
{
    private static EmbeddedCassandraService embedded;

    private static final String TEST_USER = "testuser";
    private static final String TEST_PW = "testpassword";
    private static final String TEST_WRONG_USER = "testwronguser";
    private static final String TEST_WRONG_PW = "testwrongpassword";

    private static PasswordAuthenticator authenticator;
    private final boolean authEnabled = DatabaseDescriptor.getAuthenticator().requireAuthentication();
    AuthEnforcementFlagEnum authEnforcementFlag;
    long test_user_success_count, test_user_failure_count, test_wrong_user_success_count, test_wrong_user_failure_count;

    public AuthMetricsTest(AuthEnforcementFlagEnum authEnforcementFlag)
    {
        authenticator.setAuthEnforcementFlag(authEnforcementFlag);
        this.authEnforcementFlag = authEnforcementFlag;
        if (authEnforcementFlag == AuthEnforcementFlagEnum.SOFT) {
            test_user_success_count = 2;
            test_user_failure_count = 2;
            test_wrong_user_success_count = 0;
            test_wrong_user_failure_count = 2;
        } else {
            // authEnforcementFlag = "hard"
            test_user_success_count = 2;
            test_user_failure_count = 1;
            test_wrong_user_success_count = 0;
            test_wrong_user_failure_count = 1;
        }
    }

    @Parameters()
    public static List<AuthEnforcementFlagEnum> generateData()
    {
        return Arrays.asList(AuthEnforcementFlagEnum.HARD, AuthEnforcementFlagEnum.SOFT);
    }

    @BeforeClass
    public static void setup() throws Exception
    {
        OverrideConfigurationLoader.override((config) -> {
            config.authenticator = "PasswordAuthenticator";
            config.role_manager = "CassandraRoleManager";
            config.authorizer = "CassandraAuthorizer";
        });
        CQLTester.prepareServer();

        System.setProperty("cassandra.superuser_setup_delay_ms", "0");
        embedded = new EmbeddedCassandraService();
        embedded.start();
        authenticator = (PasswordAuthenticator) DatabaseDescriptor.getAuthenticator();

        executeWithCredentials(
        Arrays.asList(getCreateRoleCql(TEST_USER, true, false, TEST_PW),
                      "CREATE KEYSPACE testks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}",
                      "CREATE TABLE testks.table1 (key text PRIMARY KEY, col1 int, col2 int)"),
        "cassandra", "cassandra");

    }

    @AfterClass
    public static void shutdown()
    {
        embedded.stop();
    }

    @Test
    public void testAuthMetrics()
    {
        long test_user_success_count_old = AuthMetricsManager.getMetrics(TEST_USER, authEnabled, authEnforcementFlag.name()).userSuccessMetrics.getCount();
        long test_user_failure_count_old = AuthMetricsManager.getMetrics(TEST_USER, authEnabled, authEnforcementFlag.name()).userFailureMetrics.getCount();
        long test_wrong_user_success_count_old = AuthMetricsManager.getMetrics(TEST_WRONG_USER, authEnabled, authEnforcementFlag.name()).userSuccessMetrics.getCount();
        long test_wrong_user_failure_count_old = AuthMetricsManager.getMetrics(TEST_WRONG_USER, authEnabled, authEnforcementFlag.name()).userFailureMetrics.getCount();

        String cql = "SELECT * FROM testks.table1";
        executeWithCredentials(Arrays.asList(cql, cql), TEST_USER, TEST_PW);
        executeWithCredentials(Collections.singletonList(cql), TEST_USER, TEST_WRONG_PW);
        executeWithCredentials(Collections.singletonList(cql), TEST_WRONG_USER, TEST_WRONG_PW);

        long test_user_success_count_new = AuthMetricsManager.getMetrics(TEST_USER, authEnabled, authEnforcementFlag.name()).userSuccessMetrics.getCount();
        long test_user_failure_count_new = AuthMetricsManager.getMetrics(TEST_USER, authEnabled, authEnforcementFlag.name()).userFailureMetrics.getCount();
        long test_wrong_user_success_count_new = AuthMetricsManager.getMetrics(TEST_WRONG_USER, authEnabled, authEnforcementFlag.name()).userSuccessMetrics.getCount();
        long test_wrong_user_failure_count_new = AuthMetricsManager.getMetrics(TEST_WRONG_USER, authEnabled, authEnforcementFlag.name()).userFailureMetrics.getCount();

        /*
         * Whenever a new session is created, Cassandra creates 2 caches (PermissionsCache and RolesCache).
         * User is authenticated during creation of each these 2 caches and hence we expect user authentication to happen
         * 2 times whenever a new session is created.
         * */
        assertEquals(test_user_success_count_new - test_user_success_count_old, test_user_success_count);
        assertEquals(test_user_failure_count_new - test_user_failure_count_old, test_user_failure_count);
        assertEquals(test_wrong_user_success_count_new - test_wrong_user_success_count_old, test_wrong_user_success_count);
        assertEquals(test_wrong_user_failure_count_new - test_wrong_user_failure_count_old, test_wrong_user_failure_count);
    }

    /**
     * Helper methods
     */

    public static void executeWithCredentials(List<String> queries, String username, String password)
    {
        try (Cluster cluster = Cluster.builder().addContactPoints(InetAddress.getLoopbackAddress())
                                      .withoutJMXReporting()
                                      .withCredentials(username, password)
                                      .withPort(DatabaseDescriptor.getNativeTransportPort())
                                      .build()) {
            try (Session session = cluster.connect()) {
                for (String query : queries)
                    session.execute(query);
            } catch (AuthenticationException | UnauthorizedException  e) {
                // noop
            } finally {
                cluster.close();
            }
        }
    }

    private static String getCreateRoleCql(String role, boolean login, boolean superUser, String password)
    {
        return String.format("CREATE ROLE IF NOT EXISTS %s WITH LOGIN = %s AND SUPERUSER = %s AND PASSWORD = '%s'",
                             role, login, superUser, password);
    }
}
