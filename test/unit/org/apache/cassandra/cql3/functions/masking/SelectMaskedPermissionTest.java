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

package org.apache.cassandra.cql3.functions.masking;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.awaitility.core.ThrowingRunnable;

import static java.lang.String.format;

/**
 * Tests the {@link org.apache.cassandra.auth.Permission#SELECT_MASKED} permission.
 */
public class SelectMaskedPermissionTest extends CQLTester
{
    private static final Object[] CLEAR_ROW = row(7, 7, 7, 7);

    private static final String USER = "ddm_user"; // user that will have their permissions changed
    private static final String PASSWORD = "ddm_password";

    @BeforeClass
    public static void beforeClass()
    {
        DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
        DatabaseDescriptor.setPermissionsValidity(0);
        DatabaseDescriptor.setRolesValidity(0);
        CQLTester.setUpClass();
        requireAuthentication();
        requireNetwork();
    }

    @Before
    public void before() throws Throwable
    {
        useSuperUser();

        createTable("CREATE TABLE %s (k int, c int, s int static, v int, PRIMARY KEY (k, c))");
        executeNet("INSERT INTO %s(k, c, s, v) VALUES (?, ?, ?, ?)", CLEAR_ROW);

        executeNet(format("CREATE USER IF NOT EXISTS %s WITH PASSWORD '%s'", USER, PASSWORD));
        executeNet(format("GRANT SELECT ON ALL KEYSPACES TO %s", USER));
    }

    @After
    public void after() throws Throwable
    {
        useSuperUser();
        executeNet("DROP USER IF EXISTS " + USER);
        alterTable("ALTER TABLE %s ALTER k DROP MASKED");
        alterTable("ALTER TABLE %s ALTER c DROP MASKED");
        alterTable("ALTER TABLE %s ALTER s DROP MASKED");
        alterTable("ALTER TABLE %s ALTER v DROP MASKED");
    }

    @Test
    public void testPartitionKeyColumn() throws Throwable
    {
        alterTable("ALTER TABLE %s ALTER k MASKED WITH DEFAULT");
        Object[] maskedRow = row(0, 7, 7, 7);

        // test queries with default permissions (no UNMASK nor SELECT_MASKED)
        testPartitionKeyColumnWithDefaultPermissions(maskedRow);

        // test queries with only SELECT_MASKED permission
        executeNet(format("GRANT SELECT_MASKED ON ALL KEYSPACES TO %s", USER));
        testPartitionKeyColumnWithOnlySelectMasked(maskedRow);

        // test queries with only UNMASK permission (which includes SELECT_MASKED)
        executeNet(format("REVOKE SELECT_MASKED ON ALL KEYSPACES FROM %s", USER));
        executeNet(format("GRANT UNMASK ON ALL KEYSPACES TO %s", USER));
        testPartitionKeyColumnWithUnmask();

        // test queries with both UNMASK and SELECT_MASKED permissions
        executeNet(format("GRANT UNMASK, SELECT_MASKED ON ALL KEYSPACES TO %s", USER));
        testPartitionKeyColumnWithUnmask();

        // test queries again without both UNMASK and SELECT_MASKED permissions
        executeNet(format("REVOKE UNMASK, SELECT_MASKED ON ALL KEYSPACES FROM %s", USER));
        testPartitionKeyColumnWithDefaultPermissions(maskedRow);
    }

    private void testPartitionKeyColumnWithDefaultPermissions(Object[] maskedRow) throws Throwable
    {
        assertWithUser(() -> {
            assertAuthorizedQuery("SELECT * FROM %s", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE s = 7 ALLOW FILTERING", maskedRow);
            assertUnauthorizedQuery("SELECT * FROM %s WHERE k = 7", "[k]");
            assertUnauthorizedQuery("SELECT * FROM %s WHERE k >= 7 ALLOW FILTERING", "[k]");
            assertUnauthorizedQuery("SELECT * FROM %s WHERE k = 7 AND c = 7", "[k]");
            assertUnauthorizedQuery("SELECT * FROM %s WHERE k = 7 AND v = 7 ALLOW FILTERING", "[k]");
            assertUnauthorizedQuery("SELECT * FROM %s WHERE token(k) = token(7)", "[k]");
            assertUnauthorizedQuery("SELECT * FROM %s WHERE token(k) >= token(7)", "[k]");
        });
    }

    private void testPartitionKeyColumnWithOnlySelectMasked(Object[] maskedRow) throws Throwable
    {
        assertWithUser(() -> {
            assertAuthorizedQuery("SELECT * FROM %s", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE s = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k >= 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7 AND c = 7", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7 AND v = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) = token(7)", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) >= token(7)", maskedRow);
        });
    }

    private void testPartitionKeyColumnWithUnmask() throws Throwable
    {
        assertWithUser(() -> {
            assertAuthorizedQuery("SELECT * FROM %s", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE s = 7 ALLOW FILTERING", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k >= 7 ALLOW FILTERING", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7 AND c = 7", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7 AND v = 7 ALLOW FILTERING", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) = token(7)", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) >= token(7)", CLEAR_ROW);
        });
    }

    @Test
    public void testClusteringKeyColumn() throws Throwable
    {
        alterTable("ALTER TABLE %s ALTER c MASKED WITH DEFAULT");
        Object[] maskedRow = row(7, 0, 7, 7);

        // test queries with default permissions (no UNMASK nor SELECT_MASKED)
        testClusteringKeyColumnWithDefaultPermissions(maskedRow);

        // test queries with only SELECT_MASKED permission
        executeNet(format("GRANT SELECT_MASKED ON ALL KEYSPACES TO %s", USER));
        testClusteringKeyColumnWithOnlySelectMasked(maskedRow);

        // test queries with only UNMASK permission (which includes SELECT_MASKED)
        executeNet(format("REVOKE SELECT_MASKED ON ALL KEYSPACES FROM %s", USER));
        executeNet(format("GRANT UNMASK ON ALL KEYSPACES TO %s", USER));
        testClusteringKeyColumnWithUnmask();

        // test queries with both UNMASK and SELECT_MASKED permissions
        executeNet(format("GRANT UNMASK, SELECT_MASKED ON ALL KEYSPACES TO %s", USER));
        testClusteringKeyColumnWithUnmask();

        // test queries again without both UNMASK and SELECT_MASKED permissions
        executeNet(format("REVOKE UNMASK, SELECT_MASKED ON ALL KEYSPACES FROM %s", USER));
        testClusteringKeyColumnWithDefaultPermissions(maskedRow);
    }

    private void testClusteringKeyColumnWithDefaultPermissions(Object[] maskedRow) throws Throwable
    {
        assertWithUser(() -> {
            assertAuthorizedQuery("SELECT * FROM %s", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE s = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) = token(7)", maskedRow);
            assertUnauthorizedQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", "[c]");
            assertUnauthorizedQuery("SELECT * FROM %s WHERE k = 7 AND c = 7", "[c]");
        });
    }

    private void testClusteringKeyColumnWithOnlySelectMasked(Object[] maskedRow) throws Throwable
    {
        assertWithUser(() -> {
            assertAuthorizedQuery("SELECT * FROM %s", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE s = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) = token(7)", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7 AND c = 7", maskedRow);
        });
    }

    private void testClusteringKeyColumnWithUnmask() throws Throwable
    {
        assertWithUser(() -> {
            assertAuthorizedQuery("SELECT * FROM %s", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE s = 7 ALLOW FILTERING", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) = token(7)", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7 AND c = 7", CLEAR_ROW);
        });
    }

    @Test
    public void testStaticColumn() throws Throwable
    {
        alterTable("ALTER TABLE %s ALTER s MASKED WITH DEFAULT");
        Object[] maskedRow = row(7, 7, 0, 7);

        // test queries with default permissions (no UNMASK nor SELECT_MASKED)
        testStaticColumnWithDefaultPermissions(maskedRow);

        // test queries with only SELECT_MASKED permission
        executeNet(format("GRANT SELECT_MASKED ON ALL KEYSPACES TO %s", USER));
        testStaticColumnWithOnlySelectMasked(maskedRow);

        // test queries with only UNMASK permission (which includes SELECT_MASKED)
        executeNet(format("REVOKE SELECT_MASKED ON ALL KEYSPACES FROM %s", USER));
        executeNet(format("GRANT UNMASK ON ALL KEYSPACES TO %s", USER));
        testStaticColumnWithUnmask();

        // test queries with both UNMASK and SELECT_MASKED permissions
        executeNet(format("GRANT UNMASK, SELECT_MASKED ON ALL KEYSPACES TO %s", USER));
        testStaticColumnWithUnmask();

        // test queries again without both UNMASK and SELECT_MASKED permissions
        executeNet(format("REVOKE UNMASK, SELECT_MASKED ON ALL KEYSPACES FROM %s", USER));
        testStaticColumnWithDefaultPermissions(maskedRow);
    }

    private void testStaticColumnWithDefaultPermissions(Object[] maskedRow) throws Throwable
    {
        assertWithUser(() -> {
            assertAuthorizedQuery("SELECT * FROM %s", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) = token(7)", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7 AND c = 7", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING", maskedRow);
            assertUnauthorizedQuery("SELECT * FROM %s WHERE s = 7 ALLOW FILTERING", "[s]");
        });
    }

    private void testStaticColumnWithOnlySelectMasked(Object[] maskedRow) throws Throwable
    {
        assertWithUser(() -> {
            assertAuthorizedQuery("SELECT * FROM %s", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) = token(7)", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7 AND c = 7", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE s = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING", maskedRow);
        });
    }

    private void testStaticColumnWithUnmask() throws Throwable
    {
        assertWithUser(() -> {
            assertAuthorizedQuery("SELECT * FROM %s", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) = token(7)", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7 AND c = 7", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE s = 7 ALLOW FILTERING", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING", CLEAR_ROW);
        });
    }

    @Test
    public void testCollections() throws Throwable
    {
        alterTable("ALTER TABLE %s ALTER v MASKED WITH DEFAULT");
        Object[] maskedRow = row(7, 7, 7, 0);

        // test queries with default permissions (no UNMASK nor SELECT_MASKED)
        testCollectionsWithDefaultPermissions(maskedRow);

        // test queries with only SELECT_MASKED permission
        executeNet(format("GRANT SELECT_MASKED ON ALL KEYSPACES TO %s", USER));
        testCollectionsWithOnlySelectMasked(maskedRow);

        // test queries with only UNMASK permission (which includes SELECT_MASKED)
        executeNet(format("REVOKE SELECT_MASKED ON ALL KEYSPACES FROM %s", USER));
        executeNet(format("GRANT UNMASK ON ALL KEYSPACES TO %s", USER));
        testCollectionsWithUnmask();

        // test queries with both UNMASK and SELECT_MASKED permissions
        executeNet(format("GRANT UNMASK, SELECT_MASKED ON ALL KEYSPACES TO %s", USER));
        testCollectionsWithUnmask();

        // test queries again without both UNMASK and SELECT_MASKED permissions
        executeNet(format("REVOKE UNMASK, SELECT_MASKED ON ALL KEYSPACES FROM %s", USER));
        testCollectionsWithDefaultPermissions(maskedRow);
    }

    private void testCollectionsWithDefaultPermissions(Object[] maskedRow) throws Throwable
    {
        assertWithUser(() -> {
            assertAuthorizedQuery("SELECT * FROM %s", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) = token(7)", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7 AND c = 7", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE s = 7 ALLOW FILTERING", maskedRow);
            assertUnauthorizedQuery("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING", "[v]");
        });
    }

    private void testCollectionsWithOnlySelectMasked(Object[] maskedRow) throws Throwable
    {
        assertWithUser(() -> {
            assertAuthorizedQuery("SELECT * FROM %s", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) = token(7)", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7 AND c = 7", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE s = 7 ALLOW FILTERING", maskedRow);
            assertAuthorizedQuery("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING", maskedRow);
        });
    }

    private void testCollectionsWithUnmask() throws Throwable
    {
        assertWithUser(() -> {
            assertAuthorizedQuery("SELECT * FROM %s", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE token(k) = token(7)", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE k = 7 AND c = 7", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE c = 7 ALLOW FILTERING", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE s = 7 ALLOW FILTERING", CLEAR_ROW);
            assertAuthorizedQuery("SELECT * FROM %s WHERE v = 7 ALLOW FILTERING", CLEAR_ROW);
        });
    }

    private void assertAuthorizedQuery(String query, Object[]... rows) throws Throwable
    {
        assertRowsNet(executeNet(query), rows);
    }

    private void assertUnauthorizedQuery(String query, String unauthorizedColumns) throws Throwable
    {
        assertInvalidMessageNet(format("User %s has no UNMASK nor SELECT_MASKED permission on table %s.%s, " +
                                       "cannot query masked columns %s",
                                       USER, KEYSPACE, currentTable(), unauthorizedColumns), query);
    }

    private void assertWithUser(ThrowingRunnable assertion) throws Throwable
    {
        useUser(USER, PASSWORD);
        assertion.run();
        useSuperUser();
    }
}
