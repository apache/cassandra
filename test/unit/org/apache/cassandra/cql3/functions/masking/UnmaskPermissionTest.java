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

import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;

import static java.lang.String.format;

/**
 * Tests the {@link org.apache.cassandra.auth.Permission#UNMASK} permission.
 * <p>
 * The permission is tested for a regular user with the {@code UNMASK} permissions on different resources,
 * while also verifying the absence of side effects on other ordinary users, superusers and internal queries.
 */
public class UnmaskPermissionTest extends CQLTester
{
    private static final String CREATE_KEYSPACE = "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = " +
                                                  "{'class': 'SimpleStrategy', 'replication_factor': '1'}";
    private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS %s.%s " +
                                               "(k int, c int, v text MASKED WITH mask_replace('redacted'), " +
                                               "PRIMARY KEY (k, c))";
    private static final String INSERT = "INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?)";
    private static final String SELECT_WILDCARD = "SELECT * FROM %s.%s";
    private static final String SELECT_COLUMNS = "SELECT k, c, v FROM %s.%s";

    private static final Object[] CLEAR_ROW = row(0, 0, "sensitive");
    private static final Object[] MASKED_ROW = row(0, 0, "redacted");

    private static final String KEYSPACE_1 = "mask_keyspace_1";
    private static final String KEYSPACE_2 = "mask_keyspace_2";
    private static final String TABLE_1 = "mask_table_1";
    private static final String TABLE_2 = "mask_table_2";

    private static final String USER = "ddm_user"; // user that will have their permissions changed
    private static final String OTHER_USER = "ddm_ordinary_user"; // user that won't have their permissions altered
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

        schemaChange(format(CREATE_KEYSPACE, KEYSPACE_1));
        schemaChange(format(CREATE_KEYSPACE, KEYSPACE_2));

        createTable(format(CREATE_TABLE, KEYSPACE_1, TABLE_1));
        createTable(format(CREATE_TABLE, KEYSPACE_1, TABLE_2));
        createTable(format(CREATE_TABLE, KEYSPACE_2, TABLE_1));

        execute(format(INSERT, KEYSPACE_1, TABLE_1), CLEAR_ROW);
        execute(format(INSERT, KEYSPACE_1, TABLE_2), CLEAR_ROW);
        execute(format(INSERT, KEYSPACE_2, TABLE_1), CLEAR_ROW);

        for (String user : Arrays.asList(USER, OTHER_USER))
        {
            executeNet(format("CREATE USER IF NOT EXISTS %s WITH PASSWORD '%s'", user, PASSWORD));
            executeNet(format("GRANT SELECT ON ALL KEYSPACES TO %s", user));
        }
    }

    @After
    public void after() throws Throwable
    {
        useSuperUser();
        executeNet("DROP USER IF EXISTS " + USER);
    }

    @Test
    public void testUnmaskDefaults() throws Throwable
    {
        // ordinary user without changed permissions should see masked data
        useUser(OTHER_USER, PASSWORD);
        assertMasked(KEYSPACE_1, TABLE_1);
        assertMasked(KEYSPACE_1, TABLE_2);
        assertMasked(KEYSPACE_2, TABLE_1);

        // super user should see unmasked data
        useSuperUser();
        assertClear(KEYSPACE_1, TABLE_1);
        assertClear(KEYSPACE_1, TABLE_2);
        assertClear(KEYSPACE_2, TABLE_1);

        // internal user should see unmasked data
        assertClearInternal(KEYSPACE_1, TABLE_1);
        assertClearInternal(KEYSPACE_1, TABLE_2);
        assertClearInternal(KEYSPACE_2, TABLE_1);
    }

    @Test
    public void testUnmaskOnAllKeyspaces() throws Throwable
    {
        assertPermissions(format("GRANT UNMASK ON ALL KEYSPACES TO %s", USER), () -> {
            assertClear(KEYSPACE_1, TABLE_1);
            assertClear(KEYSPACE_1, TABLE_2);
            assertClear(KEYSPACE_2, TABLE_1);
        });

        assertPermissions(format("REVOKE UNMASK ON ALL KEYSPACES FROM %s", USER), () -> {
            assertMasked(KEYSPACE_1, TABLE_1);
            assertMasked(KEYSPACE_1, TABLE_2);
            assertMasked(KEYSPACE_2, TABLE_1);
        });
    }

    @Test
    public void testUnmaskOnKeyspace() throws Throwable
    {
        assertPermissions(format("GRANT UNMASK ON KEYSPACE %s TO %s", KEYSPACE_1, USER), () -> {
            assertClear(KEYSPACE_1, TABLE_1);
            assertClear(KEYSPACE_1, TABLE_2);
            assertMasked(KEYSPACE_2, TABLE_1);
        });

        assertPermissions(format("REVOKE UNMASK ON KEYSPACE %s FROM %s", KEYSPACE_1, USER), () -> {
            assertMasked(KEYSPACE_1, TABLE_1);
            assertMasked(KEYSPACE_1, TABLE_2);
            assertMasked(KEYSPACE_2, TABLE_1);
        });
    }

    @Test
    public void testUnmaskOnTable() throws Throwable
    {
        assertPermissions(format("GRANT UNMASK ON TABLE %s.%s TO %s", KEYSPACE_1, TABLE_1, USER), () -> {
            assertClear(KEYSPACE_1, TABLE_1);
            assertMasked(KEYSPACE_1, TABLE_2);
            assertMasked(KEYSPACE_2, TABLE_1);
        });

        assertPermissions(format("REVOKE UNMASK ON TABLE %s.%s FROM %s", KEYSPACE_1, TABLE_1, USER), () -> {
            assertMasked(KEYSPACE_1, TABLE_1);
            assertMasked(KEYSPACE_1, TABLE_2);
            assertMasked(KEYSPACE_2, TABLE_1);
        });
    }

    private void assertPermissions(String alterPermissionsQuery, ThrowingRunnable assertion) throws Throwable
    {
        // alter permissions as superuser
        useSuperUser();
        executeNet(alterPermissionsQuery);

        // verify the tested user permissions
        useUser(USER, PASSWORD);
        assertion.run();

        // the ordinary user without modified permissions should keep seeing masked data
        useUser(OTHER_USER, PASSWORD);
        assertMasked(KEYSPACE_1, TABLE_1);
        assertMasked(KEYSPACE_1, TABLE_2);
        assertMasked(KEYSPACE_2, TABLE_1);

        // super user should keep seeing unmasked data
        useSuperUser();
        assertClear(KEYSPACE_1, TABLE_1);
        assertClear(KEYSPACE_1, TABLE_2);
        assertClear(KEYSPACE_2, TABLE_1);

        // internal user should keep seeing unmasked data
        assertClearInternal(KEYSPACE_1, TABLE_1);
        assertClearInternal(KEYSPACE_1, TABLE_2);
        assertClearInternal(KEYSPACE_2, TABLE_1);
    }

    private void assertMasked(String keyspace, String table) throws Throwable
    {
        assertRowsNet(executeNet(format(SELECT_WILDCARD, keyspace, table)), MASKED_ROW);
        assertRowsNet(executeNet(format(SELECT_COLUMNS, keyspace, table)), MASKED_ROW);
    }

    private void assertClear(String keyspace, String table) throws Throwable
    {
        assertRowsNet(executeNet(format(SELECT_WILDCARD, keyspace, table)), CLEAR_ROW);
        assertRowsNet(executeNet(format(SELECT_COLUMNS, keyspace, table)), CLEAR_ROW);
    }

    private void assertClearInternal(String keyspace, String table) throws Throwable
    {
        assertRows(execute(format(SELECT_WILDCARD, keyspace, table)), CLEAR_ROW);
        assertRows(execute(format(SELECT_COLUMNS, keyspace, table)), CLEAR_ROW);
    }

    @FunctionalInterface
    private interface ThrowingRunnable
    {
        void run() throws Throwable;
    }
}
