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

import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;

import static org.junit.Assert.fail;

public class GuardrailDDLEnabledTest extends GuardrailTester
{
    private static final String TEST_KS = "ddlks";
    private static final String TEST_TABLE = "ddltbl";
    private static final String TEST_VIEW = "ddlview";
    private static final String TEST_KS_NEW = "ddlks2";
    private static final String TEST_TABLE_NEW = "ddltbl2";
    private static final String TEST_VIEW_NEW = "ddlview2";
    private static final String DDL_ERROR_MSG = "DDL statement is not allowed";
    private void setGuardrail(boolean enabled)
    {
        Guardrails.instance.setDDLEnabled(enabled);
    }

    @Before
    public void beforeGuardrailTest() throws Throwable
    {
        super.beforeGuardrailTest();
        // create keyspace ddlks and table ddltbl
        execute(superClientState, getCreateKeyspaceCQL(TEST_KS, true));
        execute(superClientState, getCreateTableCQL(TEST_KS, TEST_TABLE, true));
        execute(superClientState, getCreateViewCQL(TEST_KS, TEST_VIEW, true));
        assertRowCount(execute(getSystemSchemaKeyspaceCQL(TEST_KS)), 1);
        assertRowCount(execute(getSystemSchemaTableCQL(TEST_KS, TEST_TABLE)), 1);
        assertRowCount(execute(getSystemSchemaViewCQL(TEST_KS, TEST_VIEW)), 1);
    }

    @After
    public void afterTest()
    {
        setGuardrail(true);
    }

    @Test
    public void testCannotCreateKeyspaceWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        // CREATE should not fail if: keyspace exists and IF NOT EXISTS specified (no-op)
        execute(userClientState, getCreateKeyspaceCQL(TEST_KS, true));

        // CREATE will fail with guardrail exception if user tries to create a keyspace
        assertFails(() -> execute(userClientState,
                                  getCreateKeyspaceCQL(TEST_KS_NEW, false)),
                                  DDL_ERROR_MSG);

        // CREATE will also fail if user doesn't specify IF NOT EXISTS but create an already existing keyspace
        try {
            execute(userClientState, getCreateKeyspaceCQL(TEST_KS, false));
        } catch (AlreadyExistsException e) {
            // expected
        } catch (Exception e) {
            fail(String.format("failed with unexpected error: %s", e.getMessage()));
        }

        // No new keyspace should be created
        assertEmpty(execute(getSystemSchemaKeyspaceCQL(TEST_KS_NEW)));
    }

    @Test
    public void testCannotCreateTableWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        // CREATE should not fail if: table exists and IF NOT EXISTS specified (no-op)
        execute(userClientState, getCreateTableCQL(TEST_KS, TEST_TABLE, true));

        // CREATE will fail with guardrail exception if user tries to create a table
        assertFails(() -> execute(userClientState,
                                  getCreateTableCQL(TEST_KS, TEST_TABLE_NEW, false)),
                                  DDL_ERROR_MSG);

        // CREATE will also fail if user doesn't specify IF NOT EXISTS but create an already existing table
        try {
            execute(userClientState, getCreateTableCQL(TEST_KS, TEST_TABLE, false));
        } catch (AlreadyExistsException e) {
            // expected
        } catch (Exception e) {
            fail(String.format("failed with unexpected error: %s", e.getMessage()));
        }

        // No new table should be created
        assertEmpty(execute(getSystemSchemaTableCQL(TEST_KS, TEST_TABLE_NEW)));
    }

    @Test
    public void testCannotCreateViewWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        // CREATE should not fail if: view exists and IF NOT EXISTS specified (no-op)
        execute(userClientState, getCreateViewCQL(TEST_KS, TEST_VIEW, true));

        // CREATE will fail with guardrail exception if user tries to create a view
        assertFails(() -> execute(userClientState,
                                  getCreateViewCQL(TEST_KS, TEST_VIEW_NEW, false)),
                                  DDL_ERROR_MSG);

        // CREATE will also fail if user doesn't specify IF NOT EXISTS but create an already existing view.
        try {
            execute(userClientState, getCreateViewCQL(TEST_KS, TEST_VIEW, false));
        } catch (AlreadyExistsException e) {
            // expected
        } catch (Exception e) {
            fail(String.format("failed with unexpected error: %s", e.getMessage()));
        }

        // No new view should be created
        assertEmpty(execute(getSystemSchemaViewCQL(TEST_KS, TEST_TABLE_NEW)));
    }

    @Test
    public void testCannotDropKeyspaceWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        // DROP should not fail if: keyspace not exists and IF EXISTS specified (no-op)
        execute(userClientState, getDropKeyspaceCQL(TEST_KS_NEW, true));

        // DROP will fail with guardrail exception if user tries to drop an existing keyspace
        assertFails(() -> execute(userClientState,
                                  getDropKeyspaceCQL(TEST_KS, false)),
                                  DDL_ERROR_MSG);

        // DROP will also fail if user doesn't specify IF EXISTS but keyspace doesn't exist
        try {
            execute(userClientState, getDropKeyspaceCQL(TEST_KS_NEW, false));
        } catch (InvalidRequestException e) {
            // expected
        } catch (Exception e) {
            fail(String.format("failed with unexpected error: %s", e.getMessage()));
        }

        // TEST_KS should not be dropped
        assertRowCount(execute(getSystemSchemaKeyspaceCQL(TEST_KS)), 1);
    }

    @Test
    public void testCannotDropTableWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        // DROP should not fail if: table not exists and IF EXISTS specified (no-op)
        execute(userClientState, getDropTableCQL(TEST_KS, TEST_TABLE_NEW, true));

        // DROP will fail with guardrail exception if user tries to drop an existing table
        assertFails(() -> execute(userClientState,
                                  getDropTableCQL(TEST_KS, TEST_TABLE, false)),
                                  DDL_ERROR_MSG);

        // DROP will also fail if user doesn't specify IF EXISTS but table doesn't exist
        try {
            execute(userClientState, getDropTableCQL(TEST_KS, TEST_TABLE_NEW, false));
        } catch (InvalidRequestException e) {
            // expected
        } catch (Exception e) {
            fail(String.format("failed with unexpected error: %s", e.getMessage()));
        }

        // TEST_TABLE should not be dropped
        assertRowCount(execute(getSystemSchemaTableCQL(TEST_KS, TEST_TABLE)), 1);
    }

    @Test
    public void testCannotDropViewWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        // DROP should not fail if: view not exists and IF EXISTS specified (no-op)
        execute(userClientState, getDropViewCQL(TEST_KS, TEST_VIEW_NEW, true));

        // DROP will fail with guardrail exception if user tries to drop an existing view
        assertFails(() -> execute(userClientState,
                                  getDropViewCQL(TEST_KS, TEST_VIEW, false)),
                                  DDL_ERROR_MSG);

        // DROP will also fail if user doesn't specify IF EXISTS but view doesn't exist
        try {
            execute(userClientState, getDropViewCQL(TEST_KS, TEST_VIEW_NEW, false));
        } catch (InvalidRequestException e) {
            // expected
        } catch (Exception e) {
            fail(String.format("failed with unexpected error: %s", e.getMessage()));
        }

        // TEST_VIEW should not be dropped
        assertRowCount(execute(getSystemSchemaViewCQL(TEST_KS, TEST_VIEW)), 1);
    }

    @Test
    public void testCannotDropColumnWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        // table have column col1
        assertRowCount(execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND column_name='col1'",
                                             SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                             SchemaKeyspaceTables.COLUMNS,
                                             TEST_KS, TEST_TABLE)),
                       1);
        // ALTER TABLE drop column should not fail if: table exists and the column doesn't exist and user specify IF EXISTS (no-op)
        execute(userClientState, String.format("ALTER TABLE %s.%s DROP IF EXISTS col3", TEST_KS, TEST_TABLE));

        // ALTER TABLE will fail with guardrail excepetion if user tries to add new column to this table
        assertFails(() -> execute(userClientState,
                                  String.format("ALTER TABLE %s.%s DROP col1", TEST_KS, TEST_TABLE)),
                                  DDL_ERROR_MSG);

        // column col1 should not be dropped
        assertRowCount(execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND column_name='col1'",
                                             SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                             SchemaKeyspaceTables.COLUMNS,
                                             TEST_KS, TEST_TABLE)),
                       1);
    }

    @Test
    public void testCannotAlterKeyspaceWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        // keyspace should have durable_write=true by default
        assertEmpty(execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND durable_writes=false ALLOW FILTERING",
                                          SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                          SchemaKeyspaceTables.KEYSPACES,
                                          TEST_KS)));
        // ALTER KEYSPACE should not fail if: keyspace not exists and IF EXISTS specified (no-op)
        execute(userClientState, String.format("ALTER KEYSPACE IF EXISTS %s WITH durable_writes=false", TEST_KS_NEW));
        // ALTER TABLE will fail with guardrail excepetion if user tries to alter anything related to this table
        assertFails(() -> execute(userClientState,
                                  String.format("ALTER KEYSPACE %s WITH durable_writes=false", TEST_KS)),
                                  DDL_ERROR_MSG);

        // keyspace should still have durable_write=true
        assertEmpty(execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND durable_writes=false ALLOW FILTERING",
                                          SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                          SchemaKeyspaceTables.KEYSPACES,
                                          TEST_KS)));
    }

    @Test
    public void testCannotAlterTableWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        // table doesn't have comment
        assertEmpty(execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND comment='test' ALLOW FILTERING",
                                          SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                          SchemaKeyspaceTables.TABLES,
                                          TEST_KS, TEST_TABLE)));
        // ALTER TABLE should not fail if: table not exists and IF EXISTS specified (no-op)
        execute(userClientState, String.format("ALTER TABLE IF EXISTS %s.%s WITH comment='test'", TEST_KS, TEST_TABLE_NEW));
        // ALTER TABLE will fail with guardrail excepetion if user tries to alter anything related to this table
        assertFails(() -> execute(userClientState,
                                  String.format("ALTER TABLE %s.%s WITH comment='test'", TEST_KS, TEST_TABLE)),
                                  DDL_ERROR_MSG);

        // table should not have comment
        assertEmpty(execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND comment='test' ALLOW FILTERING",
                                          SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                          SchemaKeyspaceTables.TABLES,
                                          TEST_KS, TEST_TABLE)));
    }

    @Test
    public void testCannotAddColumnWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        // table doesn't have new column col3
        assertEmpty(execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND column_name='col3'",
                                          SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                          SchemaKeyspaceTables.COLUMNS,
                                          TEST_KS, TEST_TABLE)));
        // ALTER TABLE add column should not fail if: table not exists and IF EXISTS specified (no-op)
        execute(userClientState, String.format("ALTER TABLE IF EXISTS %s.%s ADD col3 text", TEST_KS, TEST_TABLE_NEW));
        // ALTER TABLE will fail with guardrail excepetion if user tries to add new column to this table
        assertFails(() -> execute(userClientState,
                                  String.format("ALTER TABLE %s.%s ADD col3 text", TEST_KS, TEST_TABLE)),
                                  DDL_ERROR_MSG);

        // table should not have new column col3
        assertEmpty(execute(String.format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND column_name='col3'",
                                          SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                          SchemaKeyspaceTables.COLUMNS,
                                          TEST_KS, TEST_TABLE)));
    }

    private String getCreateKeyspaceCQL(String ks, boolean ifNotExists) {
        if (ifNotExists) {
            return String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", ks);
        }
        return String.format("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", ks);
    }

    private String getDropKeyspaceCQL(String ks, boolean ifExists) {
        if (ifExists) {
            return String.format("DROP KEYSPACE IF EXISTS %s", ks);
        }
        return String.format("DROP KEYSPACE %s", ks);
    }

    private String getCreateTableCQL(String ks, String table, boolean ifNotExists) {
        if (ifNotExists) {
            return String.format("CREATE TABLE IF NOT EXISTS %s.%s (key text PRIMARY KEY, col1 text, col2 text)", ks, table);
        }
        return String.format("CREATE TABLE %s.%s (key text PRIMARY KEY, col1 text, col2 text)", ks, table);
    }

    private String getDropTableCQL(String ks, String table, boolean ifExists) {
        if (ifExists) {
            return String.format("DROP TABLE IF EXISTS %s.%s", ks, table);
        }
        return String.format("DROP TABLE %s.%s", ks, table);
    }

    private String getCreateViewCQL(String ks, String table, boolean ifNotExists) {
        if (ifNotExists) {
            return String.format("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT key FROM %s.%s WHERE key IS NOT NULL PRIMARY KEY (key)",
                                 ks, table, TEST_KS, TEST_TABLE);
        }
        return String.format("CREATE MATERIALIZED VIEW %s.%s AS SELECT key FROM %s.%s WHERE key IS NOT NULL PRIMARY KEY (key)",
                             ks, table, TEST_KS, TEST_TABLE);
    }

    private String getDropViewCQL(String ks, String view, boolean ifExists) {
        if (ifExists) {
            return String.format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", ks, view);
        }
        return String.format("DROP MATERIALIZED VIEW %s.%s", ks, view);
    }

    private String getSystemSchemaKeyspaceCQL(String ks)
    {
        return String.format("SELECT * FROM %s.%s WHERE keyspace_name='%s'",
                             SchemaConstants.SCHEMA_KEYSPACE_NAME,
                             SchemaKeyspaceTables.KEYSPACES,
                             ks);
    }

    private String getSystemSchemaTableCQL(String ks, String table)
    {
        return String.format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s'",
                             SchemaConstants.SCHEMA_KEYSPACE_NAME,
                             SchemaKeyspaceTables.TABLES,
                             ks, table);
    }

    private String getSystemSchemaViewCQL(String ks, String view)
    {
        return String.format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND view_name='%s'",
                             SchemaConstants.SCHEMA_KEYSPACE_NAME,
                             SchemaKeyspaceTables.VIEWS,
                             ks, view);
    }
}
