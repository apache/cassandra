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
import org.apache.cassandra.cql3.statements.schema.AlterSchemaStatement;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.service.QueryState;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class GuardrailDDLEnabledTest extends GuardrailTester
{
    private static final String TEST_KS = "ddlks";
    private static final String TEST_TABLE = "ddltbl";
    private static final String TEST_VIEW = "ddlview";
    private static final String DDL_ERROR_MSG = "DDL statement is not allowed";

    private void setGuardrail(boolean enabled)
    {
        Guardrails.instance.setDDLEnabled(enabled);
    }

    @Before
    public void beforeGuardrailTest() throws Throwable
    {
        super.beforeGuardrailTest();
        // grant current user permission to all keyspaces
        useSuperUser();
        executeNet(format("GRANT ALL ON ALL KEYSPACES TO %s", USERNAME));
        useUser(USERNAME, PASSWORD);
    }

    @After
    public void afterTest()
    {
        setGuardrail(true);
        executeNet(getDropViewCQL(TEST_KS, TEST_VIEW));
        executeNet(getDropKeyspaceCQL(TEST_KS));
    }

    @Test
    public void testCannotCreateKeyspaceWhileFeatureDisabled() throws Throwable
    {
        setGuardrail(false);
        shouldFailWithDDLErrorMsg(getCreateKeyspaceCQL(TEST_KS));
        // No new keyspace should be created
        assertEmpty(execute(getSystemSchemaKeyspaceCQL(TEST_KS)));

        setGuardrail(true);
        executeNet(getCreateKeyspaceCQL(TEST_KS));
        assertRowCount(execute(getSystemSchemaKeyspaceCQL(TEST_KS)), 1);
    }

    @Test
    public void testCannotCreateTableWhileFeatureDisabled() throws Throwable
    {
        executeNet(getCreateKeyspaceCQL(TEST_KS));

        setGuardrail(false);
        shouldFailWithDDLErrorMsg(getCreateTableCQL(TEST_KS, TEST_TABLE));
        // No new table should be created
        assertEmpty(execute(getSystemSchemaTableCQL(TEST_KS, TEST_TABLE)));

        setGuardrail(true);
        executeNet(getCreateTableCQL(TEST_KS, TEST_TABLE));
        assertRowCount(execute(getSystemSchemaTableCQL(TEST_KS, TEST_TABLE)), 1);
    }

    @Test
    public void testCannotCreateViewWhileFeatureDisabled() throws Throwable
    {
        executeNet(getCreateKeyspaceCQL(TEST_KS));
        executeNet(getCreateTableCQL(TEST_KS, TEST_TABLE));

        setGuardrail(false);
        shouldFailWithDDLErrorMsg(getCreateViewCQL(TEST_KS, TEST_VIEW));
        // No new view should be created
        assertEmpty(execute(getSystemSchemaViewCQL(TEST_KS, TEST_VIEW)));

        setGuardrail(true);
        executeNet(getCreateViewCQL(TEST_KS, TEST_VIEW));
        assertRowCount(execute(getSystemSchemaViewCQL(TEST_KS, TEST_VIEW)), 1);
    }

    @Test
    public void testCannotDropKeyspaceWhileFeatureDisabled() throws Throwable
    {
        executeNet(getCreateKeyspaceCQL(TEST_KS));

        setGuardrail(false);
        shouldFailWithDDLErrorMsg(getDropKeyspaceCQL(TEST_KS));
        assertRowCount(execute(getSystemSchemaKeyspaceCQL(TEST_KS)), 1);

        setGuardrail(true);
        executeNet(getDropKeyspaceCQL(TEST_KS));
        assertEmpty(execute(getSystemSchemaKeyspaceCQL(TEST_KS)));
    }

    @Test
    public void testCannotDropTableWhileFeatureDisabled() throws Throwable
    {
        executeNet(getCreateKeyspaceCQL(TEST_KS));
        executeNet(getCreateTableCQL(TEST_KS, TEST_TABLE));

        setGuardrail(false);
        shouldFailWithDDLErrorMsg(getDropTableCQL(TEST_KS, TEST_TABLE));
        assertRowCount(execute(getSystemSchemaTableCQL(TEST_KS, TEST_TABLE)), 1);

        setGuardrail(true);
        executeNet(getDropTableCQL(TEST_KS, TEST_TABLE));
        assertEmpty(execute(getSystemSchemaTableCQL(TEST_KS, TEST_TABLE)));
    }

    @Test
    public void testCannotDropViewWhileFeatureDisabled() throws Throwable
    {
        executeNet(getCreateKeyspaceCQL(TEST_KS));
        executeNet(getCreateTableCQL(TEST_KS, TEST_TABLE));
        executeNet(getCreateViewCQL(TEST_KS, TEST_VIEW));

        setGuardrail(false);
        shouldFailWithDDLErrorMsg(getDropViewCQL(TEST_KS, TEST_VIEW));
        assertRowCount(execute(getSystemSchemaViewCQL(TEST_KS, TEST_VIEW)), 1);

        setGuardrail(true);
        executeNet(getDropViewCQL(TEST_KS, TEST_VIEW));
        assertEmpty(execute(getSystemSchemaViewCQL(TEST_KS, TEST_VIEW)));
    }

    @Test
    public void testCannotDropColumnWhileFeatureDisabled() throws Throwable
    {
        executeNet(getCreateKeyspaceCQL(TEST_KS));
        executeNet(getCreateTableCQL(TEST_KS, TEST_TABLE));
        setGuardrail(false);
        // table have column col1
        assertRowCount(execute(format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND column_name='col1'",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                      SchemaKeyspaceTables.COLUMNS,
                                      TEST_KS,
                                      TEST_TABLE)), 1);
        shouldFailWithDDLErrorMsg(format("ALTER TABLE %s.%s DROP IF EXISTS col1", TEST_KS, TEST_TABLE));
        // column col1 should not be dropped
        assertRowCount(execute(format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND column_name='col1'",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                      SchemaKeyspaceTables.COLUMNS,
                                      TEST_KS,
                                      TEST_TABLE)), 1);

        setGuardrail(true);
        executeNet(format("ALTER TABLE %s.%s DROP IF EXISTS col1", TEST_KS, TEST_TABLE));
        assertEmpty(execute(format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND column_name='col1'",
                                   SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                   SchemaKeyspaceTables.COLUMNS,
                                   TEST_KS,
                                   TEST_TABLE)));
    }

    @Test
    public void testCannotAlterKeyspaceWhileFeatureDisabled() throws Throwable
    {
        executeNet(getCreateKeyspaceCQL(TEST_KS));
        setGuardrail(false);
        // keyspace should have durable_write=true by default
        assertEmpty(execute(format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND durable_writes=false ALLOW FILTERING",
                                   SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                   SchemaKeyspaceTables.KEYSPACES,
                                   TEST_KS)));
        shouldFailWithDDLErrorMsg(format("ALTER KEYSPACE %s WITH durable_writes=false", TEST_KS));
        // keyspace should still have durable_write=true
        assertEmpty(execute(format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND durable_writes=false ALLOW FILTERING",
                                   SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                   SchemaKeyspaceTables.KEYSPACES,
                                   TEST_KS)));

        setGuardrail(true);
        executeNet(format("ALTER KEYSPACE %s WITH durable_writes=false", TEST_KS));
        assertRowCount(execute(format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND durable_writes=false ALLOW FILTERING",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                      SchemaKeyspaceTables.KEYSPACES,
                                      TEST_KS)), 1);
    }

    @Test
    public void testCannotAlterTableWhileFeatureDisabled() throws Throwable
    {
        executeNet(getCreateKeyspaceCQL(TEST_KS));
        executeNet(getCreateTableCQL(TEST_KS, TEST_TABLE));
        setGuardrail(false);
        // table doesn't have comment
        assertEmpty(execute(format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND comment='test' ALLOW FILTERING",
                                   SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                   SchemaKeyspaceTables.TABLES,
                                   TEST_KS, TEST_TABLE)));
        shouldFailWithDDLErrorMsg(format("ALTER TABLE %s.%s WITH comment='test'", TEST_KS, TEST_TABLE));
        // table should not have comment
        assertEmpty(execute(format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND comment='test' ALLOW FILTERING",
                                   SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                   SchemaKeyspaceTables.TABLES,
                                   TEST_KS, TEST_TABLE)));

        setGuardrail(true);
        executeNet(format("ALTER TABLE %s.%s WITH comment='test'", TEST_KS, TEST_TABLE));
        assertRowCount(execute(format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND comment='test' ALLOW FILTERING",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                      SchemaKeyspaceTables.TABLES,
                                      TEST_KS, TEST_TABLE)), 1);
    }

    @Test
    public void testCannotAddColumnWhileFeatureDisabled() throws Throwable
    {
        executeNet(getCreateKeyspaceCQL(TEST_KS));
        executeNet(getCreateTableCQL(TEST_KS, TEST_TABLE));
        setGuardrail(false);
        // table doesn't have new column col3
        assertEmpty(execute(format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND column_name='col3'",
                                   SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                   SchemaKeyspaceTables.COLUMNS,
                                   TEST_KS, TEST_TABLE)));
        shouldFailWithDDLErrorMsg(format("ALTER TABLE %s.%s ADD col3 text", TEST_KS, TEST_TABLE));
        // table should not have new column col3
        assertEmpty(execute(format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND column_name='col3'",
                                   SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                   SchemaKeyspaceTables.COLUMNS,
                                   TEST_KS, TEST_TABLE)));

        setGuardrail(true);
        executeNet(format("ALTER TABLE %s.%s ADD col3 text", TEST_KS, TEST_TABLE));
        // table should not have new column col3
        assertRowCount(execute(format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s' AND column_name='col3'",
                                      SchemaConstants.SCHEMA_KEYSPACE_NAME,
                                      SchemaKeyspaceTables.COLUMNS,
                                      TEST_KS, TEST_TABLE)), 1);
    }

    @Test
    public void testMockAlterSchemaStatementsWhileFeaturesDisabled()
    {
        AlterSchemaStatement alterSchemaStatement = mock(AlterSchemaStatement.class);
        QueryState queryState = mock(QueryState.class);

        doReturn(userClientState).when(queryState).getClientState();
        doCallRealMethod().when(alterSchemaStatement).isDDLStatement();
        QueryProcessor qp = QueryProcessor.instance;

        setGuardrail(false);

        assertThatThrownBy(() -> qp.processStatement(alterSchemaStatement, queryState, QueryOptions.DEFAULT, 0L))
        .isInstanceOf(GuardrailViolatedException.class);

        verify(alterSchemaStatement).isDDLStatement();

        setGuardrail(true);
        qp.processStatement(alterSchemaStatement, queryState, QueryOptions.DEFAULT, 0L);
    }

    private void shouldFailWithDDLErrorMsg(String query)
    {
        assertThatThrownBy(() -> executeNet(query))
        .isInstanceOf(InvalidQueryException.class)
        .hasMessageContaining(DDL_ERROR_MSG);
    }

    private String getCreateKeyspaceCQL(String ks)
    {
        return format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}", ks);
    }

    private String getDropKeyspaceCQL(String ks)
    {
        return format("DROP KEYSPACE IF EXISTS %s", ks);
    }

    private String getCreateTableCQL(String ks, String table)
    {
        return format("CREATE TABLE IF NOT EXISTS %s.%s (key text PRIMARY KEY, col1 text, col2 text)", ks, table);
    }

    private String getDropTableCQL(String ks, String table)
    {
        return format("DROP TABLE IF EXISTS %s.%s", ks, table);
    }

    private String getCreateViewCQL(String ks, String table)
    {
        return format("CREATE MATERIALIZED VIEW IF NOT EXISTS %s.%s AS SELECT key FROM %s.%s WHERE key IS NOT NULL PRIMARY KEY (key)", ks, table, TEST_KS, TEST_TABLE);
    }

    private String getDropViewCQL(String ks, String view)
    {
        return format("DROP MATERIALIZED VIEW IF EXISTS %s.%s", ks, view);
    }

    private String getSystemSchemaKeyspaceCQL(String ks)
    {
        return format("SELECT * FROM %s.%s WHERE keyspace_name='%s'", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.KEYSPACES, ks);
    }

    private String getSystemSchemaTableCQL(String ks, String table)
    {
        return format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND table_name='%s'", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES, ks, table);
    }

    private String getSystemSchemaViewCQL(String ks, String view)
    {
        return format("SELECT * FROM %s.%s WHERE keyspace_name='%s' AND view_name='%s'", SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.VIEWS, ks, view);
    }
}
