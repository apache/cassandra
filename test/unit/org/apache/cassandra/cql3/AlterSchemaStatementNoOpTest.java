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

package org.apache.cassandra.cql3;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.triggers.TriggersTest;

import static java.lang.String.format;
import static org.junit.Assert.assertTrue;

public class AlterSchemaStatementNoOpTest extends CQLTester
{
    // Tests that when an AlterSchemaStatement is a no-op according to the local schema representation, then the
    // schema transformation is not submitted to the CMS and serialized into the cluster metadata log. e.g.
    // * The statement has an IF NOT EXISTS and the target element is present in local schema
    // * The statement has IF EXISTS and the target element is not present locally
    @Test
    public void testKeyspaceNoOps()
    {
        String ks = name();
        assertNoEpochChange(format("ALTER KEYSPACE IF EXISTS %s WITH replication = {'class': 'SimpleStrategy'}", ks));
        assertMultipleExecutionSingleEpoch(format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy'}", ks));
        assertMultipleExecutionSingleEpoch(format("DROP KEYSPACE IF EXISTS %s", ks));
    }

    @Test
    public void testTableNoOps()
    {
        boolean ddm = DatabaseDescriptor.getDynamicDataMaskingEnabled();
        try
        {
            DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
            String table = KEYSPACE + '.' + name();
            assertNoEpochChange(format("ALTER TABLE IF EXISTS %s WITH comment = 'test'", table));
            assertNoEpochChange(format("ALTER TABLE IF EXISTS %s DROP COMPACT STORAGE", table));
            assertMultipleExecutionSingleEpoch(format("CREATE TABLE IF NOT EXISTS %s (k int PRIMARY KEY, v int)", table));
            assertNoEpochChange(format("ALTER TABLE %s ADD IF NOT EXISTS v text", table));
            assertNoEpochChange(format("ALTER TABLE %s ALTER IF EXISTS foo MASKED WITH mask_default()", table));
            assertNoEpochChange(format("ALTER TABLE %s ALTER IF EXISTS foo DROP MASKED", table));
            assertNoEpochChange(format("ALTER TABLE %s DROP IF EXISTS foo", table));
            // RENAME <col> doesn't currently obey the IF EXISTS clause as an IRE is thrown if the column isn't found
            // assertNoEpochChange(format("ALTER TABLE %s RENAME IF EXISTS foo TO bar", table));
            assertMultipleExecutionSingleEpoch(format("DROP TABLE IF EXISTS %s", table));
        }
        finally
        {
            DatabaseDescriptor.setDynamicDataMaskingEnabled(ddm);
        }
    }

    @Test
    public void testUserTypeNoOps()
    {
        String type = KEYSPACE + '.' + name();
        assertNoEpochChange(format("ALTER TYPE IF EXISTS %s ADD f1 int", type));
        assertMultipleExecutionSingleEpoch(format("CREATE TYPE IF NOT EXISTS %s (f1 int);", type));
        assertNoEpochChange(format("ALTER TYPE IF EXISTS %s ADD IF NOT EXISTS f1 int", type));
        assertNoEpochChange(format("ALTER TYPE IF EXISTS %s RENAME IF EXISTS foo to bar", type));
        assertMultipleExecutionSingleEpoch(format("DROP TYPE IF EXISTS %s", type));
    }

    @Test
    public void testIndexNoOps()
    {
        String table = createTable("create table %s (k int primary key, v int)");
        String index = name();
        assertMultipleExecutionSingleEpoch(format("CREATE INDEX IF NOT EXISTS %s on %s.%s(v)", index, KEYSPACE, table));
        assertMultipleExecutionSingleEpoch(format("DROP INDEX IF EXISTS %s.%s", KEYSPACE, index));
    }

    @Test
    public void testMaterializedViewNoOps()
    {
        String table = KEYSPACE + '.' + createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY(k,c))");
        String view = KEYSPACE + '.' + name();
        assertNoEpochChange(format("ALTER MATERIALIZED VIEW IF EXISTS %s WITH comment = 'test'", view));
        assertMultipleExecutionSingleEpoch(format("CREATE MATERIALIZED VIEW IF NOT EXISTS %s " +
                                                  "AS SELECT * FROM %s " +
                                                  "WHERE v IS NOT NULL AND k IS NOT NULL AND c IS NOT NULL " +
                                                  "PRIMARY KEY (v,k,c)",
                                                  view, table));
        assertMultipleExecutionSingleEpoch(format("DROP MATERIALIZED VIEW IF EXISTS %s", view));
    }

    @Test
    public void testTriggerNoOps()
    {
        String table = createTable("create table %s (k int primary key, v int)");
        String trigger = name();
        assertMultipleExecutionSingleEpoch(format("CREATE TRIGGER IF NOT EXISTS %s ON %s.%s USING '%s'",
                                                  trigger, KEYSPACE, table, TriggersTest.TestTrigger.class.getName()));
        assertMultipleExecutionSingleEpoch(format("DROP TRIGGER IF EXISTS %s ON %s.%s", trigger, KEYSPACE, table));
    }

    @Test
    public void testUserFunctionNoOp()
    {
        String function = name();
        assertMultipleExecutionSingleEpoch(format("CREATE FUNCTION IF NOT EXISTS %s.%s(a int, b int) " +
                                                  "CALLED ON NULL INPUT " +
                                                  "RETURNS int " +
                                                  "LANGUAGE java " +
                                                  "AS 'return Integer.valueOf((a!=null?a.intValue():0) + b.intValue());'",
                                                  KEYSPACE, function));
        assertMultipleExecutionSingleEpoch(format("DROP FUNCTION IF EXISTS %s.%s", KEYSPACE, function));
    }

    @Test
    public void testUserAggregateNoOp() throws Throwable
    {
        String function = createFunction(KEYSPACE,
                                  "double, double",
                                  "CREATE OR REPLACE FUNCTION %s(state double, val double) " +
                                  "RETURNS NULL ON NULL INPUT " +
                                  "RETURNS double " +
                                  "LANGUAGE java " +
                                  "AS 'return state;';");
        String aggregate = name();
        assertMultipleExecutionSingleEpoch(format("CREATE AGGREGATE IF NOT EXISTS %s.%s(double) " +
                                                  "SFUNC %s " +
                                                  "STYPE double " +
                                                  "INITCOND 0",
                                                  KEYSPACE, aggregate, shortFunctionName(function)));
        assertMultipleExecutionSingleEpoch(format("DROP AGGREGATE IF EXISTS %s.%s", KEYSPACE, aggregate));
    }

    private void assertMultipleExecutionSingleEpoch(String cql)
    {
        // execute the statement once to ensure it is valid and to set
        // up the test condition (i.e. the thing exists/doesn't exist)
        Epoch first = ClusterMetadata.current().epoch;
        QueryProcessor.executeInternal(cql);
        Epoch second = ClusterMetadata.current().epoch;
        assertTrue(second.isAfter(first));
        // execute again to check this is truly a no-op and the epoch isn't incremented
        QueryProcessor.executeInternal(cql);
        Epoch third = ClusterMetadata.current().epoch;
        assertTrue(third.is(second));
    }

    private void assertNoEpochChange(String cql)
    {
        Epoch first = ClusterMetadata.current().epoch;
        QueryProcessor.executeInternal(cql);
        Epoch second = ClusterMetadata.current().epoch;
        assertTrue(second.is(first));
    }

    private String name()
    {
        return "n" + System.nanoTime();
    }
}
