/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.cassandra.schema;

import java.io.IOException;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.QueryMessage;

import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CreateTableValidationTest extends CQLTester
{
    @Test
    public void testInvalidBloomFilterFPRatio() throws Throwable
    {
        try
        {
            createTableMayThrow("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.0000001");
            fail("Expected an fp chance of 0.0000001 to be rejected");
        }
        catch (ConfigurationException exc) { }

        try
        {
            createTableMayThrow("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 1.1");
            fail("Expected an fp chance of 1.1 to be rejected");
        }
        catch (ConfigurationException exc) { }

        // sanity check
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b int) WITH bloom_filter_fp_chance = 0.1");
    }

    @Deprecated // these warning thresholds will be replaced by equivalent guardrails
    @Test
    public void testCreateKeyspaceTableWarning() throws IOException
    {
        requireNetwork();
        int tableCountWarn = DatabaseDescriptor.tableCountWarnThreshold();
        int keyspaceCountWarn = DatabaseDescriptor.keyspaceCountWarnThreshold();
        DatabaseDescriptor.setTableCountWarnThreshold(Schema.instance.getNumberOfTables());
        DatabaseDescriptor.setKeyspaceCountWarnThreshold(Schema.instance.getKeyspaces().size());

        try (SimpleClient client = newSimpleClient(ProtocolVersion.CURRENT).connect(false))
        {
            String createKeyspace = "CREATE KEYSPACE createkswarning%d WITH REPLICATION={'class':'org.apache.cassandra.locator.NetworkTopologyStrategy','datacenter1':'2'}";
            QueryMessage query = new QueryMessage(String.format(createKeyspace, 1), QueryOptions.DEFAULT);
            Message.Response resp = client.execute(query);
            List<String> warns = resp.getWarnings();
            warns.removeIf(w -> w.contains("is higher than the number of nodes"));
            assertTrue(warns.size() > 0);
            assertTrue(warns.get(0).contains("Having a large number of keyspaces will significantly"));

            DatabaseDescriptor.setKeyspaceCountWarnThreshold(Schema.instance.getKeyspaces().size() + 2);
            query = new QueryMessage(String.format(createKeyspace, 2), QueryOptions.DEFAULT);
            resp = client.execute(query);
            warns = resp.getWarnings();
            if (warns != null)
                warns.removeIf(w -> w.contains("is higher than the number of nodes"));
            assertTrue(warns == null || warns.isEmpty());

            query = new QueryMessage(String.format("CREATE TABLE %s.%s (id int primary key, x int)", KEYSPACE, "test1"), QueryOptions.DEFAULT);
            resp = client.execute(query);
            warns = resp.getWarnings();
            warns.removeIf(w -> w.contains("is higher than the number of nodes"));
            assertTrue(warns.size() > 0);
            assertTrue(warns.get(0).contains("Having a large number of tables"));

            DatabaseDescriptor.setTableCountWarnThreshold(Schema.instance.getNumberOfTables() + 1);
            query = new QueryMessage(String.format("CREATE TABLE %s.%s (id int primary key, x int)", KEYSPACE, "test2"), QueryOptions.DEFAULT);
            resp = client.execute(query);
            assertTrue(resp.getWarnings() == null || resp.getWarnings().isEmpty());
        }
        finally
        {
            DatabaseDescriptor.setTableCountWarnThreshold(tableCountWarn);
            DatabaseDescriptor.setKeyspaceCountWarnThreshold(keyspaceCountWarn);
        }
    }

    @Test
    public void testCreateTableOnSelectedClusteringColumn()
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC);");
    }

    @Test
    public void testCreateTableOnAllClusteringColumns()
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC);");
    }

    @Test
    public void testCreateTableErrorOnNonClusteringKey()
    {
        String expectedMessage = "Only clustering key columns can be defined in CLUSTERING ORDER directive";
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC, v ASC);",
                        expectedMessage+": [v]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (v ASC);",
                        expectedMessage+": [v]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk ASC);",
                        expectedMessage+": [pk]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk ASC, ck1 DESC);",
                        expectedMessage+": [pk]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, ck2 DESC, pk DESC);",
                        expectedMessage+": [pk]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk DESC, v DESC);",
                        expectedMessage+": [pk, v]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (pk DESC, v DESC, ck1 DESC);",
                        expectedMessage+": [pk, v]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck1 ASC, v ASC);",
                        expectedMessage+": [v]");
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (v ASC, ck1 DESC);",
                        expectedMessage+": [v]");
    }

    @Test
    public void testCreateTableInWrongOrder()
    {
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck2 ASC, ck1 DESC);",
                        "The order of columns in the CLUSTERING ORDER directive must match that of the clustering columns");
    }

    @Test
    public void testCreateTableWithMissingClusteringColumn()
    {
        expectedFailure("CREATE TABLE %s (pk int, ck1 int, ck2 int, v int, PRIMARY KEY ((pk),ck1, ck2)) WITH CLUSTERING ORDER BY (ck2 ASC);",
                        "Missing CLUSTERING ORDER for column ck1");
    }

    private void expectedFailure(String statement, String errorMsg)
    {

        assertThatExceptionOfType(InvalidRequestException.class)
        .isThrownBy(() -> createTableMayThrow(statement)) .withMessageContaining(errorMsg);
    }
}
