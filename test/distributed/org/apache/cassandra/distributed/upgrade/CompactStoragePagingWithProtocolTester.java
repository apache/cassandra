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

package org.apache.cassandra.distributed.upgrade;

import java.nio.ByteBuffer;
import java.util.List;

import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;

/**
 * Tests paging over a table with {@code COMPACT STORAGE} in a mixed version cluster using different protocol versions.
 */
public abstract class CompactStoragePagingWithProtocolTester extends UpgradeTestBase
{
    /**
     * The initial version from which we are upgrading.
     */
    protected abstract Semver initialVersion();

    @Test
    public void testPagingWithCompactStorageSingleClustering() throws Throwable
    {
        Object[] row1 = new Object[]{ "0", "01", "v" };
        Object[] row2 = new Object[]{ "0", "02", "v" };
        Object[] row3 = new Object[]{ "1", "01", "v" };
        Object[] row4 = new Object[]{ "1", "02", "v" };

        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1)
        .singleUpgradeToCurrentFrom(initialVersion())
        .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
        .setup(c -> {
            c.schemaChange(withKeyspace("CREATE TABLE %s.t (pk text, ck text, v text, " +
                                        "PRIMARY KEY (pk, ck)) WITH COMPACT STORAGE"));
            String insert = withKeyspace("INSERT INTO %s.t (pk, ck, v) VALUES (?, ?, ?)");
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, row1);
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, row2);
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, row3);
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, row4);
        })
        .runAfterNodeUpgrade((cluster, node) -> assertRowsWithAllProtocolVersions(row1, row2, row3, row4))
        .run();
    }

    @Test
    public void testPagingWithCompactStorageMultipleClusterings() throws Throwable
    {
        Object[] row1 = new Object[]{ "0", "01", "10", "v" };
        Object[] row2 = new Object[]{ "0", "01", "20", "v" };
        Object[] row3 = new Object[]{ "0", "02", "10", "v" };
        Object[] row4 = new Object[]{ "0", "02", "20", "v" };
        Object[] row5 = new Object[]{ "1", "01", "10", "v" };

        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1)
        .singleUpgradeToCurrentFrom(initialVersion())
        .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
        .setup(c -> {
            c.schemaChange(withKeyspace("CREATE TABLE %s.t (pk text, ck1 text, ck2 text, v text, " +
                                        "PRIMARY KEY (pk, ck1, ck2)) WITH COMPACT STORAGE"));
            String insert = withKeyspace("INSERT INTO %s.t (pk, ck1, ck2, v) VALUES (?, ?, ?, ?)");
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, row1);
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, row2);
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, row3);
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, row4);
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, row5);
        })
        .runAfterNodeUpgrade((cluster, node) -> assertRowsWithAllProtocolVersions(row1, row2, row3, row4, row5))
        .run();
    }

    @Test
    public void testPagingWithCompactStorageWithoutClustering() throws Throwable
    {
        Object[] row1 = new Object[]{ "1", "v1", "v2" };
        Object[] row2 = new Object[]{ "2", "v1", "v2" };
        Object[] row3 = new Object[]{ "3", "v1", "v2" };

        new TestCase()
        .nodes(2)
        .nodesToUpgrade(1)
        .singleUpgradeToCurrentFrom(initialVersion())
        .withConfig(config -> config.with(GOSSIP, NETWORK, NATIVE_PROTOCOL))
        .setup(c -> {
            c.schemaChange(withKeyspace("CREATE TABLE %s.t (pk text PRIMARY KEY, v1 text, v2 text) WITH COMPACT STORAGE"));
            String insert = withKeyspace("INSERT INTO %s.t (pk, v1, v2) VALUES (?, ?, ?)");
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, row1);
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, row2);
            c.coordinator(1).execute(insert, ConsistencyLevel.ALL, row3);
        })
        .runAfterNodeUpgrade((cluster, node) -> assertRowsWithAllProtocolVersions(row3, row2, row1))
        .run();
    }

    private void assertRowsWithAllProtocolVersions(Object[]... rows)
    {
        String query = withKeyspace("SELECT * FROM %s.t");
        assertRows(query, ProtocolVersion.V3, rows);
        assertRows(query, ProtocolVersion.V4, rows);
        if (initialVersion().isGreaterThanOrEqualTo(v3X))
            assertRows(query, ProtocolVersion.V5, rows);
    }

    private static void assertRows(String query, ProtocolVersion protocolVersion, Object[]... expectedRows)
    {
        Cluster.Builder builder = com.datastax.driver.core.Cluster.builder()
                                                                  .addContactPoint("127.0.0.1")
                                                                  .withProtocolVersion(protocolVersion);
        try (com.datastax.driver.core.Cluster cluster = builder.build();
             Session session = cluster.connect())
        {
            Statement stmt = new SimpleStatement(query);
            stmt.setConsistencyLevel(com.datastax.driver.core.ConsistencyLevel.ALL);
            stmt.setFetchSize(1);

            ResultSet result = session.execute(stmt);
            List<Row> actualRows = result.all();
            assertEquals(expectedRows.length, actualRows.size());

            ColumnDefinitions columnDefs = result.getColumnDefinitions();
            com.datastax.driver.core.ProtocolVersion driverProtocolVersion =
            com.datastax.driver.core.ProtocolVersion.fromInt(protocolVersion.toInt());

            for (int rowIndex = 0; rowIndex < expectedRows.length; rowIndex++)
            {
                Object[] expectedRow = expectedRows[rowIndex];
                Row actualRow = actualRows.get(rowIndex);

                assertEquals(expectedRow.length, actualRow.getColumnDefinitions().size());

                for (int columnIndex = 0; columnIndex < columnDefs.size(); columnIndex++)
                {
                    DataType type = columnDefs.getType(columnIndex);
                    ByteBuffer expectedByteValue = cluster.getConfiguration()
                                                          .getCodecRegistry()
                                                          .codecFor(type)
                                                          .serialize(expectedRow[columnIndex], driverProtocolVersion);
                    ByteBuffer actualValue = actualRow.getBytesUnsafe(columnDefs.getName(columnIndex));
                    assertEquals(expectedByteValue, actualValue);
                }
            }
        }
    }
}
