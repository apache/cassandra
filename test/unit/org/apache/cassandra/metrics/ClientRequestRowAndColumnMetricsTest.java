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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.paxos.Paxos;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.BatchMessage;
import org.apache.cassandra.transport.messages.QueryMessage;

import static org.junit.Assert.assertEquals;

import static org.apache.cassandra.transport.ProtocolVersion.CURRENT;

public class ClientRequestRowAndColumnMetricsTest extends CQLTester
{
    @BeforeClass
    public static void setup()
    {
        requireNetwork();
    }

    @Before
    public void clearMetrics()
    {
        ClientRequestSizeMetrics.totalRowsRead.dec(ClientRequestSizeMetrics.totalRowsRead.getCount());
        ClientRequestSizeMetrics.totalColumnsRead.dec(ClientRequestSizeMetrics.totalColumnsRead.getCount());
        ClientRequestSizeMetrics.totalRowsWritten.dec(ClientRequestSizeMetrics.totalRowsWritten.getCount());
        ClientRequestSizeMetrics.totalColumnsWritten.dec(ClientRequestSizeMetrics.totalColumnsWritten.getCount());

        StorageProxy.instance.setClientRequestSizeMetricsEnabled(true);
    }

    @Test
    public void shouldRecordReadMetricsForMultiRowPartitionSelection()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE pk = 1");

        assertEquals(2, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The partition key is provided by the client in the request, so we don't consider those columns as read.
        assertEquals(4, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsWithOnlyPartitionKeyInSelect()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2)");
        executeNet(CURRENT, "SELECT pk FROM %s WHERE pk = 1");

        assertEquals(2, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The partition key is provided by the client in the request, so we don't consider that column read.
        assertEquals(0, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsWithOnlyClusteringKeyInSelect()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2)");
        executeNet(CURRENT, "SELECT ck FROM %s WHERE pk = 1");

        assertEquals(2, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The partition key is provided by the client in the request, so we don't consider that column read.
        assertEquals(2, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldNotRecordReadMetricsWhenDisabled()
    {
        StorageProxy.instance.setClientRequestSizeMetricsEnabled(false);

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE pk = 1");

        assertEquals(0, ClientRequestSizeMetrics.totalRowsRead.getCount());
        assertEquals(0, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsWithSingleRowSelection()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE pk = 1 AND ck = 1");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // Both the partition key and clustering key are provided by the client in the request.
        assertEquals(1, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsWithSliceRestriction()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE pk = 1 AND ck > 0");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The partition key is selected, but the restriction over the clustering key is a slice.
        assertEquals(2, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsWithINRestrictionSinglePartition()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE pk = 1 AND ck IN (0, 1)");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The partition key and clustering key are both selected.
        assertEquals(1, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsWithINRestrictionMultiplePartitions()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 2, 3)");
        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (4, 5, 6)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE pk IN (1, 4)");

        assertEquals(2, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The partition key is selected, but there is no clustering restriction.
        assertEquals(4, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsForMultiColumnClusteringRestriction()
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, ck3 int, v int, PRIMARY KEY (pk, ck1, ck2, ck3))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck1, ck2, ck3, v) VALUES (1, 2, 3, 4, 6)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE pk = 1 AND ck1 = 2 AND (ck2, ck3) = (3, 4)");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The entire primary key is selected, so only one value is actually read.
        assertEquals(1, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsForClusteringSlice()
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, ck3 int, v int, PRIMARY KEY (pk, ck1, ck2, ck3))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck1, ck2, ck3, v) VALUES (1, 2, 3, 4, 6)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE pk = 1 AND ck1 = 2 AND ck2 = 3 AND ck3 >= 4");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The last clustering key element isn't bound, so count it as being read.
        assertEquals(2, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsForTokenAndClusteringSlice()
    {
        createTable("CREATE TABLE %s (pk int, ck1 int, ck2 int, ck3 int, v int, PRIMARY KEY (pk, ck1, ck2, ck3))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck1, ck2, ck3, v) VALUES (1, 2, 3, 4, 6)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE token(pk) = token(1) AND ck1 = 2 AND ck2 = 3 AND ck3 >= 4 ALLOW FILTERING");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // Last clustering is a slice, and the partition key is restricted on token, so count them as read.
        assertEquals(3, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordWriteMetricsForSingleValueRow()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsWritten.getCount());
        assertEquals(1, ClientRequestSizeMetrics.totalColumnsWritten.getCount());
    }

    @Test
    public void shouldNotRecordWriteMetricsWhenDisabled()
    {
        StorageProxy.instance.setClientRequestSizeMetricsEnabled(false);

        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");

        assertEquals(0, ClientRequestSizeMetrics.totalRowsWritten.getCount());
        assertEquals(0, ClientRequestSizeMetrics.totalColumnsWritten.getCount());
    }

    @Test
    public void shouldRecordWriteMetricsForMultiValueRow()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int, v3 int)");

        executeNet(CURRENT, "INSERT INTO %s (pk, v1, v2, v3) VALUES (1, 2, 3, 4)");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsWritten.getCount());
        assertEquals(3, ClientRequestSizeMetrics.totalColumnsWritten.getCount());
    }

    @Test
    public void shouldRecordWriteMetricsForBatch() throws Exception
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int)");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, CURRENT))
        {
            client.connect(false);

            String first = String.format("INSERT INTO %s.%s (pk, v1, v2) VALUES (1, 10, 100)", KEYSPACE, currentTable());
            String second = String.format("INSERT INTO %s.%s (pk, v1, v2) VALUES (2, 20, 200)", KEYSPACE, currentTable());

            List<List<ByteBuffer>> values = ImmutableList.of(Collections.emptyList(), Collections.emptyList());
            BatchMessage batch = new BatchMessage(BatchStatement.Type.LOGGED, ImmutableList.of(first, second), values, QueryOptions.DEFAULT);
            client.execute(batch);

            // The metrics should reflect the batch as a single write operation with multiple rows and columns.
            assertEquals(2, ClientRequestSizeMetrics.totalRowsWritten.getCount());
            assertEquals(4, ClientRequestSizeMetrics.totalColumnsWritten.getCount());
        }
    }

    @Test
    public void shouldRecordWriteMetricsForCellDeletes()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int, v3 int)");

        executeNet(CURRENT, "DELETE v1, v2, v3 FROM %s WHERE pk = 1");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsWritten.getCount());
        assertEquals(3, ClientRequestSizeMetrics.totalColumnsWritten.getCount());
    }

    @Test
    public void shouldRecordWriteMetricsForCellNulls()
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int, v3 int)");

        executeNet(CURRENT, "INSERT INTO %s (pk, v1, v2, v3) VALUES (1, null, null, null)");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsWritten.getCount());
        assertEquals(3, ClientRequestSizeMetrics.totalColumnsWritten.getCount());
    }

    @Test
    public void shouldRecordWriteMetricsForSingleStaticInsert()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v0 int static, v1 int, v2 int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v0, v1, v2) VALUES (0, 1, 2, 3, 4)");

        assertEquals(2, ClientRequestSizeMetrics.totalRowsWritten.getCount());
        assertEquals(3, ClientRequestSizeMetrics.totalColumnsWritten.getCount());
    }

    @Test
    public void shouldRecordWriteMetricsForBatchedStaticInserts() throws Exception
    {
        createTable("CREATE TABLE %s (pk int, ck int, v0 int static, v1 int, v2 int, PRIMARY KEY (pk, ck))");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, CURRENT))
        {
            client.connect(false);

            String first = String.format("INSERT INTO %s.%s (pk, ck, v0, v1, v2) VALUES (0, 1, 2, 3, 4)", KEYSPACE, currentTable());
            String second = String.format("INSERT INTO %s.%s (pk, ck, v0, v1, v2) VALUES (0, 2, 3, 5, 6)", KEYSPACE, currentTable());

            List<List<ByteBuffer>> values = ImmutableList.of(Collections.emptyList(), Collections.emptyList());
            BatchMessage batch = new BatchMessage(BatchStatement.Type.LOGGED, ImmutableList.of(first, second), values, QueryOptions.DEFAULT);
            client.execute(batch);

            // Two normal rows and the single static row:
            assertEquals(3, ClientRequestSizeMetrics.totalRowsWritten.getCount());
            // Two normal columns per insert, and then one columns for the static row:
            assertEquals(5, ClientRequestSizeMetrics.totalColumnsWritten.getCount());
        }
    }

    @Test
    public void shouldRecordWriteMetricsForRowDelete()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v0 int static, v1 int, v2 int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "DELETE FROM %s WHERE pk = 1 AND ck = 1");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsWritten.getCount());
        // The columns metric should account for all regular columns.
        assertEquals(2, ClientRequestSizeMetrics.totalColumnsWritten.getCount());
    }

    @Test
    public void shouldRecordWriteMetricsForRangeDelete()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v0 int static, v1 int, v2 int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "DELETE FROM %s WHERE pk = 1 AND ck > 1");

        // The range delete is intended to delete at least one row, but that is only a lower bound.
        assertEquals(1, ClientRequestSizeMetrics.totalRowsWritten.getCount());
        // The columns metric should account for all regular columns.
        assertEquals(2, ClientRequestSizeMetrics.totalColumnsWritten.getCount());
    }

    @Test
    public void shouldRecordWriteMetricsForPartitionDelete()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v0 int static, v1 int, v2 int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "DELETE FROM %s WHERE pk = 1");

        // A partition deletion intends to delete at least one row.
        assertEquals(1, ClientRequestSizeMetrics.totalRowsWritten.getCount());
        // If we delete one row, we intended to delete all its regular and static columns.
        assertEquals(3, ClientRequestSizeMetrics.totalColumnsWritten.getCount());
    }

    @Test
    public void shouldRecordWriteMetricsForIntraRowBatch() throws Exception
    {
        createTable("CREATE TABLE %s (pk int, ck int, v1 int, v2 int, PRIMARY KEY (pk, ck))");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, CURRENT))
        {
            client.connect(false);

            String first = String.format("INSERT INTO %s.%s (pk, ck, v1, v2) VALUES (1, 2, 3, 4)", KEYSPACE, currentTable());
            String second = String.format("DELETE FROM %s.%s WHERE pk = 1 AND ck > 1", KEYSPACE, currentTable());

            List<List<ByteBuffer>> values = ImmutableList.of(Collections.emptyList(), Collections.emptyList());
            BatchMessage batch = new BatchMessage(BatchStatement.Type.LOGGED, ImmutableList.of(first, second), values, QueryOptions.DEFAULT);
            client.execute(batch);

            // Both operations affect the same row, but writes and deletes are distinct.
            assertEquals(2, ClientRequestSizeMetrics.totalRowsWritten.getCount());
            assertEquals(4, ClientRequestSizeMetrics.totalColumnsWritten.getCount());
        }
    }

    @Test
    public void shouldRecordWriteMetricsForIfNotExistsV1() throws Exception
    {
        Paxos.setPaxosVariant(Config.PaxosVariant.v1);
        shouldRecordWriteMetricsForIfNotExists();
    }

    @Test
    public void shouldRecordWriteMetricsForIfNotExistsV2() throws Exception
    {
        Paxos.setPaxosVariant(Config.PaxosVariant.v2);
        shouldRecordWriteMetricsForIfNotExists();
    }

    public void shouldRecordWriteMetricsForIfNotExists() throws Exception
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int, v3 int)");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, CURRENT))
        {
            client.connect(false);
            client.execute(new QueryMessage(String.format("INSERT INTO %s.%s (pk, v1, v2, v3) VALUES (1, 2, 3, 4) IF NOT EXISTS", KEYSPACE, currentTable()), QueryOptions.DEFAULT));

            assertEquals(1, ClientRequestSizeMetrics.totalRowsWritten.getCount());
            assertEquals(3, ClientRequestSizeMetrics.totalColumnsWritten.getCount());

            // We read internally, but don't reflect that in our read metrics.
            assertEquals(0, ClientRequestSizeMetrics.totalRowsRead.getCount());
            assertEquals(0, ClientRequestSizeMetrics.totalColumnsRead.getCount());
        }
    }

    @Test
    public void shouldRecordWriteMetricsForCASV1() throws Exception
    {
        Paxos.setPaxosVariant(Config.PaxosVariant.v1);
        shouldRecordWriteMetricsForCAS();
    }

    @Test
    public void shouldRecordWriteMetricsForCASV2() throws Exception
    {
        Paxos.setPaxosVariant(Config.PaxosVariant.v2);
        shouldRecordWriteMetricsForCAS();
    }

    public void shouldRecordWriteMetricsForCAS() throws Exception
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int)");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, CURRENT))
        {
            client.connect(false);
            client.execute(new QueryMessage(String.format("INSERT INTO %s.%s (pk, v1, v2) VALUES (1, 2, 3)", KEYSPACE, currentTable()), QueryOptions.DEFAULT));

            assertEquals(1, ClientRequestSizeMetrics.totalRowsWritten.getCount());
            assertEquals(2, ClientRequestSizeMetrics.totalColumnsWritten.getCount());

            client.execute(new QueryMessage(String.format("UPDATE %s.%s SET v2 = 4 WHERE pk = 1 IF v1 = 2", KEYSPACE, currentTable()), QueryOptions.DEFAULT));

            assertEquals(3, ClientRequestSizeMetrics.totalColumnsWritten.getCount());

            // We read internally, but don't reflect that in our read metrics.
            assertEquals(0, ClientRequestSizeMetrics.totalRowsRead.getCount());
            assertEquals(0, ClientRequestSizeMetrics.totalColumnsRead.getCount());
        }
    }

    @Test
    public void shouldNotRecordWriteMetricsForFailedCASV1() throws Exception
    {
        Paxos.setPaxosVariant(Config.PaxosVariant.v1);
        shouldNotRecordWriteMetricsForFailedCAS();
    }

    @Test
    public void shouldNotRecordWriteMetricsForFailedCASV2() throws Exception
    {
        Paxos.setPaxosVariant(Config.PaxosVariant.v2);
        shouldNotRecordWriteMetricsForFailedCAS();
    }

    public void shouldNotRecordWriteMetricsForFailedCAS() throws Exception
    {
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, v1 int, v2 int)");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, CURRENT))
        {
            client.connect(false);
            client.execute(new QueryMessage(String.format("INSERT INTO %s.%s (pk, v1, v2) VALUES (1, 2, 3)", KEYSPACE, currentTable()), QueryOptions.DEFAULT));

            assertEquals(1, ClientRequestSizeMetrics.totalRowsWritten.getCount());
            assertEquals(2, ClientRequestSizeMetrics.totalColumnsWritten.getCount());

            client.execute(new QueryMessage(String.format("UPDATE %s.%s SET v2 = 4 WHERE pk = 1 IF v1 = 4", KEYSPACE, currentTable()), QueryOptions.DEFAULT));

            // We didn't actually write anything, so don't reflect a write against the metrics.
            assertEquals(2, ClientRequestSizeMetrics.totalColumnsWritten.getCount());

            // Don't reflect in our read metrics the result returned to the client by the failed CAS write. 
            assertEquals(0, ClientRequestSizeMetrics.totalRowsRead.getCount());
            assertEquals(0, ClientRequestSizeMetrics.totalColumnsRead.getCount());
        }
    }

    @Test
    public void shouldRecordReadMetricsOnSerialReadV1() throws Exception
    {
        Paxos.setPaxosVariant(Config.PaxosVariant.v1);
        shouldRecordReadMetricsOnSerialRead();
    }

    @Test
    public void shouldRecordReadMetricsOnSerialReadV2() throws Exception
    {
        Paxos.setPaxosVariant(Config.PaxosVariant.v2);
        shouldRecordReadMetricsOnSerialRead();
    }

    public void shouldRecordReadMetricsOnSerialRead() throws Exception
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        try (SimpleClient client = new SimpleClient(nativeAddr.getHostAddress(), nativePort, CURRENT))
        {
            client.connect(false);
            client.execute(new QueryMessage(String.format("INSERT INTO %s.%s (pk, ck, v) VALUES (1, 1, 1)", KEYSPACE, currentTable()), QueryOptions.DEFAULT));

            QueryMessage query = new QueryMessage(String.format("SELECT * FROM %s.%s WHERE pk = 1 AND ck = 1", KEYSPACE, currentTable()),
                                                  QueryOptions.forInternalCalls(ConsistencyLevel.SERIAL, Collections.emptyList()));
            client.execute(query);

            assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());

            // Both the partition key and clustering key are provided by the client in the request.
            assertEquals(1, ClientRequestSizeMetrics.totalColumnsRead.getCount());
        }
    }

    @Test
    public void shouldRecordReadMetricsForGlobalIndexQuery()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX on %s (v)");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (2, 2, 2)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE v = 1");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The index search term is provided by the client in the request, so we don't consider that column read.
        assertEquals(2, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsForPartitionRestrictedIndexQuery()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX on %s (v)");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE pk = 1 AND v = 1");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The partition key and index search term are provided by the client, so we don't consider those columns read.
        assertEquals(1, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsForClusteringKeyIndexQuery()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX on %s (ck)");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 2, 2)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE ck = 2");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The index search term is provided by the client in the request, so we don't consider that column read.
        assertEquals(2, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsForFilteringQuery()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (2, 2, 2)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE v = 1 ALLOW FILTERING");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The filtering term is provided by the client in the request, so we don't consider that column read.
        assertEquals(2, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsForRangeFilteringQuery()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (2, 2, 2)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE v > 1 ALLOW FILTERING");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The value column is restricted over a range, not bound to a particular value.
        assertEquals(3, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsForINFilteringQuery()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v int, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, 1)");
        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (2, 2, 2)");
        executeNet(CURRENT, "SELECT * FROM %s WHERE v IN (1) ALLOW FILTERING");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The filtering term is provided by the client in the request, so we don't consider that column read.
        assertEquals(2, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }

    @Test
    public void shouldRecordReadMetricsForContainsQuery()
    {
        createTable("CREATE TABLE %s (pk int, ck int, v set<int>, PRIMARY KEY (pk, ck))");

        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (1, 1, {1, 2, 3} )");
        executeNet(CURRENT, "INSERT INTO %s (pk, ck, v) VALUES (2, 2, {4, 5, 6})");
        executeNet(CURRENT, "SELECT * FROM %s WHERE v CONTAINS 1 ALLOW FILTERING");

        assertEquals(1, ClientRequestSizeMetrics.totalRowsRead.getCount());
        // The filtering term is provided by the client in the request, so we don't consider that column read.
        assertEquals(3, ClientRequestSizeMetrics.totalColumnsRead.getCount());
    }
}
