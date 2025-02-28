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
package org.apache.cassandra.db.monitoring;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.regex.Pattern;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import junit.framework.Assert;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.schema.IndexTarget;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.MonitoringService;
import org.apache.cassandra.transport.ProtocolVersion;

@RunWith(Parameterized.class)
public class BadQueryTest extends CQLTester
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";
    private static final String INDEX = "v_index";

    private static TableMetadata cfm;
    private static ColumnMetadata v;
    private static ColumnMetadata s;
    private static ProtocolVersion protocolVersion = ProtocolVersion.V4;
    private static IBadQueryReporter bq;
    ColumnFamilyStore cfs;
    long originalBadQueryWriteMaxPartitionSizeInbytes;
    long originalBadQueryReadMaxPartitionSizeInbytes;
    private boolean tracingEnabled;

    public BadQueryTest(String badQueryReporter, boolean tracing)
    {
        requireNetwork();
        tracingEnabled = tracing;
        DatabaseDescriptor.setBadQueryTracingStatus(tracing);
        DatabaseDescriptor.setBadQueryReporter(badQueryReporter);
        bq = DatabaseDescriptor.getBadQueryReporter();
        BadQuery.setup();
    }

    @Parameters()
    public static Collection<Object[]> generateData()
    {
        return Arrays.asList(new Object[][]{ {"BadQueriesInSystemLog", true},
                                             {"BadQueriesInTable", true},
                                             {"BadQueriesInSystemLog", false},
                                             {"BadQueriesInTable", false} });
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        TableMetadata.Builder builder = TableMetadata.builder(KEYSPACE, TABLE)
                                                     .addPartitionKeyColumn("k", UTF8Type.instance)
                                                     .addStaticColumn("s", UTF8Type.instance)
                                                     .addClusteringColumn("i", IntegerType.instance)
                                                     .addRegularColumn("v", UTF8Type.instance);

        Indexes.Builder indexes = Indexes.builder();
        indexes.add(IndexMetadata.fromIndexTargets(Collections.singletonList(
                                                   new IndexTarget(new ColumnIdentifier("v", true),
                                                                   IndexTarget.Type.VALUES)),
                                                   INDEX,
                                                   IndexMetadata.Kind.COMPOSITES,
                                                   Collections.EMPTY_MAP));
        builder.indexes(indexes.build());
        cfm = builder.build();

        SchemaLoader.prepareServer();
        SchemaLoader.createKeyspace(KEYSPACE, KeyspaceParams.simple(1), cfm);
        cfm = Schema.instance.getTableMetadata(KEYSPACE, TABLE);
        v = cfm.getColumn(new ColumnIdentifier("v", true));
        s = cfm.getColumn(new ColumnIdentifier("s", true));
    }

    @Before
    public void truncate() throws Throwable
    {
        try
        {
            executeNet(protocolVersion, String.format("CREATE MATERIALIZED VIEW %s.view1 AS SELECT k, v, i FROM %s.%s WHERE v IS NOT NULL AND i IS NOT NULL AND k IS NOT NULL PRIMARY KEY (v, k, i)", KEYSPACE, KEYSPACE, TABLE));
        }
        catch (Throwable e)
        {
            // View already exists
        }
        MonitoringService.instance.setBadQueryTracingFraction(1.0);
        MonitoringService.instance.getBadQueryIgnoreKeyspaces().clear();
        cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(TABLE);
        cfs.truncateBlocking();
        bq.clearUnsafe(true);
        // clear metric for large parition reads and writes
        cfs.metric.largePartitionReadSize.dec(cfs.metric.largePartitionReadSize.getCount());
        cfs.metric.largePartitionWriteSize.dec(cfs.metric.largePartitionWriteSize.getCount());
        originalBadQueryWriteMaxPartitionSizeInbytes = MonitoringService.instance.getBadQueryWriteMaxPartitionSizeInbytes();
        originalBadQueryReadMaxPartitionSizeInbytes = MonitoringService.instance.getBadQueryReadMaxPartitionSizeInbytes();
    }

    @After
    public void restoreSetup()
    {
        MonitoringService.instance.setBadQueryWriteMaxPartitionSizeInbytes(originalBadQueryWriteMaxPartitionSizeInbytes);
        MonitoringService.instance.setBadQueryReadMaxPartitionSizeInbytes(originalBadQueryReadMaxPartitionSizeInbytes);
    }

    private void executeCQL()
    {
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s, i, v) VALUES ('k', 's', 1, 'v')");
        QueryProcessor.executeInternal("SELECT s FROM ks.tbl WHERE k='k'");
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    private void executeCQLMV()
    {
        QueryProcessor.executeInternal("SELECT * FROM ks.view1 WHERE v='k'");
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    @Test
    public void testLargeWrites()
    {
        MonitoringService.instance.setBadQueryWriteMaxPartitionSizeInbytes(0);
        executeCQL();
        if (tracingEnabled)
        {
            Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.LARGE_PARTITION_WRITE).size() > 0);
            Assert.assertTrue(cfs.metric.largePartitionWriteSize.getCount() > 0);
        }
        else
        {
            Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.LARGE_PARTITION_WRITE).size() == 0);
            Assert.assertTrue(cfs.metric.largePartitionWriteSize.getCount() == 0);
        }
    }

    @Test
    public void testLargeReads()
    {
        MonitoringService.instance.setBadQueryReadMaxPartitionSizeInbytes(0);
        executeCQL();
        if (tracingEnabled)
        {
            Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.LARGE_PARTITION_READ).size() > 0);
            Assert.assertTrue(cfs.metric.largePartitionReadSize.getCount() > 0);
        }
        else
        {
            Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.LARGE_PARTITION_READ).size() == 0);
            Assert.assertTrue(cfs.metric.largePartitionReadSize.getCount() == 0);
        }
    }

    @Test
    public void testSlowRead()
    {
        MonitoringService.instance.setBadQueryReadSlowLocalLatencyInms(Integer.MIN_VALUE);
        executeCQL();
        if (tracingEnabled)
            Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.SLOW_READ_LOCAL).size() > 0);
        else
            Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.SLOW_READ_LOCAL).size() == 0);
    }

    @Test
    public void testIgnoreKeyspacePattern()
    {
        if (!tracingEnabled)
            return;
        MonitoringService.instance.setBadQueryIgnoreKeyspacesPattern(Pattern.compile("system.*|.*staging.*|.*test.*|health|pingless|" + KEYSPACE));
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.SLOW_READ_LOCAL).size() == 0);
        // below is same as testSlowReadWithTracingEnabled, if keypsace is not ignored, the count will increase
        MonitoringService.instance.setBadQueryReadSlowLocalLatencyInms(Integer.MIN_VALUE);
        executeCQL();
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.SLOW_READ_LOCAL).size() == 0);
    }

    @Test
    public void testIgnoreKeyspaceUpdateWithKeyspaceCreateAndDrop()
    {
        if (!tracingEnabled)
            return;
        MonitoringService.instance.setBadQueryIgnoreKeyspacesPattern(Pattern.compile("test.*"));
        String keyspace = "test_keyspace";
        Assert.assertFalse(MonitoringService.instance.getBadQueryIgnoreKeyspaces().contains(keyspace));

        TableMetadata tableMetadata = TableMetadata.builder(keyspace, TABLE)
                                       .addPartitionKeyColumn("k", UTF8Type.instance)
                                       .addStaticColumn("s", UTF8Type.instance)
                                       .addClusteringColumn("i", IntegerType.instance)
                                       .addRegularColumn("v", UTF8Type.instance)
                                       .build();
        // create
        SchemaLoader.createKeyspace(keyspace, KeyspaceParams.simple(1), tableMetadata);
        Assert.assertTrue(MonitoringService.instance.getBadQueryIgnoreKeyspaces().contains(keyspace));

        schemaChange(String.format("DROP KEYSPACE %s", keyspace));
        Assert.assertFalse(MonitoringService.instance.getBadQueryIgnoreKeyspaces().contains(keyspace));
    }

    @Test
    public void testTooManyTombstones()
    {
        MonitoringService.instance.setBadQueryTombstoneLimit(Integer.MIN_VALUE);
        executeCQL();
        if (tracingEnabled)
            Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TOO_MANY_TOMBSTONES).size() > 0);
        else
            Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TOO_MANY_TOMBSTONES).size() == 0);
    }

    @Test
    public void testMVIsExperimental()
    {
        executeCQLMV();
        if (tracingEnabled)
            Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.MV_IN_USE).size() > 0);
        else
            Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.MV_IN_USE).size() == 0);
    }

    @Test
    public void testPreparedCacheOverflow() throws Throwable
    {
        if (!tracingEnabled)
            return;
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.PREPARED_CACHE_OVERFLOW).size() == 0);
        BadQuery.checkForPreparedCacheOverflow(0);
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.PREPARED_CACHE_OVERFLOW).size() == 0);
        BadQuery.checkForPreparedCacheOverflow(1000);
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.PREPARED_CACHE_OVERFLOW).size() > 0);
    }

    @Test
    public void testTierMismatch()
    {
        if (!tracingEnabled)
            return;
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TIER_MISMATCH).size() == 0);

        System.setProperty("cassandra.db_tier", "5");
        BadQuery.checkForTierMismatch("4", "");
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TIER_MISMATCH).size() == 1);

        System.setProperty("cassandra.db_tier", "1");
        BadQuery.checkForTierMismatch("0", "");
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TIER_MISMATCH).size() == 2);

        System.setProperty("cassandra.db_tier", "4");
        BadQuery.checkForTierMismatch("4", "");
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TIER_MISMATCH).size() == 2);

        System.setProperty("cassandra.db_tier", "3");
        BadQuery.checkForTierMismatch("4", "");
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TIER_MISMATCH).size() == 2);

        System.setProperty("cassandra.db_tier", "-1");
        BadQuery.checkForTierMismatch("4", "");
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TIER_MISMATCH).size() == 2);

        System.setProperty("cassandra.db_tier", "3");
        BadQuery.checkForTierMismatch("-1", "");
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TIER_MISMATCH).size() == 2);

        System.setProperty("cassandra.db_tier", "invalid tier");
        BadQuery.checkForTierMismatch("3", "");
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TIER_MISMATCH).size() == 2);

        System.setProperty("cassandra.db_tier", "3");
        BadQuery.checkForTierMismatch("invalid tier", "");
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TIER_MISMATCH).size() == 2);
    }

    @Test
    public void testNonExistsCFSForLargeReadsAndWrites()
    {
        if (!tracingEnabled)
            return;
        // in some edge cases, the CFS might be dropped and after enqueued and null is returned as the CFS objects
        LargePartition op = new LargePartition(TableMetadata.builder(KEYSPACE, "nonexist")
                                                            .addPartitionKeyColumn("k", IntegerType.instance)
                                                            .addRegularColumn("v", IntegerType.instance)
                                                            .id(TableId.generate()).build(),
                                               "col",
                                               100);
        BadQuery.report(BadQuery.BadQueryCategory.LARGE_PARTITION_READ, op);
        BadQuery.report(BadQuery.BadQueryCategory.LARGE_PARTITION_WRITE, op);
    }
}
