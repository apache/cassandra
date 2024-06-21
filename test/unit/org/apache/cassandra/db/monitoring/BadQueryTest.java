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
import java.util.List;
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
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MonitoringService;
import org.apache.cassandra.transport.ProtocolVersion;

@RunWith(Parameterized.class)
public class BadQueryTest extends CQLTester
{
    private static final String KEYSPACE = "ks";
    private static final String TABLE = "tbl";

    private static TableMetadata cfm;
    private static ColumnMetadata v;
    private static ColumnMetadata s;
    private static ProtocolVersion protocolVersion = ProtocolVersion.V4;
    private static IBadQueryReporter bq;
    ColumnFamilyStore cfs;
    long originalBadQueryWriteMaxPartitionSizeInbytes;
    long originalBadQueryReadMaxPartitionSizeInbytes;

    public BadQueryTest(String badQueryReporter)
    {
        requireNetwork();
        DatabaseDescriptor.setBadQueryReporter(badQueryReporter);
        bq = DatabaseDescriptor.getBadQueryReporter();
        BadQuery.setup();
    }

    @Parameters()
    public static List<String> generateData()
    {
        return Arrays.asList("BadQueriesInSystemLog", "BadQueriesInTable");
    }

    @BeforeClass
    public static void defineSchema() throws ConfigurationException
    {
        cfm = TableMetadata.builder(KEYSPACE, TABLE)
                                .addPartitionKeyColumn("k", UTF8Type.instance)
                                .addStaticColumn("s", UTF8Type.instance)
                                .addClusteringColumn("i", IntegerType.instance)
                                .addRegularColumn("v", UTF8Type.instance)
                                .build();

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
        QueryProcessor.executeInternal("INSERT INTO ks.tbl (k, s) VALUES ('k', 's')");
        QueryProcessor.executeInternal("SELECT s FROM ks.tbl WHERE k='k'");
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    private void executeCQLMV()
    {
        QueryProcessor.executeInternal("SELECT * FROM ks.view1 WHERE v='k'");
        cfs.forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
    }

    @Test
    public void testLargeWritesWithTracingEnabled()
    {
        DatabaseDescriptor.setBadQueryTracingStatus(true);
        MonitoringService.instance.setBadQueryWriteMaxPartitionSizeInbytes(0);
        executeCQL();
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.LARGE_PARTITION_WRITE).size() > 0);
        Assert.assertTrue(cfs.metric.largePartitionWriteSize.getCount() > 0);
    }

    @Test
    public void testLargeReadsWithTracingEnabled()
    {
        DatabaseDescriptor.setBadQueryTracingStatus(true);

        MonitoringService.instance.setBadQueryReadMaxPartitionSizeInbytes(0);
        executeCQL();
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.LARGE_PARTITION_READ).size() > 0);
        Assert.assertTrue(cfs.metric.largePartitionReadSize.getCount() > 0);
    }

    @Test
    public void testSlowReadWithTracingEnabled()
    {
        DatabaseDescriptor.setBadQueryTracingStatus(true);

        MonitoringService.instance.setBadQueryReadSlowLocalLatencyInms(Integer.MIN_VALUE);
        executeCQL();
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.SLOW_READ_LOCAL).size() > 0);
    }

    @Test
    public void testIgnoreKeyspacePattern()
    {
        DatabaseDescriptor.setBadQueryTracingStatus(true);
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
        DatabaseDescriptor.setBadQueryTracingStatus(true);
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
    public void testTooManyTombstonesWithTracingEnabled()
    {
        DatabaseDescriptor.setBadQueryTracingStatus(true);

        MonitoringService.instance.setBadQueryTombstoneLimit(Integer.MIN_VALUE);
        executeCQL();
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TOO_MANY_TOMBSTONES).size() > 0);
    }

    @Test
    public void testLargeWritesWithTracingDisabled()
    {
        DatabaseDescriptor.setBadQueryTracingStatus(false);

        MonitoringService.instance.setBadQueryWriteMaxPartitionSizeInbytes(0);
        executeCQL();
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.LARGE_PARTITION_WRITE).size() == 0);
        Assert.assertTrue(cfs.metric.largePartitionWriteSize.getCount() == 0);
    }

    @Test
    public void testLargeReadsWithTracingDisabled()
    {
        DatabaseDescriptor.setBadQueryTracingStatus(false);

        MonitoringService.instance.setBadQueryReadMaxPartitionSizeInbytes(0);
        executeCQL();
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.LARGE_PARTITION_READ).size() == 0);
        Assert.assertTrue(cfs.metric.largePartitionReadSize.getCount() == 0);
    }

    @Test
    public void testSlowReadWithTracingDisabled()
    {
        DatabaseDescriptor.setBadQueryTracingStatus(false);

        MonitoringService.instance.setBadQueryReadSlowLocalLatencyInms(Integer.MIN_VALUE);
        executeCQL();
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.SLOW_READ_LOCAL).size() == 0);
    }

    @Test
    public void testTooManyTombstonesWithTracingDisabled()
    {
        DatabaseDescriptor.setBadQueryTracingStatus(false);

        MonitoringService.instance.setBadQueryTombstoneLimit(Integer.MIN_VALUE);
        executeCQL();
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.TOO_MANY_TOMBSTONES).size() == 0);
    }

    @Test
    public void testMVIsExperimentalWithTracingDisabled()
    {
        DatabaseDescriptor.setBadQueryTracingStatus(false);
        executeCQLMV();
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.MV_IN_USE).size() == 0);
    }

    @Test
    public void testMVIsExperimentalWithTracingEnabled()
    {
        DatabaseDescriptor.setBadQueryTracingStatus(true);
        executeCQLMV();
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.MV_IN_USE).size() > 0);
    }

    @Test
    public void testPreparedCacheOverflow() throws Throwable
    {
        DatabaseDescriptor.setBadQueryTracingStatus(true);
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.PREPARED_CACHE_OVERFLOW).size() == 0);
        BadQuery.checkForPreparedCacheOverflow(0);
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.PREPARED_CACHE_OVERFLOW).size() == 0);
        BadQuery.checkForPreparedCacheOverflow(1000);
        Assert.assertTrue(bq.getBadQueryCategoryQueues().get(BadQuery.BadQueryCategory.PREPARED_CACHE_OVERFLOW).size() > 0);
    }
}
