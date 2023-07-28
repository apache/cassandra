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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.OverrideConfigurationLoader;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.EmbeddedCassandraService;
import org.apache.cassandra.service.StorageService;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.config.CassandraRelevantProperties.MEMTABLE_SHARD_COUNT;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(BMUnitRunner.class)
public class TrieMemtableMetricsTest extends SchemaLoader
{
    private static final int NUM_SHARDS = 13;

    private static final Logger logger = LoggerFactory.getLogger(TrieMemtableMetricsTest.class);
    private static Session session;

    private static final String KEYSPACE = "triememtable";
    private static final String TABLE = "metricstest";

    @BeforeClass
    public static void loadSchema() throws ConfigurationException
    {
        // shadow superclass method; we'll call it directly
        // after tinkering with the Config
    }

    @BeforeClass
    public static void setup() throws ConfigurationException, IOException
    {
        OverrideConfigurationLoader.override((config) -> {
            config.partitioner = "Murmur3Partitioner";
        });
        MEMTABLE_SHARD_COUNT.setInt(NUM_SHARDS);

        SchemaLoader.loadSchema();

        EmbeddedCassandraService cassandra = new EmbeddedCassandraService();
        cassandra.start();

        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withPort(DatabaseDescriptor.getNativeTransportPort()).build();
        session = cluster.connect();

        session.execute(String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };", KEYSPACE));
    }

    private ColumnFamilyStore recreateTable()
    {
        return recreateTable(TABLE);
    }

    private ColumnFamilyStore recreateTable(String table)
    {
        session.execute(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, table));
        session.execute(String.format("CREATE TABLE IF NOT EXISTS %s.%s (id int, val1 text, val2 text, PRIMARY KEY(id, val1)) WITH MEMTABLE = 'test_memtable_metrics';", KEYSPACE, table));
        return ColumnFamilyStore.getIfExists(KEYSPACE, table);
    }

    @Test
    public void testRegularStatementsAreCounted()
    {
        ColumnFamilyStore cfs = recreateTable();
        TrieMemtableMetricsView metrics = getMemtableMetrics(cfs);
        assertEquals(0, metrics.contendedPuts.getCount());
        assertEquals(0, metrics.uncontendedPuts.getCount());

        for (int i = 0; i < 10; i++)
        {
            session.execute(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, '%s', '%s')", KEYSPACE, TABLE, i, "val" + i, "val" + i));
        }

        long allPuts = metrics.contendedPuts.getCount() + metrics.uncontendedPuts.getCount();
        assertEquals(10, allPuts);
    }

    @Test
    public void testFlushRelatedMetrics() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = recreateTable();
        TrieMemtableMetricsView metrics = getMemtableMetrics(cfs);

        StorageService.instance.forceKeyspaceFlush(KEYSPACE, TABLE);
        assertEquals(0, metrics.contendedPuts.getCount() + metrics.uncontendedPuts.getCount());

        writeAndFlush(10);
        assertEquals(10, metrics.contendedPuts.getCount() + metrics.uncontendedPuts.getCount());

        // verify that metrics survive flush / memtable switching
        writeAndFlush(10);
        assertEquals(20, metrics.contendedPuts.getCount() + metrics.uncontendedPuts.getCount());
        assertEquals(metrics.lastFlushShardDataSizes.toString(), NUM_SHARDS, metrics.lastFlushShardDataSizes.numSamplesGauge.getValue().intValue());
    }

    @Test
    @BMRules(rules = { @BMRule(name = "Delay memtable update",
    targetClass = "InMemoryTrie",
    targetMethod = "putSingleton",
    action = "java.lang.Thread.sleep(10)")})
    public void testContentionMetrics() throws IOException, ExecutionException, InterruptedException
    {
        ColumnFamilyStore cfs = recreateTable();
        TrieMemtableMetricsView metrics = getMemtableMetrics(cfs);
        assertEquals(0, (int) metrics.lastFlushShardDataSizes.numSamplesGauge.getValue());

        StorageService.instance.forceKeyspaceFlush(KEYSPACE, TABLE);

        writeAndFlush(100);

        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        metrics.contentionTime.latency.getSnapshot().dump(stream);

        assertEquals(100, metrics.contendedPuts.getCount() + metrics.uncontendedPuts.getCount());
        assertThat(metrics.contendedPuts.getCount(), greaterThan(0L));
        assertThat(metrics.contentionTime.totalLatency.getCount(), greaterThan(0L));
    }

    @Test
    public void testMetricsCleanupOnDrop()
    {
        String tableName = TABLE + "_metrics_cleanup";
        CassandraMetricsRegistry registry = CassandraMetricsRegistry.Metrics;
        Supplier<Stream<String>> metrics = () -> registry.getNames().stream().filter(m -> m.contains(tableName));

        // no metrics before creating
        assertEquals(0, metrics.get().count());

        recreateTable(tableName);
        // some metrics
        assertTrue(metrics.get().count() > 0);

        session.execute(String.format("DROP TABLE IF EXISTS %s.%s", KEYSPACE, tableName));
        // no metrics after drop
        assertEquals(metrics.get().collect(Collectors.joining(",")), 0, metrics.get().count());
    }

    private TrieMemtableMetricsView getMemtableMetrics(ColumnFamilyStore cfs)
    {
        return new TrieMemtableMetricsView(cfs.getKeyspaceName(), cfs.name);
    }

    private void writeAndFlush(int rows) throws IOException, ExecutionException, InterruptedException
    {
        logger.info("writing {} rows", rows);
        Future[] futures = new Future[rows];
        for (int i = 0; i < rows; i++)
        {
            logger.info("writing {} row", i);
            futures[i] = session.executeAsync(String.format("INSERT INTO %s.%s (id, val1, val2) VALUES (%d, '%s', '%s')", KEYSPACE, TABLE, i, "val" + i, "val" + i));
        }
        for (int i = 0; i < rows; i++)
        {
            futures[i].get();
            logger.info("writing {} row completed", i);
        }
        logger.info("forcing flush");
        StorageService.instance.forceKeyspaceFlush(KEYSPACE, TABLE);
        logger.info("table flushed");
    }

    @AfterClass
    public static void teardown()
    {
        session.close();
    }
}
