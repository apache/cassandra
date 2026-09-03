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

package org.apache.cassandra.distributed.test.guardrails;

import java.io.IOException;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.guardrails.Guardrails;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.exceptions.TooManySSTablesReadAbortException;
import org.apache.cassandra.service.ClientWarn;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;

import static java.lang.String.format;
import static org.apache.cassandra.db.ConsistencyLevel.ALL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * Distributed test for the {@code sstables_per_read} guardrail (CASSANDRA-21478): a single local read
 * that touches more than the warn/fail number of SSTables warns the client (warn) or aborts the read
 * on the replica and rejects it to the client (fail). Internal reads are never aborted.
 *
 * The keyspace is recreated before each test so the per-keyspace warn/abort metrics are isolated.
 */
public class GuardrailSSTablesPerReadTest extends TestBaseImpl
{
    private static final String TABLE = "tbl";

    private static Cluster cluster;

    @BeforeClass
    public static void setupCluster() throws IOException
    {
        cluster = Cluster.build(2)
                         .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                           .set("read_thresholds_enabled", "true"))
                         .start();
    }

    @AfterClass
    public static void teardownCluster()
    {
        if (cluster != null)
            cluster.close();
    }

    @Before
    public void beforeEachTest()
    {
        cluster.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(cluster);
        cluster.schemaChange(format("CREATE TABLE %s.%s (k int, c int, v int, PRIMARY KEY (k, c))", KEYSPACE, TABLE));
        cluster.forEach(i -> i.acceptsOnInstance((IIsolatedExecutor.SerializableBiConsumer<String, String>) (ks, tb) ->
            Keyspace.open(ks).getColumnFamilyStore(tb).disableAutoCompaction()).accept(KEYSPACE, TABLE));
    }

    @Test
    public void testSinglePartitionWarnThreshold()
    {
        setThresholds(5, -1);

        // 3 SSTables for partition 0 -> below warn (5)
        flushPartition(0, 3);
        assertNull(clientReadWarnings(format("SELECT * FROM %s.%s WHERE k = 0", KEYSPACE, TABLE)));
        assertWarnAborts(0, 0);

        // 3 more SSTables for partition 0 -> 6 > warn (5)
        flushPartition(0, 3);
        List<String> warnings = clientReadWarnings(format("SELECT * FROM %s.%s WHERE k = 0", KEYSPACE, TABLE));
        assertNotNull(warnings);
        assertThat(warnings.toString()).contains("2 nodes")
                                       .contains("SSTables in a single local read")
                                       .contains("sstables_per_read_warn_threshold");
        assertWarnAborts(1, 0);
    }

    @Test
    public void testSinglePartitionFailThreshold()
    {
        setThresholds(2, 5);

        // 6 SSTables for partition 0 -> above fail (5)
        flushPartition(0, 6);
        String abortMessage = clientReadAbortMessage(format("SELECT * FROM %s.%s WHERE k = 0", KEYSPACE, TABLE));
        assertNotNull("expected the read to be aborted", abortMessage);
        assertThat(abortMessage).contains("SSTables in a single local read")
                                .contains("aborted")
                                .contains("sstables_per_read_fail_threshold");
        // a failing read must not also emit a warning
        assertWarnAborts(0, 1);
    }

    @Test
    public void testRangeReadWarnThreshold()
    {
        setThresholds(5, -1);

        // 6 distinct partitions, each in its own SSTable -> a full range scan touches 6 > warn (5)
        for (int i = 0; i < 6; i++)
            flushPartition(i, 1);

        List<String> warnings = clientReadWarnings(format("SELECT * FROM %s.%s", KEYSPACE, TABLE));
        assertNotNull(warnings);
        assertThat(warnings.toString()).contains("2 nodes")
                                       .contains("SSTables in a single local read")
                                       .contains("sstables_per_read_warn_threshold");
        assertWarnAborts(1, 0);
    }

    @Test
    public void testRangeReadFailThreshold()
    {
        setThresholds(2, 5);

        // 6 distinct partitions, each in its own SSTable -> a full range scan touches 6 > fail (5)
        for (int i = 0; i < 6; i++)
            flushPartition(i, 1);

        String abortMessage = clientReadAbortMessage(format("SELECT * FROM %s.%s", KEYSPACE, TABLE));
        assertNotNull("expected the range read to be aborted", abortMessage);
        assertThat(abortMessage).contains("SSTables in a single local read")
                                .contains("aborted")
                                .contains("sstables_per_read_fail_threshold");
        // a failing read must not also emit a warning
        assertWarnAborts(0, 1);
    }

    @Test
    public void testRuntimeThresholdChange()
    {
        setThresholds(2, -1);
        flushPartition(0, 6); // 6 SSTables for partition 0 -> above warn (2)

        String query = format("SELECT * FROM %s.%s WHERE k = 0", KEYSPACE, TABLE);

        assertNotNull(clientReadWarnings(query));
        assertWarnAborts(1, 0);

        // raise the warn threshold above the count at runtime -> the guardrail stops firing
        setThresholds(100, -1);
        assertNull(clientReadWarnings(query));
        assertWarnAborts(1, 0);

        // lower it again -> it fires once more
        setThresholds(2, -1);
        assertNotNull(clientReadWarnings(query));
        assertWarnAborts(2, 0);
    }

    @Test
    public void testCompactionClearsCondition()
    {
        setThresholds(5, -1);
        flushPartition(0, 6); // 6 SSTables for partition 0 -> above warn (5)

        String query = format("SELECT * FROM %s.%s WHERE k = 0", KEYSPACE, TABLE);

        assertNotNull(clientReadWarnings(query));
        assertWarnAborts(1, 0);

        // compacting merges the 6 SSTables into 1, dropping below the threshold -> no more warnings
        compactAll();
        assertNull(clientReadWarnings(query));
        assertWarnAborts(1, 0);
    }

    @Test
    public void testInternalReadNotAborted()
    {
        setThresholds(2, 5);

        // 6 SSTables for partition 0 -> above fail (5)
        flushPartition(0, 6);

        // An internal read (no warning tracking) must never be aborted, regardless of the fail threshold.
        Object[][] result = cluster.get(1).executeInternal(format("SELECT * FROM %s.%s WHERE k = 0", KEYSPACE, TABLE));
        assertNotNull(result);
        // internal reads never warn or abort through the guardrail
        assertWarnAborts(0, 0);
    }

    /** Inserts {@code count} rows into {@code partition}, flushing after each so each becomes its own SSTable. */
    private void flushPartition(int partition, int count)
    {
        for (int i = 0; i < count; i++)
        {
            cluster.coordinator(1).execute(format("INSERT INTO %s.%s (k, c, v) VALUES (?, ?, ?)", KEYSPACE, TABLE),
                                           ConsistencyLevel.ALL, partition, i, i);
            cluster.forEach(instance -> instance.flush(KEYSPACE));
        }
    }

    /** Force-major-compacts the table on every node, merging the per-partition SSTables. */
    private void compactAll()
    {
        cluster.forEach(i -> i.acceptsOnInstance((IIsolatedExecutor.SerializableBiConsumer<String, String>) (ks, tb) ->
            Keyspace.open(ks).getColumnFamilyStore(tb).forceMajorCompaction()).accept(KEYSPACE, TABLE));
    }

    private void setThresholds(int warn, int fail)
    {
        cluster.stream().forEach(instance ->
            instance.acceptsOnInstance((IIsolatedExecutor.SerializableBiConsumer<Integer, Integer>)
                                       (w, f) -> Guardrails.instance.setSSTablesPerReadThreshold(w, f)).accept(warn, fail));
    }

    private void assertWarnAborts(int warns, int aborts)
    {
        assertEquals(warns, totalWarnings());
        assertEquals(aborts, totalAborts());
    }

    private long totalWarnings()
    {
        return cluster.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.SSTablesPerReadWarnings." + KEYSPACE)).sum();
    }

    private long totalAborts()
    {
        return cluster.stream().mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.SSTablesPerReadAborts." + KEYSPACE)).sum();
    }

    /** Runs a client read (with warning tracking) on node 1; returns the client warnings, or null if none. */
    private List<String> clientReadWarnings(String query)
    {
        return cluster.get(1).callsOnInstance((IIsolatedExecutor.SerializableCallable<List<String>>) () -> {
            ClientWarn.instance.captureWarnings();
            CoordinatorWarnings.init();
            try
            {
                QueryProcessor.execute(query, ALL, QueryState.forInternalCalls());
            }
            finally
            {
                CoordinatorWarnings.done();
                CoordinatorWarnings.reset();
            }
            return ClientWarn.instance.getWarnings();
        }).call();
    }

    /** Runs a client read expected to abort; returns the abort message, or null if it did not abort.
     *  The exception is caught inside the instance to avoid crossing the classloader boundary. */
    private String clientReadAbortMessage(String query)
    {
        return cluster.get(1).callsOnInstance((IIsolatedExecutor.SerializableCallable<String>) () -> {
            ClientWarn.instance.captureWarnings();
            CoordinatorWarnings.init();
            try
            {
                QueryProcessor.execute(query, ALL, QueryState.forInternalCalls());
                return null;
            }
            catch (TooManySSTablesReadAbortException e)
            {
                return e.getMessage();
            }
            finally
            {
                CoordinatorWarnings.done();
                CoordinatorWarnings.reset();
            }
        }).call();
    }
}
