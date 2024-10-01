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

package org.apache.cassandra.distributed.test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import harry.core.Configuration;
import harry.core.Run;
import harry.ddl.ColumnSpec;
import harry.ddl.SchemaSpec;
import harry.model.QuiescentChecker;
import harry.model.clock.OffsetClock;
import harry.operations.Query;
import harry.visitors.MutatingRowVisitor;
import harry.visitors.MutatingVisitor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.fuzz.FixedSchemaProviderConfiguration;
import org.apache.cassandra.distributed.fuzz.HarryHelper;
import org.apache.cassandra.distributed.fuzz.InJvmSut;

import org.awaitility.Awaitility;

import static org.junit.Assert.assertEquals;

public class UnifiedCompactionHarryTest extends TestBaseImpl
{
    @Test
    public void testReconcileSinglePartitionSSTables() throws Exception
    {
        final long seed = 21615L;
        System.out.println("Harry seed: " + seed);
        final int partitionCount = 64;
        final int visits = 2 * partitionCount;
        final String table = "tiny_harry";
        HarryHelper.init();
        // UnifiedCompactionDensitiesTest covers repair and trigger selection; focus here on reconciliation.
        Cluster cluster = init(builder().withNodes(1).withDataDirCount(1).start(), 1);
        InJvmSut sut = new InJvmSut(cluster);
        try
        {
            SchemaSpec schema = new SchemaSpec(KEYSPACE, table,
                                                Collections.singletonList(ColumnSpec.pk("pk", ColumnSpec.int64Type)),
                                                Collections.singletonList(ColumnSpec.ck("ck", ColumnSpec.int64Type, false)),
                                                Arrays.asList(ColumnSpec.regularColumn("v", ColumnSpec.int64Type),
                                                              ColumnSpec.regularColumn("text_value", ColumnSpec.asciiType)),
                                                Collections.singletonList(ColumnSpec.staticColumn("s", ColumnSpec.int64Type)));
            cluster.schemaChange(schema.compile().cql());
            // Keep every input in one level and shard so complete compaction must produce one SSTable.
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + '.' + table + " WITH compaction = " +
                                 "{'class':'UnifiedCompactionStrategy', 'scaling_parameters':'T4', " +
                                 "'max_sstables_per_shard_factor':'1', 'base_shard_count':'1', " +
                                 "'sstable_growth':'1', 'flush_size_override':'1GiB'}");
            cluster.forEach(node -> node.nodetool("disableautocompaction", KEYSPACE, table));

            Run run = HarryHelper.defaultConfiguration()
                                 .setSeed(seed)
                                 .setClock(() -> new OffsetClock(10000L))
                                 .setSchemaProvider(new FixedSchemaProviderConfiguration(schema))
                                 .setPartitionDescriptorSelector(new Configuration.DefaultPDSelectorConfiguration(partitionCount, 2))
                                 .setClusteringDescriptorSelector(HarryHelper.defaultClusteringDescriptorSelectorConfiguration()
                                                                           .setMaxPartitionSize(4).build())
                                 .setCreateSchema(false)
                                 .setSUT(() -> sut)
                                 .build().createRun();
            Set<Long> partitions = new LinkedHashSet<>();
            run.tracker.onLtsStarted(lts -> partitions.add(run.pdSelector.pd(lts, schema)));
            MutatingVisitor writer = new MutatingVisitor(run, MutatingRowVisitor::new);
            for (int i = 0; i < visits; ++i)
            {
                long pd = run.pdSelector.pd(i, schema);
                assertEquals("seed=" + seed + ": each partition must be revisited after its first flush",
                             i < partitionCount, !partitions.contains(pd));
                writer.visit();
                cluster.get(1).flush(KEYSPACE);
            }
            assertEquals(partitionCount, partitions.size());
            assertEquals(visits, (int) cluster.get(1).callOnInstance(() ->
                Keyspace.open(KEYSPACE).getColumnFamilyStore(table).getLiveSSTables().size()));
            QuiescentChecker checker = new QuiescentChecker(run);
            for (long pd : partitions)
                checker.validate(Query.selectPartition(schema, pd, false));

            cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).enableAutoCompaction(true));
            Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals("seed=" + seed + ": all inputs must be compacted", 1,
                             (int) cluster.get(1).callOnInstance(() ->
                                 Keyspace.open(KEYSPACE).getColumnFamilyStore(table).getLiveSSTables().size())));
            for (long pd : partitions)
            {
                checker.validate(Query.selectPartition(schema, pd, false));
                checker.validate(Query.selectPartition(schema, pd, true));
            }
        }
        finally
        {
            sut.shutdown();
        }
    }
}
