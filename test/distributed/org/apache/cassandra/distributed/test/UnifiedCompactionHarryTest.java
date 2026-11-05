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
import java.util.concurrent.TimeUnit;

import org.awaitility.Awaitility;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.harry.ColumnSpec;
import org.apache.cassandra.harry.SchemaSpec;
import org.apache.cassandra.harry.dsl.ReplayingHistoryBuilder;
import org.apache.cassandra.harry.execution.InJvmDTestVisitExecutor;
import org.apache.cassandra.harry.op.Operations;
import org.apache.cassandra.harry.util.BitSet;

import static org.apache.cassandra.harry.MagicConstants.UNSET_IDX;
import static org.junit.Assert.assertEquals;

public class UnifiedCompactionHarryTest extends TestBaseImpl
{
    @Test
    public void testReconcileSinglePartitionSSTables() throws Exception
    {
        final long seed = 21615L;
        final int partitionCount = 64;
        final int visits = 2;
        final String table = "tiny_harry";
        // UnifiedCompactionDensitiesTest covers repair and trigger selection; focus here on reconciliation.
        try (Cluster cluster = init(builder().withNodes(1).withDataDirCount(1).start(), 1))
        {
            SchemaSpec schema = new SchemaSpec(seed, 1000, KEYSPACE, table,
                                               Collections.singletonList(ColumnSpec.pk("pk", ColumnSpec.int64Type)),
                                               Collections.singletonList(ColumnSpec.ck("ck", ColumnSpec.int64Type, false)),
                                               Arrays.asList(ColumnSpec.regularColumn("v", ColumnSpec.int64Type),
                                                             ColumnSpec.regularColumn("text_value", ColumnSpec.asciiType)),
                                               Collections.singletonList(ColumnSpec.staticColumn("s", ColumnSpec.int64Type)));
            cluster.schemaChange(schema.compile());
            // Keep every input in one level and shard so complete compaction must produce one SSTable.
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + '.' + table + " WITH compaction = " +
                                 "{'class':'UnifiedCompactionStrategy', 'scaling_parameters':'T4', " +
                                 "'max_sstables_per_shard_factor':'1', 'base_shard_count':'1', " +
                                 "'sstable_growth':'1', 'flush_size_override':'1GiB'}");
            cluster.forEach(node -> node.nodetool("disableautocompaction", KEYSPACE, table));

            ReplayingHistoryBuilder history = new ReplayingHistoryBuilder(schema.valueGenerators,
                                                                          hb -> InJvmDTestVisitExecutor.builder().build(schema, hb, cluster));
            // Each visit writes to one partition and is flushed on its own, so every sstable holds a single partition.
            // The second visit deletes, updates or overwrites what the first wrote, which compaction has to reconcile.
            for (int visit = 0; visit < visits; ++visit)
            {
                for (int pd = 0; pd < partitionCount; ++pd)
                {
                    if (visit == 0)
                    {
                        for (int row = 0; row < 4; ++row)
                            history.insert(pd, row);
                    }
                    else
                    {
                        switch (pd % 12)
                        {
                            case 0:
                                history.insert(pd, 4);
                                history.deleteRow(pd, 1);
                                break;
                            case 1:
                                history.deletePartition(pd);
                                history.insert(pd, 2);
                                break;
                            case 2:
                                history.update(pd, 1);
                                history.update(pd, 4);
                                break;
                            case 3:
                                history.insert(pd, 1);
                                history.insert(pd, 4);
                                break;
                            case 4:
                                history.deleteColumns(pd, 1, BitSet.create(1L, 2), BitSet.allUnset(1));
                                break;
                            case 5:
                                history.deleteColumns(pd, 1, BitSet.create(2L, 2), BitSet.allSet(1));
                                break;
                            case 6:
                                history.deleteRowRange(pd, 1, 2, 0, true, true);
                                break;
                            case 7:
                                history.deleteRowSliceByLowerBound(pd, 1, 0, false);
                                break;
                            case 8:
                                history.deleteRowSliceByUpperBound(pd, 2, 0, false);
                                break;
                            case 9:
                                // Update one cell without overwriting the other regular cell or the statics.
                                history.update(pd, 1, new int[]{ pd, UNSET_IDX }, new int[]{ UNSET_IDX });
                                break;
                            case 10:
                                history.insert(pd, 1, new int[]{ UNSET_IDX, pd }, new int[]{ UNSET_IDX });
                                break;
                            default:
                                history.deleteColumns(pd, 1, BitSet.allUnset(2), BitSet.allSet(1));
                        }
                    }
                    cluster.get(1).flush(KEYSPACE);
                }
            }
            assertEquals(visits * partitionCount, liveSSTableCount(cluster, table));
            for (int pd = 0; pd < partitionCount; ++pd)
                history.selectPartition(pd);

            cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).enableAutoCompaction(true));
            Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() ->
                assertEquals("seed=" + seed + ": all inputs must be compacted", 1, liveSSTableCount(cluster, table)));
            for (int pd = 0; pd < partitionCount; ++pd)
            {
                history.selectPartition(pd);
                history.selectPartition(pd, Operations.ClusteringOrderBy.DESC);
            }
        }
    }

    private static int liveSSTableCount(Cluster cluster, String table)
    {
        return cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore(table).getLiveSSTables().size());
    }
}
