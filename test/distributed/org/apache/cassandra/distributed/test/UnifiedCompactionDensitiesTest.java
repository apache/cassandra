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

import java.io.IOException;
import java.util.LongSummaryStatistics;

import org.junit.Test;

import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.hamcrest.Matchers;

import static org.apache.cassandra.cql3.TombstonesWithIndexedSSTableTest.makeRandomString;
import static org.junit.Assert.assertThat;

public class UnifiedCompactionDensitiesTest extends TestBaseImpl
{
    @Test
    public void testTargetSSTableSize1Node1Dir() throws IOException
    {
        testTargetSSTableSize(1, 1);
    }

    @Test
    public void testTargetSSTableSize1Node2Dirs() throws IOException
    {
        testTargetSSTableSize(1, 2);
    }

    @Test
    public void testTargetSSTableSize2Nodes1Dir() throws IOException
    {
        testTargetSSTableSize(2, 1);
    }

    @Test
    public void testTargetSSTableSize2Nodes3Dirs() throws IOException
    {
        testTargetSSTableSize(2, 3);
    }

    private void testTargetSSTableSize(int nodeCount, int dataDirs) throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(nodeCount)
                                             .withDataDirCount(dataDirs)
                                             .withConfig(cfg -> cfg.set("memtable_heap_space", "100MiB"))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("alter keyspace %s with replication = {'class': 'SimpleStrategy', 'replication_factor':1}"));
            cluster.schemaChange(withKeyspace("create table %s.tbl (id bigint primary key, value text) with compaction = {'class':'UnifiedCompactionStrategy', " +
                                                                                                                                    "'target_sstable_size' : '1MiB', " +
                                                                                                                                    "'min_sstable_size' : '0B', " +
                                                                                                                                    "'sstable_growth': '0'}"));
            long targetSize = 1L<<20;
            long targetMin = targetSize * 10 / 16;  // Size must be within sqrt(0.5), sqrt(2) of target, use 1.6 to account for estimations
            long targetMax = targetSize * 16 / 10;
            long toWrite = targetSize * nodeCount * dataDirs * 8; // 8 MiB per data directory, to be guaranteed to be over the 1MiB target size, and also different from the base shard count
            int payloadSize = 1024;
            cluster.forEach(x -> x.nodetool("disableautocompaction"));

            // The first flush will not have the flush size metric initialized, so first check distribution after compaction.
            int i = 0;
            for (; i < 2; ++i)
            {
                writeData(cluster, i * toWrite, toWrite, payloadSize);
                cluster.forEach(x -> x.flush(KEYSPACE));
            }

            cluster.forEach(x -> x.forceCompact(KEYSPACE, "tbl"));
            checkSSTableSizes(nodeCount, cluster, targetMin, targetMax);

            // Now check that the sstables created by flushes are of the right size.
            for (; i < 2; ++i)
            {
                writeData(cluster, i * toWrite, toWrite, payloadSize);
                cluster.forEach(x -> x.flush(KEYSPACE));
            }
            checkSSTableSizes(nodeCount, cluster, targetMin, targetMax);

            // Compact again, as this time there will be independent buckets whose splitting must also work correctly.
            cluster.forEach(x -> x.forceCompact(KEYSPACE, "tbl"));
            checkSSTableSizes(nodeCount, cluster, targetMin, targetMax);
        }
    }

    private static void writeData(Cluster cluster, long offset, long toWrite, int payloadSize)
    {
        for (int i = 0; i < toWrite; i += payloadSize)
            cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id, value) values (?, ?)"), ConsistencyLevel.ONE, i + offset, makeRandomString(payloadSize));
    }

    private void checkSSTableSizes(int nodeCount, Cluster cluster, long targetMin, long targetMax)
    {
        for (int i = 1; i <= nodeCount; ++i)
        {
            LongSummaryStatistics stats = cluster.get(i).callOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                return cfs.getLiveSSTables().stream().mapToLong(SSTableReader::onDiskLength).summaryStatistics();
            });
            long sstableCount = stats.getCount();
            long minSize = stats.getMin();
            long maxSize = stats.getMax();

            LoggerFactory.getLogger(getClass()).info("Node {} sstables {} min/max size: {}/{} avg {} total {}",
                                                     i,
                                                     sstableCount,
                                                     FBUtilities.prettyPrintMemory(minSize),
                                                     FBUtilities.prettyPrintMemory(maxSize),
                                                     FBUtilities.prettyPrintBinary(stats.getAverage(), "", "B"),
                                                     FBUtilities.prettyPrintMemory(stats.getSum()));
            assertThat(sstableCount, Matchers.greaterThan(0L));
            assertThat(minSize, Matchers.greaterThan(targetMin));
            assertThat(maxSize, Matchers.lessThan(targetMax));
        }
    }
}
