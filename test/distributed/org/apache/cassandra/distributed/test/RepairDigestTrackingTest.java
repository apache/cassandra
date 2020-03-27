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
import java.io.Serializable;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.StorageProxy;

public class RepairDigestTrackingTest extends TestBaseImpl implements Serializable// TODO: why serializable?
{

    @Test
    public void testInconsistenciesFound() throws Throwable
    {
        try (Cluster cluster = (Cluster) init(builder().withNodes(2).start()))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
            });

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k INT, c INT, v INT, PRIMARY KEY (k,c)) with read_repair='NONE'");
            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (?, ?, ?)",
                                               ConsistencyLevel.ALL,
                                               i, i, i);
            }

            cluster.get(1).runOnInstance(() ->
               Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").forceBlockingFlush()
            );
            cluster.get(2).runOnInstance(() ->
               Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").forceBlockingFlush()
            );

            for (int i = 10; i < 20; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (?, ?, ?)",
                                               ConsistencyLevel.ALL,
                                               i, i, i);
            }

            cluster.get(1).runOnInstance(() ->
                                         Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").forceBlockingFlush()
            );
            cluster.get(2).runOnInstance(() ->
                                         Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").forceBlockingFlush()
            );

            cluster.get(1).runOnInstance(() ->
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().forEach(this::assertNotRepaired)
            );
            cluster.get(2).runOnInstance(() ->
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().forEach(this::assertNotRepaired)
            );

            cluster.get(2).runOnInstance(() ->
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().forEach(this::markRepaired)
            );


            cluster.get(1).executeInternal("INSERT INTO " + KEYSPACE + ".tbl (k, c, v) VALUES (?, ?, ?)", 5, 5, 55);
            cluster.get(1).runOnInstance(() ->
              Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().forEach(this::assertNotRepaired)
            );
            cluster.get(2).runOnInstance(() ->
              Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").getLiveSSTables().forEach(this::assertRepaired)
            );

            long ccBefore = cluster.get(1).callOnInstance(() ->
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric.confirmedRepairedInconsistencies.table.getCount()
            );

            cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl", ConsistencyLevel.ALL);
            long ccAfter = cluster.get(1).callOnInstance(() ->
                Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metric.confirmedRepairedInconsistencies.table.getCount()
            );

            Assert.assertEquals("confirmed count should differ by 1 after range read", ccBefore + 1, ccAfter);
        }
    }

    @Test
    public void testPurgeableTombstonesAreIgnored() throws Throwable
    {
        try (Cluster cluster = (Cluster) init(builder().withNodes(2).start()))
        {

            cluster.get(1).runOnInstance(() -> {
                StorageProxy.instance.enableRepairedDataTrackingForRangeReads();
            });

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl2 (k INT, c INT, v1 INT, v2 INT, PRIMARY KEY (k,c)) WITH gc_grace_seconds=0");
            // on node1 only insert some tombstones, then flush
            for (int i = 0; i < 10; i++)
            {
                cluster.get(1).executeInternal("DELETE v1 FROM " + KEYSPACE + ".tbl2 USING TIMESTAMP 0 WHERE k=? and c=? ", i, i);
            }
            cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl2").forceBlockingFlush());

            // insert data on both nodes and flush
            for (int i = 0; i < 10; i++)
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl2 (k, c, v2) VALUES (?, ?, ?) USING TIMESTAMP 1",
                                               ConsistencyLevel.ALL,
                                               i, i, i);
            }
            cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl2").forceBlockingFlush());
            cluster.get(2).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl2").forceBlockingFlush());

            // nothing is repaired yet
            cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl2").getLiveSSTables().forEach(this::assertNotRepaired));
            cluster.get(2).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl2").getLiveSSTables().forEach(this::assertNotRepaired));
            // mark everything repaired
            cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl2").getLiveSSTables().forEach(this::markRepaired));
            cluster.get(2).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl2").getLiveSSTables().forEach(this::markRepaired));
            cluster.get(1).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl2").getLiveSSTables().forEach(this::assertRepaired));
            cluster.get(2).runOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl2").getLiveSSTables().forEach(this::assertRepaired));

            // now overwrite on node2 only to generate digest mismatches, but don't flush so the repaired dataset is not affected
            for (int i = 0; i < 10; i++)
            {
                cluster.get(2).executeInternal("INSERT INTO " + KEYSPACE + ".tbl2 (k, c, v2) VALUES (?, ?, ?) USING TIMESTAMP 2", i, i, i * 2);
            }

            long ccBefore = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl2").metric.confirmedRepairedInconsistencies.table.getCount());
            // Unfortunately we need to sleep here to ensure that nowInSec > the local deletion time of the tombstones
            TimeUnit.SECONDS.sleep(2);
            cluster.coordinator(1).execute("SELECT * FROM " + KEYSPACE + ".tbl2", ConsistencyLevel.ALL);
            long ccAfter = cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl2").metric.confirmedRepairedInconsistencies.table.getCount());

            Assert.assertEquals("No repaired data inconsistencies should be detected", ccBefore, ccAfter);
        }
    }

    private void assertNotRepaired(SSTableReader reader) {
        Assert.assertTrue("repaired at is set for sstable: " + reader.descriptor, getRepairedAt(reader) == ActiveRepairService.UNREPAIRED_SSTABLE);
    }

    private void assertRepaired(SSTableReader reader) {
        Assert.assertTrue("repaired at is not set for sstable: " + reader.descriptor, getRepairedAt(reader) > 0);
    }

    private long getRepairedAt(SSTableReader reader)
    {
        Descriptor descriptor = reader.descriptor;
        try
        {
            Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer()
                                                                      .deserialize(descriptor, EnumSet.of(MetadataType.STATS));

            StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
            return stats.repairedAt;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    private void markRepaired(SSTableReader reader) {
        Descriptor descriptor = reader.descriptor;
        try
        {
            descriptor.getMetadataSerializer().mutateRepairMetadata(descriptor, System.currentTimeMillis(), null, false);
            reader.reloadSSTableMetadata();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
