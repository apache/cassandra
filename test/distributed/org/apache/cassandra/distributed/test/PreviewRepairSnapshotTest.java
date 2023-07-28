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
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.concurrent.Refs;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class PreviewRepairSnapshotTest extends TestBaseImpl
{
    /**
     * Makes sure we only snapshot sstables containing the mismatching token
     * <p>
     * 1. create 100 sstables per instance, compaction disabled, one token per sstable
     * 2. make 3 tokens mismatching on node2, one token per sstable
     * 3. run preview repair
     * 4. make sure that only the sstables containing the token are in the snapshot
     */
    @Test
    public void testSnapshotOfSStablesContainingMismatchingTokens() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(config ->
                                                                config.set("snapshot_on_repaired_data_mismatch", true)
                                                                      .with(GOSSIP)
                                                                      .with(NETWORK)).start()))
        {
            Set<Integer> tokensToMismatch = Sets.newHashSet(1, 50, 99);
            cluster.schemaChange(withKeyspace("create table %s.tbl (id int primary key) with compaction = {'class' : 'SizeTieredCompactionStrategy', 'enabled':false }"));
            // 1 token per sstable;
            for (int i = 0; i < 100; i++)
            {
                cluster.coordinator(1).execute(withKeyspace("insert into %s.tbl (id) values (?)"), ConsistencyLevel.ALL, i);
                cluster.stream().forEach(instance -> instance.flush(KEYSPACE));
            }
            cluster.stream().forEach(instance -> instance.flush(KEYSPACE));
            for (int i = 1; i <= 2; i++)
                markRepaired(cluster, i);

            cluster.get(1)
                   .nodetoolResult("repair", "-vd", "-pr", KEYSPACE, "tbl")
                   .asserts()
                   .success()
                   .stdoutContains("Repaired data is in sync");

            Set<Token> mismatchingTokens = new HashSet<>();
            for (Integer token : tokensToMismatch)
            {
                cluster.get(2).executeInternal(withKeyspace("insert into %s.tbl (id) values (?)"), token);
                cluster.get(2).flush(KEYSPACE);
                Object[][] res = cluster.get(2).executeInternal(withKeyspace("select token(id) from %s.tbl where id = ?"), token);
                mismatchingTokens.add(new Murmur3Partitioner.LongToken((long) res[0][0]));
            }

            markRepaired(cluster, 2);

            cluster.get(1)
                   .nodetoolResult("repair", "-vd", KEYSPACE, "tbl")
                   .asserts()
                   .success()
                   .stdoutContains("Repaired data is inconsistent");

            cluster.get(1).runOnInstance(checkSnapshot(mismatchingTokens, 3));
            // node2 got the duplicate mismatch-tokens above, so it should exist in exactly 6 sstables
            cluster.get(2).runOnInstance(checkSnapshot(mismatchingTokens, 6));
        }
    }

    private IIsolatedExecutor.SerializableRunnable checkSnapshot(Set<Token> mismatchingTokens, int expectedSnapshotSize)
    {
        return () -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");

            String snapshotTag = await().atMost(1, MINUTES)
                                        .pollInterval(100, MILLISECONDS)
                                        .until(() -> {
                                            for (String tag : cfs.listSnapshots().keySet())
                                            {
                                                // we create the snapshot schema file last, so when this exists we know the snapshot is complete;
                                                if (cfs.getDirectories().getSnapshotSchemaFile(tag).exists())
                                                    return tag;
                                            }

                                            return "";
                                        }, not(emptyString()));

            Set<SSTableReader> inSnapshot = new HashSet<>();

            try (Refs<SSTableReader> sstables = cfs.getSnapshotSSTableReaders(snapshotTag))
            {
                inSnapshot.addAll(sstables);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
            assertEquals(expectedSnapshotSize, inSnapshot.size());

            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                Bounds<Token> sstableBounds = new Bounds<>(sstable.getFirst().getToken(), sstable.getLast().getToken());
                boolean shouldBeInSnapshot = false;
                for (Token mismatchingToken : mismatchingTokens)
                {
                    if (sstableBounds.contains(mismatchingToken))
                    {
                        assertFalse(shouldBeInSnapshot);
                        shouldBeInSnapshot = true;
                    }
                }
                assertEquals(shouldBeInSnapshot, inSnapshot.contains(sstable));
            }
        };
    }

    private void markRepaired(Cluster cluster, int instance)
    {
        cluster.get(instance).runOnInstance(() -> {
            ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
            for (SSTableReader sstable : cfs.getLiveSSTables())
            {
                try
                {
                    sstable.descriptor.getMetadataSerializer().mutateRepairMetadata(sstable.descriptor,
                                                                                    System.currentTimeMillis(),
                                                                                    null,
                                                                                    false);
                    sstable.reloadSSTableMetadata();
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
        });
    }
}
