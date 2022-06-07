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
package org.apache.cassandra.distributed.test.repair;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.util.concurrent.Uninterruptibles;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import com.carrotsearch.hppc.LongArrayList;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.QueryResults;
import org.apache.cassandra.distributed.api.SimpleQueryResult;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.FBUtilities;
import org.assertj.core.api.Assertions;

public class ForceRepairTest extends TestBaseImpl
{
    /**
     * Port of python dtest "repair_tests/incremental_repair_test.py::TestIncRepair::test_force" but extends to test
     * all types of repair.
     */
    @Test
    public void force() throws IOException
    {
        force(false);
    }

    @Test
    public void forceWithDifference() throws IOException
    {
        force(true);
    }

    private void force(boolean includeDifference) throws IOException
    {
        long nowInMicro = System.currentTimeMillis() * 1000;
        try (Cluster cluster = Cluster.build(3)
                                      .withConfig(c -> c.set("hinted_handoff_enabled", false)
                                                        .with(Feature.values()))
                                      .start())
        {
            init(cluster);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k INT PRIMARY KEY, v INT)"));

            for (int i = 0; i < 10; i++)
                cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (k,v) VALUES (?, ?) USING TIMESTAMP ?"), ConsistencyLevel.ALL, i, i, nowInMicro++);

            String downAddress = cluster.get(2).callOnInstance(() -> FBUtilities.getBroadcastAddressAndPort().getHostAddressAndPort());
            ClusterUtils.stopUnchecked(cluster.get(2));
            cluster.get(1).runOnInstance(() -> {
                InetAddressAndPort neighbor;
                try
                {
                    neighbor = InetAddressAndPort.getByName(downAddress);
                }
                catch (UnknownHostException e)
                {
                    throw new RuntimeException(e);
                }
                while (FailureDetector.instance.isAlive(neighbor))
                    Uninterruptibles.sleepUninterruptibly(500, TimeUnit.MILLISECONDS);
            });


            // repair should fail because node2 is down
            IInvokableInstance node1 = cluster.get(1);

            for (String[] args : Arrays.asList(new String[]{ "--full" },
                                               new String[]{ "--full", "--preview" },
                                               new String[]{ "--full", "--validate"}, // nothing should be in the repaired set, so shouldn't stream
                                               new String[]{ "--preview" }, // IR Preview
                                               new String[]{ "--validate"}, // nothing should be in the repaired set, so shouldn't stream
                                               new String[0])) // IR
            {
                if (includeDifference)
                    node1.executeInternal(withKeyspace("INSERT INTO %s.tbl (k,v) VALUES (?, ?) USING TIMESTAMP ?"), -1, -1, nowInMicro++); // each loop should have a different timestamp, causing a new difference

                try
                {
                    node1.nodetoolResult(ArrayUtils.addAll(new String[] {"repair", KEYSPACE}, args)).asserts().failure();
                    node1.nodetoolResult(ArrayUtils.addAll(new String[] {"repair", KEYSPACE, "--force"}, args)).asserts().success();

                    assertNoRepairedAt(cluster);
                }
                catch (Exception | Error e)
                {
                    // tag the error to include which args broke
                    e.addSuppressed(new AssertionError("Failure for args: " + Arrays.toString(args)));
                    throw e;
                }
            }

            if (includeDifference)
            {
                SimpleQueryResult expected = QueryResults.builder()
                                                         .row(-1, -1)
                                                         .build();
                for (IInvokableInstance node : Arrays.asList(node1, cluster.get(3)))
                {
                    SimpleQueryResult results = node.executeInternalWithResult(withKeyspace("SELECT * FROM %s.tbl WHERE k=?"), -1);
                    expected.reset();
                    AssertUtils.assertRows(results, expected);
                }
            }
        }
    }

    private static void assertNoRepairedAt(Cluster cluster)
    {
        List<long[]> repairedAt = getRepairedAt(cluster, KEYSPACE, "tbl");
        Assertions.assertThat(repairedAt).hasSize(cluster.size());
        for (int i = 0; i < repairedAt.size(); i++)
        {
            long[] array = repairedAt.get(i);
            if (array == null)
            {
                // ignore downed nodes
                Assertions.assertThat(cluster.get(i + 1).isShutdown()).isTrue();
                continue;
            }
            Assertions.assertThat(array).isNotEmpty();
            for (long a : array)
                Assertions.assertThat(a).describedAs("node%d had a repaired sstable", i + 1).isEqualTo(0);
        }
    }

    private static List<long[]> getRepairedAt(Cluster cluster, String keyspace, String table)
    {
        return cluster.stream().map(i -> {
            if (i.isShutdown())
                return null;

            return i.callOnInstance(() -> {
                TableMetadata meta = Schema.instance.getTableMetadata(keyspace, table);
                ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(meta.id);

                View view = cfs.getTracker().getView();
                LongArrayList list = new LongArrayList();
                for (SSTableReader sstable : view.liveSSTables())
                {
                    try
                    {
                        StatsMetadata metadata = sstable.getSSTableMetadata();
                        list.add(metadata.repairedAt);
                    }
                    catch (Exception e)
                    {
                        // ignore
                    }
                }
                return list.toArray();
            });
        }).collect(Collectors.toList());
    }
}
