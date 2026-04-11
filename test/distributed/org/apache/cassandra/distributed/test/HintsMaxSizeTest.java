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

import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;

import static java.lang.String.format;
import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@SuppressWarnings("Convert2MethodRef")
public class HintsMaxSizeTest extends TestBaseImpl
{
    @Test
    public void testMaxHintedHandoffSize() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                       .set("hinted_handoff_enabled", true)
                                                                       .set("max_hints_delivery_threads", "1")
                                                                       .set("hints_flush_period", "1s")
                                                                       .set("max_hints_size_per_host", "2MiB")
                                                                       .set("max_hints_file_size", "1MiB"))
                                           .start(), 2))
        {
            final IInvokableInstance node1 = cluster.get(1);
            final IInvokableInstance node2 = cluster.get(2);

            waitForExistingRoles(cluster);

            String createTableStatement = format("CREATE TABLE %s.cf (k text PRIMARY KEY, c1 text) " +
                                                 "WITH compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': 'false'} ", KEYSPACE);
            cluster.schemaChange(createTableStatement);

            UUID node2UUID = node2.callOnInstance((IIsolatedExecutor.SerializableCallable<UUID>) () -> StorageService.instance.getLocalHostUUID());

            // shutdown the second node in a blocking manner
            node2.shutdown().get();

            // insert data and sleep every 10k to have a chance to flush hints
            for (int i = 0; i < 70000; i++)
            {
                cluster.coordinator(1)
                       .execute(withKeyspace("INSERT INTO %s.cf (k, c1) VALUES (?, ?);"),
                                ONE, valueOf(i), UUID.randomUUID().toString());

                if (i % 10000 == 0)
                    await().atLeast(2, SECONDS).pollDelay(2, SECONDS).until(() -> true);
            }

            await().atLeast(3, SECONDS).pollDelay(3, SECONDS).until(() -> true);

            // we see that metrics are updated
            await().until(() -> node1.callOnInstance(() -> StorageMetrics.totalHints.getCount()) > 0);
            // we do not have 100k of hints because it violated max_hints_size_per_host
            assertThat(node1.callOnInstance(() -> StorageMetrics.totalHints.getCount())).isLessThan(70000);

            assertHintsSizes(node1, node2UUID);

            // restart the first node and check hints sizes again
            // to be sure sizes were picked up correctly
            node1.shutdown().get();
            node1.startup();

            assertHintsSizes(node1, node2UUID);
        }
    }

    private void assertHintsSizes(IInvokableInstance node, UUID node2UUID)
    {
        // we indeed have some hints in its dir
        File hintsDir = new File(node.config().getString("hints_directory"));
        assertThat(FileUtils.folderSize(hintsDir)).isPositive();

        // and there is positive size of hints on the disk for the second node
        long totalHintsSize = node.appliesOnInstance((IIsolatedExecutor.SerializableFunction<UUID, Long>) secondNode -> {
            return HintsService.instance.getTotalHintsSize(secondNode);
        }).apply(node2UUID);

        assertThat(totalHintsSize).isPositive();
        // there might be small overflow in general, depending on hints flushing etc
        assertThat(totalHintsSize).isLessThan(4 * 1000 * 1000);
        assertThat(totalHintsSize).isGreaterThan(2 * 1000 * 1000);
    }

    private static void waitForExistingRoles(Cluster cluster)
    {
        cluster.forEach(instance -> {
            await().pollDelay(1, SECONDS)
                   .pollInterval(1, SECONDS)
                   .atMost(60, SECONDS)
                   .until(() -> instance.callOnInstance(CassandraRoleManager::hasExistingRoles));
        });
    }
}
