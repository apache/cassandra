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

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.service.StorageService;

import static java.lang.String.format;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@SuppressWarnings("Convert2MethodRef")
public class HintsPersistentWindowTest extends AbstractHintWindowTest
{
    @Test
    public void testPersistentHintWindow() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                       .set("hinted_handoff_enabled", true)
                                                                       .set("max_hints_delivery_threads", "1")
                                                                       .set("hints_flush_period", "1s")
                                                                       .set("max_hint_window", "30s")
                                                                       .set("max_hints_file_size", "10MiB"))
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
            waitUntilNodeState(node1, node2UUID, false);

            Long totalHintsAfterFirstShutdown = insertData(cluster);
            Long totalHitsSizeAfterFirstShutdown = getTotalHintsSize(node1, node2UUID);

            // check hints are there etc
            assertHintsSizes(node1, node2UUID);

            pauseHintsDelivery(node1);

            // wait to pass max_hint_window
            Thread.sleep(60000);

            // start the second node, this will not deliver hints to it from the first because dispatch is paused
            // we need this in order to keep hints still on disk, so we can check that the oldest hint
            // is older than max_hint_window which will not deliver any hints even the node is not down long enough
            node2.startup();
            waitUntilNodeState(node1, node2UUID, true);

            Long totalHitsSizeAfterSecondShutdown = getTotalHintsSize(node1, node2UUID);
            assertEquals(totalHitsSizeAfterFirstShutdown, totalHitsSizeAfterSecondShutdown);

            // stop the node again
            // boolean hintWindowExpired = endpointDowntime > maxHintWindow will be false
            // then persistent window kicks in, because even it has not expired,
            // there are hints to be delivered on the disk which were stil not dispatched
            node2.shutdown().get();

            Long totalHintsAfterSecondShutdown = insertData(cluster);

            assertNotEquals(0L, (long) getTotalHintsSize(node1, node2UUID));
            assertEquals(totalHintsAfterFirstShutdown, totalHintsAfterSecondShutdown);
        }
    }
}
