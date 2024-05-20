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
import static org.junit.Assert.assertTrue;

@SuppressWarnings("Convert2MethodRef")
public class HintsMaxWindowTest extends AbstractHintWindowTest
{
    @Test
    public void testHintsKeepRecordingAfterNodeGoesOfflineRepeatedly() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withDataDirCount(1)
                                           .withConfig(config -> config.with(NETWORK, GOSSIP)
                                                                       .set("hinted_handoff_enabled", true)
                                                                       .set("max_hints_delivery_threads", "1")
                                                                       .set("hints_flush_period", "1s")
                                                                       .set("max_hint_window", "30s")
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
            waitUntilNodeState(node1, node2UUID, false);

            Long totalHintsAfterFirstShutdown = insertData(cluster);

            // check hints are there etc
            assertHintsSizes(node1, node2UUID);

            // start the second node, this will deliver hints to it from the first
            node2.startup();
            waitUntilNodeState(node1, node2UUID, true);

            waitUntilNoHints(node1, node2UUID);
            assertEquals(totalHintsAfterFirstShutdown, getTotalHintsCount(node1));

            // all hints were delivered, so lets take that node down again to see if hints
            // delivery works after node goes down for the second time
            node2.shutdown().get();
            waitUntilNodeState(node1, node2UUID, false);

            // insert again, this will increase the number of total hints sent
            Long totalHintsAfterSecondShutdown = insertData(cluster);

            assertHintsSizes(node1, node2UUID);

            // the fact it is greater than 0 means that we created new hints again
            // after we stopped the second node for the second time
            assertTrue(totalHintsAfterSecondShutdown - totalHintsAfterFirstShutdown > 0);

            // wait to pass max_hint_window
            Thread.sleep(35000);

            // we should not hint anything because the node was down more than max_hint_window
            Long totalHintsAfterThirdShutdown = insertData(cluster);

            // so we have not created any hints because we have passed max_hint_window
            assertEquals(0, totalHintsAfterThirdShutdown - totalHintsAfterSecondShutdown);
        }
    }

}
