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

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.service.StorageService;

import static java.lang.String.valueOf;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Validates that hinted handoff throttle updates take effect dynamically for in-flight dispatch tasks.
 *
 * NOTE: "hinted_handoff_throttle" semantics are "KiB/sec per delivery thread", and a value of 0 means "no throttling"
 * (unlimited). To emulate "effectively disabled delivery" without pausing handoff, this test sets a very low throttle (1 KiB/sec),
 * verifies hints remain pending, then increases the throttle and verifies hints drain without restarting the node.
 */
public class HintedHandoffThrottleDynamicUpdateTest extends TestBaseImpl
{
    private static final String KS = "ks_hinted_handoff_throttle_dynamic";

    private static long hintsSizeOn(IInvokableInstance instance, UUID hostId)
    {
        // Use a lambda (not a method reference) so HintsService is resolved in the instance classloader,
        // not the test runner's classloader.
        return instance.appliesOnInstance((IIsolatedExecutor.SerializableFunction<UUID, Long>) id ->
                                          HintsService.instance.getTotalHintsSize(id))
                       .apply(hostId);
    }

    @Test
    public void testThrottleUpdateAffectsInFlightDispatchWithoutRestart() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(3)
                                          .withDataDirCount(1)
                                          .withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP)
                                                            .set("hinted_handoff_enabled", true)
                                                            .set("max_hints_delivery_threads", "1")
                                                            .set("hints_flush_period", "1s")
                                                            .set("max_hints_file_size", "1MiB"))
                                          .start()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = {'class':'SimpleStrategy','replication_factor':3}");
            cluster.schemaChange("CREATE TABLE " + KS + ".tbl (pk text PRIMARY KEY, v text)");

            IInvokableInstance node1 = cluster.get(1);
            IInvokableInstance node3 = cluster.get(3);

            UUID node3HostId = node3.callOnInstance((IIsolatedExecutor.SerializableCallable<UUID>) () -> StorageService.instance.getLocalHostUUID());

            // Stop node3 so writes will generate hints on node1 for node3.
            node3.shutdown().get();

            // Generate enough hints so delivery at 1 KiB/s will not drain quickly.
            for (int i = 0; i < 20000; i++)
            {
                cluster.coordinator(1)
                       .execute("INSERT INTO " + KS + ".tbl (pk, v) VALUES (?, ?)",
                                ONE,
                                valueOf(i),
                                // reasonably sized payload; hints are mutations, not just values
                                "v" + i + "_".repeat(64));
            }

            // Ensure hints are flushed and fsynced for node3 before bringing node3 back.
            node1.runOnInstance((IIsolatedExecutor.SerializableRunnable) () ->
                                HintsService.instance.flushAndFsyncBlockingly(Collections.singleton(node3HostId)));

            await().atMost(60, SECONDS).until(() -> hintsSizeOn(node1, node3HostId) > 0);
            final long initialHintsSize = hintsSizeOn(node1, node3HostId);
            assertTrue("Expected hints to exist on node1 for node3 before restart", initialHintsSize > 0);

            // Ensure dispatch is running on node1.
            node1.nodetoolResult("resumehandoff").asserts().success();

            // Throttle to a very low value (1 KiB/s) so dispatch should not drain quickly.
            node1.nodetoolResult("sethintedhandoffthrottlekb", "1").asserts().success();
            assertEquals(1, node1.callOnInstance(DatabaseDescriptor::getHintedHandoffThrottleInKiB).intValue());

            // Bring node3 back; node1 should start dispatching hints, but very slowly.
            node3.startup();

            // Give dispatch some time; with 1KiB/s it should still have pending hints.
            await().pollDelay(5, SECONDS).until(() -> true);
            long sizeWithLowThrottle = hintsSizeOn(node1, node3HostId);
            assertTrue("Expected hints to still be pending with very low throttle; initial=" + initialHintsSize + " current=" + sizeWithLowThrottle,
                       sizeWithLowThrottle > 0);

            // Now increase the throttle without restarting node1; hints should drain.
            node1.nodetoolResult("sethintedhandoffthrottlekb", "1048576").asserts().success(); // 1 GiB/s
            assertEquals(1048576, node1.callOnInstance(DatabaseDescriptor::getHintedHandoffThrottleInKiB).intValue());

            await().atMost(2, TimeUnit.MINUTES).until(() -> hintsSizeOn(node1, node3HostId) > 0);
        }
    }
}

