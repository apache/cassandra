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

import org.junit.Ignore;

import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.IIsolatedExecutor;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.service.StorageService;
import org.assertj.core.api.Assertions;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Ignore
public abstract class AbstractHintWindowTest extends TestBaseImpl
{
    void waitUntilNoHints(IInvokableInstance node1, UUID node2UUID)
    {
        await().pollInterval(10, SECONDS)
               .timeout(1, MINUTES)
               .until(() -> getTotalHintsSize(node1, node2UUID) == 0);
    }

    Long getTotalHintsSize(IInvokableInstance node, UUID node2UUID)
    {
        return node.appliesOnInstance((IIsolatedExecutor.SerializableFunction<UUID, Long>) secondNode -> {
            return HintsService.instance.getTotalHintsSize(secondNode);
        }).apply(node2UUID);
    }

    void pauseHintsDelivery(IInvokableInstance node)
    {
        node.runOnInstance((IIsolatedExecutor.SerializableRunnable) () -> {
            HintsService.instance.pauseDispatch();
        });
    }

    void waitUntilNodeState(IInvokableInstance node, UUID node2UUID, boolean shouldBeOnline)
    {
        await().pollInterval(10, SECONDS)
               .timeout(1, MINUTES)
               .until(() -> node.appliesOnInstance((IIsolatedExecutor.SerializableBiFunction<UUID, Boolean, Boolean>) (secondNode, online) -> {
                   InetAddressAndPort address = StorageService.instance.getEndpointForHostId(secondNode);
                   return online == FailureDetector.instance.isAlive(address);
               }).apply(node2UUID, shouldBeOnline));
    }


    Long getTotalHintsCount(IInvokableInstance node)
    {
        return node.callOnInstance(() -> StorageMetrics.totalHints.getCount());
    }

    void assertHintsSizes(IInvokableInstance node, UUID node2UUID)
    {
        // we indeed have some hints in its dir
        File hintsDir = new File(node.config().getString("hints_directory"));
        assertThat(FileUtils.folderSize(hintsDir)).isPositive();

        // and there is positive size of hints on the disk for the second node
        long totalHintsSize = node.appliesOnInstance((IIsolatedExecutor.SerializableFunction<UUID, Long>) secondNode -> {
            return HintsService.instance.getTotalHintsSize(secondNode);
        }).apply(node2UUID);

        Assertions.assertThat(totalHintsSize).isPositive();
    }

    Long insertData(final Cluster cluster)
    {
        // insert data and sleep every 10k to have a chance to flush hints
        for (int i = 0; i < 70000; i++)
        {
            cluster.coordinator(1)
                   .execute(withKeyspace("INSERT INTO %s.cf (k, c1) VALUES (?, ?);"),
                            ONE, UUID.randomUUID().toString(), UUID.randomUUID().toString());

            if (i % 10000 == 0)
                await().atLeast(2, SECONDS).pollDelay(2, SECONDS).until(() -> true);
        }

        await().atLeast(3, SECONDS).pollDelay(3, SECONDS).until(() -> true);

        // we see that metrics are updated

        await().until(() -> cluster.get(1).callOnInstance(() -> StorageMetrics.totalHints.getCount()) > 0);
        return cluster.get(1).callOnInstance(() -> StorageMetrics.totalHints.getCount());
    }

    static void waitForExistingRoles(Cluster cluster)
    {
        cluster.forEach(instance -> await().pollDelay(1, SECONDS)
                                           .pollInterval(1, SECONDS)
                                           .atMost(60, SECONDS)
                                           .until(() -> instance.callOnInstance(CassandraRoleManager::hasExistingRoles)));
    }
}
