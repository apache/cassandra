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
import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.consistent.LocalSessionAccessor;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.TimeUUID;

import static java.util.Arrays.stream;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.repair.consistent.ConsistentSession.State.REPAIRING;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;
import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertTrue;

public class IncRepairAdminTest extends TestBaseImpl
{
    @Test
    public void testRepairAdminSummarizePending() throws IOException
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                           .start()))
        {
            // given a cluster with a table
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (k INT PRIMARY KEY, v INT)");
            // when running repair_admin summarize-pending
            NodeToolResult res = cluster.get(1).nodetoolResult("repair_admin", "summarize-pending");
            // then the table info should be present in the output
            res.asserts().success();
            String outputLine = stream(res.getStdout().split("\n"))
                    .filter(l -> l.contains(KEYSPACE) && l.contains("tbl"))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("should find tbl table in output of repair_admin summarize-pending"));
            assertTrue("should contain information about zero pending bytes", outputLine.contains("0 bytes (0 sstables / 0 sessions)"));
        }
    }

    @Test
    public void testManualSessionFail() throws IOException
    {
        repairAdminCancelHelper(true, false);
    }

    @Test
    public void testManualSessionCancelNonCoordinatorFailure() throws IOException
    {
        repairAdminCancelHelper(false, false);
    }

    @Test
    public void testManualSessionForceCancel() throws IOException
    {
        repairAdminCancelHelper(false, true);
    }

    private void repairAdminCancelHelper(boolean coordinator, boolean force) throws IOException
    {
        try (Cluster cluster = init(Cluster.build(3)
                                           .withConfig(config -> config.with(GOSSIP)
                                                                       .with(NETWORK))
                                           .start()))
        {
            boolean shouldFail = !coordinator && !force;
            cluster.schemaChange("CREATE TABLE "+KEYSPACE+".tbl (k INT PRIMARY KEY, v INT)");

            cluster.forEach(i -> {
                NodeToolResult res = i.nodetoolResult("repair_admin");
                res.asserts().stdoutContains("no sessions");
            });

            TimeUUID uuid = makeFakeSession(cluster);
            awaitNodetoolRepairAdminContains(cluster, uuid, "REPAIRING", false);
            IInvokableInstance instance = cluster.get(coordinator ? 1 : 2);

            NodeToolResult res;
            if (force)
            {
                res = instance.nodetoolResult("repair_admin", "cancel", "--session", uuid.toString(), "--force");
            }
            else
            {
                res = instance.nodetoolResult("repair_admin", "cancel", "--session", uuid.toString());
            }

            if (shouldFail)
            {
                res.asserts().failure();
                // if nodetool repair_admin cancel fails, the session should still be repairing:
                awaitNodetoolRepairAdminContains(cluster, uuid, "REPAIRING", true);
            }
            else
            {
                res.asserts().success();
                awaitNodetoolRepairAdminContains(cluster, uuid, "FAILED", true);
            }
        }
    }



    private static void awaitNodetoolRepairAdminContains(Cluster cluster, TimeUUID uuid, String state, boolean all)
    {
        cluster.forEach(i -> {
            while (true)
            {
                NodeToolResult res;
                if (all)
                    res = i.nodetoolResult("repair_admin", "list", "--all");
                else
                    res = i.nodetoolResult("repair_admin");
                res.asserts().success();
                String[] lines = res.getStdout().split("\n");
                assertTrue(lines.length > 1);
                for (String line : lines)
                {
                    if (line.contains(uuid.toString()) && line.contains(state))
                        return;
                }
                Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
            }
        });
    }

    private static TimeUUID makeFakeSession(Cluster cluster)
    {
        TimeUUID sessionId = nextTimeUUID();
        InetSocketAddress coordinator = cluster.get(1).config().broadcastAddress();
        Set<InetSocketAddress> participants = cluster.stream()
                                                     .map(i -> i.config().broadcastAddress())
                                                     .collect(Collectors.toSet());
        cluster.forEach(i -> {
            i.runOnInstance(() -> {
                ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl");
                Range<Token> range = new Range<>(cfs.metadata().partitioner.getMinimumToken(),
                                                 cfs.metadata().partitioner.getRandomToken());
                ActiveRepairService.instance().registerParentRepairSession(sessionId,
                                                                           InetAddressAndPort.getByAddress(coordinator.getAddress()),
                                                                           Lists.newArrayList(cfs),
                                                                           Sets.newHashSet(range),
                                                                           true,
                                                                           currentTimeMillis(),
                                                                           true,
                                                                           PreviewKind.NONE);
                LocalSessionAccessor.prepareUnsafe(sessionId,
                                                   InetAddressAndPort.getByAddress(coordinator.getAddress()),
                                                   participants.stream().map(participant -> InetAddressAndPort.getByAddress(participant.getAddress())).collect(Collectors.toSet()));
                LocalSessionAccessor.setState(sessionId, REPAIRING);
            });
        });
        return sessionId;
    }
}
