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

package org.apache.cassandra.distributed.test.ring;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.action.GossipHelper.decommission;
import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NATIVE_PROTOCOL;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.utils.Clock.Global.currentTimeMillis;

public class CommunicationDuringDecommissionTest extends TestBaseImpl
{
    @Test
    public void internodeConnectionsDuringDecom() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(4)
                                        .withConfig(config -> config.with(NETWORK, GOSSIP, NATIVE_PROTOCOL))
                                        .start())
        {
            BootstrapTest.populate(cluster, 0, 100);

            cluster.run(decommission(), 1);

            cluster.filters().allVerbs().from(1).messagesMatching((i, i1, iMessage) -> {
                throw new AssertionError("Decomissioned node should not send any messages");
            }).drop();


            Map<Integer, Long> connectionAttempts = new HashMap<>();
            long deadline = currentTimeMillis() + TimeUnit.SECONDS.toMillis(10);

            // Wait 10 seconds and check if there are any new connection attempts to the decomissioned node
            while (currentTimeMillis() <= deadline)
            {
                for (int i = 2; i <= cluster.size(); i++)
                {
                    Object[][] res = cluster.get(i).executeInternal("SELECT active_connections, connection_attempts FROM system_views.internode_outbound WHERE address = '127.0.0.1' AND port = 7012");
                    Assert.assertEquals(1, res.length);
                    Assert.assertEquals(0L, ((Long) res[0][0]).longValue());
                    long attempts = ((Long) res[0][1]).longValue();
                    if (connectionAttempts.get(i) == null)
                        connectionAttempts.put(i, attempts);
                    else
                        Assert.assertEquals(connectionAttempts.get(i), (Long) attempts);
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(100));
            }
        }
    }
}
