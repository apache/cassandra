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

package org.apache.cassandra.distributed.test.log;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.shared.AssertUtils;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.shared.Uninterruptibles;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.locator.NoOpProximity;
import org.apache.cassandra.locator.SimpleLocationProvider;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.distributed.Constants.KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BounceResetHostIdTest extends TestBaseImpl
{
    @Test
    public void swapIpsTest() throws Exception
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                                          // disable DistributedTestInitialLocationProvider as it tries to query before we setup
                                                          .set("node_proximity", NoOpProximity.class.getName())
                                                          .set("initial_location_provider", SimpleLocationProvider.class.getName()))
                                        .createWithoutStarting())
        {
            // This test relies on node IDs being in the same order as IP addresses
            for (int i = 1; i <= 3; i++)
                cluster.get(i).startup();

            cluster.get(2).shutdown().get();
            ClusterUtils.updateAddress(cluster.get(2), "127.0.0.4");
            cluster.get(2).startup();

            cluster.get(3).shutdown().get();
            ClusterUtils.updateAddress(cluster.get(3), "127.0.0.2");
            cluster.get(3).startup();

            cluster.get(2).shutdown().get();
            ClusterUtils.updateAddress(cluster.get(2), "127.0.0.3");
            cluster.get(2).startup();

            ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));

            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (true)
            {
                try
                {
                    AssertUtils.assertRows(sortHelper(cluster.coordinator(2).execute("select peer, host_id from system.peers_v2", ConsistencyLevel.QUORUM)),
                                           rows(row(InetAddress.getByName("127.0.0.1"), new NodeId(1).toUUID()),
                                                row(InetAddress.getByName("127.0.0.2"), new NodeId(3).toUUID())
                                           ));
                    AssertUtils.assertRows(sortHelper(cluster.coordinator(3).execute("select peer, host_id from system.peers_v2", ConsistencyLevel.QUORUM)),
                                           rows(row(InetAddress.getByName("127.0.0.1"), new NodeId(1).toUUID()),
                                                row(InetAddress.getByName("127.0.0.3"), new NodeId(2).toUUID())

                                           ));
                    return;
                }
                catch (AssertionError t)
                {
                    // If we are past the deadline, throw; allow to retry otherwise
                    if (System.nanoTime() > deadline)
                        throw t;
                }
            }
        }
    }

    @Test
    public void swapIpsDirectlyTest() throws Exception
    {
        try (Cluster cluster = builder().withNodes(3)
                                        .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                                          // disable DistributedTestInitialLocationProvider as it tries to query before we setup
                                                          .set("node_proximity", NoOpProximity.class.getName())
                                                          .set("initial_location_provider", SimpleLocationProvider.class.getName()))
                                        .createWithoutStarting())
        {
            // This test relies on node IDs being in the same order as IP addresses
            for (int i = 1; i <= 3; i++)
                cluster.get(i).startup();

            cluster.get(2).shutdown().get();
            cluster.get(3).shutdown().get();
            ClusterUtils.updateAddress(cluster.get(2), "127.0.0.3");
            ClusterUtils.updateAddress(cluster.get(3), "127.0.0.2");
            try
            {
                cluster.get(2).startup();
                fail("Should not have been able to start");
            }
            catch (Throwable t)
            {
                assertTrue(t.getMessage().contains("NodeId does not match locally set one"));
            }
            try
            {
                cluster.get(3).startup();
                fail("Should not have been able to start");
            }
            catch (Throwable t)
            {
                assertTrue(t.getMessage().contains("NodeId does not match locally set one"));
            }
        }
    }
    public static Object[][] sortHelper(Object[][] rows)
    {
        Arrays.sort(rows, Comparator.comparing(r -> ((InetAddress)r[0]).getHostAddress()));
        return rows;
    }

    @Test
    public void bounceWipedDatadirectoryTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .withDataDirCount(1)
                                             .withConfig(c -> c.with(Feature.GOSSIP, Feature.NATIVE_PROTOCOL)
                                                               .set(KEY_DTEST_API_STARTUP_FAILURE_AS_SHUTDOWN, false))
                                             .start()))
        {
            cluster.get(3).shutdown().get();
            wipeDir(((String[]) cluster.get(3).config().get("data_file_directories"))[0]);
            wipeDir((String) cluster.get(3).config().get("commitlog_directory"));
            for (int i = 0; i < 5; i++)
            {
                try
                {
                    cluster.get(3).startup();
                    fail();
                }
                catch (Exception e)
                {
                    assertTrue(e.getMessage(), e.getMessage().contains("Use cassandra.replace_address if you want to replace this node"));
                    cluster.get(3).shutdown().get();
                }
                Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
            }
        }
    }
    private static void wipeDir(String dir) throws IOException
    {
        Path source = Path.of(dir);
        Files.move(source, Path.of(source+"backup")); // remove potential trailing / from dir
        Files.createDirectories(source);
    }
}
