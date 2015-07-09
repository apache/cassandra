/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/

package org.apache.cassandra.locator;

import java.io.IOException;
import java.net.InetAddress;
import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class DynamicEndpointSnitchTest
{

    private static void setScores(DynamicEndpointSnitch dsnitch,  int rounds, List<InetAddress> hosts, Integer... scores) throws InterruptedException
    {
        for (int round = 0; round < rounds; round++)
        {
            for (int i = 0; i < hosts.size(); i++)
                dsnitch.receiveTiming(hosts.get(i), scores[i]);
        }
        Thread.sleep(150);
    }

    @Test
    public void testSnitch() throws InterruptedException, IOException, ConfigurationException
    {
        // do this because SS needs to be initialized before DES can work properly.
        StorageService.instance.initClient(0);
        SimpleSnitch ss = new SimpleSnitch();
        DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));
        InetAddress self = FBUtilities.getBroadcastAddress();
        InetAddress host1 = InetAddress.getByName("127.0.0.2");
        InetAddress host2 = InetAddress.getByName("127.0.0.3");
        InetAddress host3 = InetAddress.getByName("127.0.0.4");
        List<InetAddress> hosts = Arrays.asList(host1, host2, host3);

        // first, make all hosts equal
        setScores(dsnitch, 1, hosts, 10, 10, 10);
        List<InetAddress> order = Arrays.asList(host1, host2, host3);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));

        // make host1 a little worse
        setScores(dsnitch, 1, hosts, 20, 10, 10);
        order = Arrays.asList(host2, host3, host1);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));

        // make host2 as bad as host1
        setScores(dsnitch, 2, hosts, 15, 20, 10);
        order = Arrays.asList(host3, host1, host2);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));

        // make host3 the worst
        setScores(dsnitch, 3, hosts, 10, 10, 30);
        order = Arrays.asList(host1, host2, host3);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));

        // make host3 equal to the others
        setScores(dsnitch, 5, hosts, 10, 10, 10);
        order = Arrays.asList(host1, host2, host3);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));

        /// Tests CASSANDRA-6683 improvements
        // make the scores differ enough from the ideal order that we sort by score; under the old
        // dynamic snitch behavior (where we only compared neighbors), these wouldn't get sorted
        setScores(dsnitch, 20, hosts, 10, 70, 20);
        order = Arrays.asList(host1, host3, host2);
        assertEquals(order, dsnitch.getSortedListByProximity(self, Arrays.asList(host1, host2, host3)));
    }

    @Test
    public void testConcurrency() throws InterruptedException, IOException, ConfigurationException
    {
        // The goal of this test is to check for CASSANDRA-8448/CASSANDRA-9519
        double badness = DatabaseDescriptor.getDynamicBadnessThreshold();
        DatabaseDescriptor.setDynamicBadnessThreshold(0.0);

        final int ITERATIONS = 10;

        // do this because SS needs to be initialized before DES can work properly.
        StorageService.instance.initClient(0);
        SimpleSnitch ss = new SimpleSnitch();
        DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));
        InetAddress self = FBUtilities.getBroadcastAddress();

        List<InetAddress> hosts = new ArrayList<>();
        // We want a giant list of hosts so that sorting it takes time, making it much more likely to reproduce the
        // problem we're looking for.
        for (int i = 0; i < 10; i++)
            for (int j = 0; j < 256; j++)
                for (int k = 0; k < 256; k++)
                    hosts.add(InetAddress.getByAddress(new byte[]{127, (byte)i, (byte)j, (byte)k}));

        ScoreUpdater updater = new ScoreUpdater(dsnitch, hosts);
        updater.start();

        List<InetAddress> result = null;
        for (int i = 0; i < ITERATIONS; i++)
            result = dsnitch.getSortedListByProximity(self, hosts);

        updater.stopped = true;
        updater.join();

        DatabaseDescriptor.setDynamicBadnessThreshold(badness);
    }

    public static class ScoreUpdater extends Thread
    {
        private static final int SCORE_RANGE = 100;

        public volatile boolean stopped;

        private final DynamicEndpointSnitch dsnitch;
        private final List<InetAddress> hosts;
        private final Random random = new Random();

        public ScoreUpdater(DynamicEndpointSnitch dsnitch, List<InetAddress> hosts)
        {
            this.dsnitch = dsnitch;
            this.hosts = hosts;
        }

        public void run()
        {
            while (!stopped)
            {
                InetAddress host = hosts.get(random.nextInt(hosts.size()));
                int score = random.nextInt(SCORE_RANGE);
                dsnitch.receiveTiming(host, score);
            }
        }
    }
}
