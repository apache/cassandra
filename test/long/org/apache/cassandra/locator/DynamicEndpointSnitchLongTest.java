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

public class DynamicEndpointSnitchLongTest
{
    @Test
    public void testConcurrency() throws InterruptedException, IOException, ConfigurationException
    {
        // The goal of this test is to check for CASSANDRA-8448/CASSANDRA-9519
        double badness = DatabaseDescriptor.getDynamicBadnessThreshold();
        DatabaseDescriptor.setDynamicBadnessThreshold(0.0);

        try
        {
            final int ITERATIONS = 10000;

            // do this because SS needs to be initialized before DES can work properly.
            StorageService.instance.unsafeInitialize();
            SimpleSnitch ss = new SimpleSnitch();
            DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(ss, String.valueOf(ss.hashCode()));
            InetAddress self = FBUtilities.getBroadcastAddress();

            List<InetAddress> hosts = new ArrayList<>();
            // We want a big list of hosts so  sorting takes time, making it much more likely to reproduce the
            // problem we're looking for.
            for (int i = 0; i < 100; i++)
                for (int j = 0; j < 256; j++)
                    hosts.add(InetAddress.getByAddress(new byte[]{127, 0, (byte)i, (byte)j}));

            ScoreUpdater updater = new ScoreUpdater(dsnitch, hosts);
            updater.start();

            List<InetAddress> result = null;
            for (int i = 0; i < ITERATIONS; i++)
                result = dsnitch.getSortedListByProximity(self, hosts);

            updater.stopped = true;
            updater.join();
        }
        finally
        {
            DatabaseDescriptor.setDynamicBadnessThreshold(badness);
        }
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

