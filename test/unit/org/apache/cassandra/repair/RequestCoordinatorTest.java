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
package org.apache.cassandra.repair;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.locator.AbstractEndpointSnitch;
import org.junit.Before;
import org.junit.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class RequestCoordinatorTest implements IRequestProcessor<InetAddress>
{
    private InetAddress[] endpoints;
    private List<InetAddress> activeRequests;
    private static Random random = new Random(0);

    @Before
    public void setup() throws UnknownHostException
    {
        endpoints = new InetAddress[12];
        for (int i = 0; i < 12; i++)
            endpoints[i] = InetAddress.getByName("127.0.0." + (i + 1));
        activeRequests = new ArrayList<>();
        DatabaseDescriptor.setEndpointSnitch(new AbstractEndpointSnitch()
        {
            @Override
            public String getRack(InetAddress endpoint)
            {
                return "rack1";
            }

            @Override
            public String getDatacenter(InetAddress endpoint)
            {
                // 127.0.0.1, 127.0.0.2, 127.0.0.3 -> datacenter1
                // 127.0.0.4, 127.0.0.5, 127.0.0.6 -> datacenter2 etc
                int no = endpoint.getAddress()[3] - 1;
                return "datacenter" + (no / 3 + 1);
            }

            @Override
            public int compareEndpoints(InetAddress target, InetAddress a1, InetAddress a2)
            {
                return 0;
            }
        });
    }

    @Override
    public void process(InetAddress request)
    {
        activeRequests.add(request);
    }

    @Test
    public void testSequentialRequestCoordinator()
    {
        SequentialRequestCoordinator<InetAddress> coordinator = new SequentialRequestCoordinator<>(this);
        for (InetAddress endpoint : endpoints)
            coordinator.add(endpoint);
        coordinator.start();
        int max = finishRequests(coordinator);
        assertEquals(1, max);
    }

    @Test
    public void testParallelRequestCoordinator()
    {
        ParallelRequestCoordinator<InetAddress> coordinator = new ParallelRequestCoordinator<>(this);
        for (InetAddress endpoint : endpoints)
            coordinator.add(endpoint);
        coordinator.start();
        int max = finishRequests(coordinator);
        assertEquals(endpoints.length, max);
    }

    @Test
    public void testDatacenterAwareRequestCoordinator()
    {
        DatacenterAwareRequestCoordinator coordinator = new DatacenterAwareRequestCoordinator(this);
        for (InetAddress endpoint : endpoints)
            coordinator.add(endpoint);
        coordinator.start();
        int max = finishRequests(coordinator);
        assertEquals(4, max);
    }

    private int finishRequests(IRequestCoordinator<InetAddress> requestCoordinator)
    {
        int max = 0;
        while (activeRequests.size() > 0)
        {
            max = Math.max(max, activeRequests.size());
            // Finish a request
            int ix = random.nextInt(activeRequests.size());
            InetAddress finished = activeRequests.get(ix);
            activeRequests.remove(ix);
            requestCoordinator.completed(finished);
        }
        return max;
    }
}
