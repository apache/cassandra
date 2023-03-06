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

package org.apache.cassandra.tcm;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.ServerTestUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RemoteProcessorTest
{
    @Before
    public void before()
    {
        DatabaseDescriptor.daemonInitialization();
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        ServerTestUtils.daemonInitialization();
        ServerTestUtils.prepareServer();
    }

    @Test
    public void simpleTestCMSIterator()
    {
        int endpointCount = 10;
        List<InetAddressAndPort> allEndpoints = eps(endpointCount);
        Set<InetAddressAndPort> cms = new HashSet<>(allEndpoints.subList(0, 2));
        Set<InetAddressAndPort> discovery = new HashSet<>(allEndpoints.subList(2, 4));
        RemoteProcessor.CandidateIterator iter = new RemoteProcessor.CandidateIterator(discovery, false);

        InetAddressAndPort returned = iter.next();
        assertTrue(discovery.contains(returned));
        iter.addCandidates(new Discovery.DiscoveredNodes(cms, Discovery.DiscoveredNodes.Kind.CMS_ONLY));
        returned = iter.next();
        assertFalse(discovery.contains(returned));
        assertTrue(cms.contains(returned));
    }

    @Test
    public void timeoutTest()
    {
        // make sure that a node marked as timed out will not be returned until we've cycled through all other candidates
        // when using the iterator in a RemoteProcessor::sendWithCallback call, the Backoff will trigger the breaking
        // out of the cycle.
        int endpointCount = 10;
        List<InetAddressAndPort> allEndpoints = eps(endpointCount);
        Set<InetAddressAndPort> discovery = new HashSet<>(allEndpoints.subList(0, 4));
        RemoteProcessor.CandidateIterator iter = new RemoteProcessor.CandidateIterator(discovery, false);
        InetAddressAndPort timeout = iter.peek();
        for (int i = 1; i < 10; i++)
        {
            assertTrue(iter.hasNext());
            InetAddressAndPort returned = iter.next();
            if (returned.equals(timeout))
            {
                iter.timeout(returned);
                assertEquals(timeout, iter.peekLast());
            }
        }
    }

    @Test
    public void notCMSTest()
    {
        // make sure that a node marked as notCMS will not be returned until we've cycled through all other candidates
        // when using the iterator in a RemoteProcessor::sendWithCallback call, the Backoff will trigger the breaking
        // out of the cycle.
        int endpointCount = 10;
        List<InetAddressAndPort> allEndpoints = eps(endpointCount);
        Set<InetAddressAndPort> discovery = new HashSet<>(allEndpoints.subList(0, 4));
        RemoteProcessor.CandidateIterator iter = new RemoteProcessor.CandidateIterator(discovery, false);
        InetAddressAndPort notcms = iter.peek();
        for (int i = 1; i < 10; i++)
        {
            assertTrue(iter.hasNext());
            InetAddressAndPort returned = iter.next();
            assertTrue(discovery.contains(returned));
            if (returned.equals(notcms))
            {
                iter.notCms(returned);
                assertEquals(notcms, iter.peekLast());
            }
        }
    }

    private List<InetAddressAndPort> eps(int endpointCount)
    {
        List<InetAddressAndPort> allEndpoints = new ArrayList<>(endpointCount);
        for (int i = 0; i < endpointCount; i++)
        {
            InetAddressAndPort ep = InetAddressAndPort.getByNameUnchecked("127.0.0."+i);
            allEndpoints.add(ep);
        }
        return allEndpoints;
    }
}
