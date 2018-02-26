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

package org.apache.cassandra.net;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import com.google.common.net.InetAddresses;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.locator.InetAddressAndPort;

public class StartupClusterConnectivityCheckerTest
{
    @Test
    public void testConnectivity_SimpleHappyPath() throws UnknownHostException
    {
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(70, 10, addr -> true);
        int count = 10;
        Set<InetAddressAndPort> peers = createNodes(count);
        Assert.assertEquals(StartupClusterConnectivityChecker.State.FINISH_SUCCESS,
                            connectivityChecker.checkStatus(peers, new AtomicInteger(count * 2), System.nanoTime(), false, 0));
    }

    @Test
    public void testConnectivity_SimpleContinue() throws UnknownHostException
    {
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(70, 10, addr -> true);
        int count = 10;
        Set<InetAddressAndPort> peers = createNodes(count);
        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, new AtomicInteger(0), System.nanoTime(), false, 0));
    }

    @Test
    public void testConnectivity_Timeout() throws UnknownHostException
    {
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(70, 10, addr -> true);
        int count = 10;
        Set<InetAddressAndPort> peers = createNodes(count);
        Assert.assertEquals(StartupClusterConnectivityChecker.State.CONTINUE,
                            connectivityChecker.checkStatus(peers, new AtomicInteger(0), System.nanoTime(), false, 4));
        Assert.assertEquals(StartupClusterConnectivityChecker.State.FINISH_TIMEOUT,
                            connectivityChecker.checkStatus(peers, new AtomicInteger(0), System.nanoTime(), true, 5));
    }

    @Test
    public void testConnectivity_SimpleUpdating() throws UnknownHostException
    {
        UpdatablePredicate predicate = new UpdatablePredicate();
        final int count = 100;
        final int thresholdPercentage = 70;
        StartupClusterConnectivityChecker connectivityChecker = new StartupClusterConnectivityChecker(thresholdPercentage, 10, predicate);
        Set<InetAddressAndPort> peers = createNodes(count);

        AtomicInteger connectedCount = new AtomicInteger();

        for (int i = 0; i < count; i++)
        {
            predicate.reset(i);
            connectedCount.set(i * 2);
            StartupClusterConnectivityChecker.State expectedState = i < thresholdPercentage ?
                                                                    StartupClusterConnectivityChecker.State.CONTINUE :
                                                                    StartupClusterConnectivityChecker.State.FINISH_SUCCESS;
            Assert.assertEquals("failed on iteration " + i,
                                expectedState, connectivityChecker.checkStatus(peers, connectedCount, System.nanoTime(), false, i));
        }
    }

    /**
     * returns true until index = threshold, then returns false.
     */
    private class UpdatablePredicate implements Predicate<InetAddressAndPort>
    {
        int index;
        int threshold;

        void reset(int threshold)
        {
            index = 0;
            this.threshold = threshold;
        }

        @Override
        public boolean test(InetAddressAndPort inetAddressAndPort)
        {
            index++;
            return index <= threshold;
        }
    }

    private static Set<InetAddressAndPort> createNodes(int count) throws UnknownHostException
    {
        Set<InetAddressAndPort> nodes = new HashSet<>();

        if (count < 1)
            Assert.fail("need at least *one* node in the set!");

        InetAddressAndPort node = InetAddressAndPort.getByName("127.0.0.1");
        nodes.add(node);
        for (int i = 1; i < count; i++)
        {
            node = InetAddressAndPort.getByAddress(InetAddresses.increment(node.address));
            nodes.add(node);
        }
        return nodes;
    }

}
