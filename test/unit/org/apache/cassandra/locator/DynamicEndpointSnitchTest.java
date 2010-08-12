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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

import org.junit.Test;

import static org.junit.Assert.*;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.utils.FBUtilities;

public class DynamicEndpointSnitchTest
{
    @Test
    public void testSnitch() throws UnknownHostException, InterruptedException
    {
        DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(new SimpleSnitch());
        InetAddress self = FBUtilities.getLocalAddress();
        ArrayList<InetAddress> order = new ArrayList<InetAddress>();
        InetAddress host1 = InetAddress.getByName("127.0.0.1");
        InetAddress host2 = InetAddress.getByName("127.0.0.2");
        InetAddress host3 = InetAddress.getByName("127.0.0.3");

        // first, make all hosts equal
        for (int i = 0; i < 5; i++)
        {
            dsnitch.receiveTiming(host1, 1.0);
            dsnitch.receiveTiming(host2, 1.0);
            dsnitch.receiveTiming(host3, 1.0);
        }

        Thread.sleep(1500);

        order.add(host1);
        order.add(host2);
        order.add(host3);

        assert dsnitch.getSortedListByProximity(self, order).equals(order);

        // make host1 a little worse
        dsnitch.receiveTiming(host1, 2.0);
        Thread.sleep(1500);
        order.clear();

        order.add(host2);
        order.add(host3);
        order.add(host1);

        assert dsnitch.getSortedListByProximity(self, order).equals(order);

        // make host2 a little worse
        dsnitch.receiveTiming(host2, 2.0);
        Thread.sleep(1500);
        order.clear();

        order.add(host3);
        order.add(host2);
        order.add(host1);

        assert dsnitch.getSortedListByProximity(self, order).equals(order);

        // make host3 the worst
        for (int i = 0; i < 2; i++)
        {
            dsnitch.receiveTiming(host3, 2.0);
        }
        Thread.sleep(1500);
        order.clear();

        order.add(host2);
        order.add(host1);
        order.add(host3);

        // make host3 equal to the others
        for (int i = 0; i < 2; i++)
        {
            dsnitch.receiveTiming(host3, 1.0);
        }
        Thread.sleep(1500);
        order.clear();

        order.add(host1);
        order.add(host2);
        order.add(host3);

        assert dsnitch.getSortedListByProximity(self, order).equals(order);
    }
}
