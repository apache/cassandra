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
import java.util.ArrayList;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.service.StorageService;
import org.junit.Test;

import org.apache.cassandra.utils.FBUtilities;

public class DynamicEndpointSnitchTest
{
    @Test
    public void testSnitch() throws InterruptedException, IOException, ConfigurationException
    {
        // do this because SS needs to be initialized before DES can work properly.
        StorageService.instance.initClient(0);
        int sleeptime = 150;
        DynamicEndpointSnitch dsnitch = new DynamicEndpointSnitch(new SimpleSnitch());
        InetAddress self = FBUtilities.getBroadcastAddress();
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

        Thread.sleep(sleeptime);

        order.add(host1);
        order.add(host2);
        order.add(host3);
        assert dsnitch.getSortedListByProximity(self, order).equals(order);

        // make host1 a little worse
        dsnitch.receiveTiming(host1, 2.0);
        Thread.sleep(sleeptime);

        order.clear();
        order.add(host2);
        order.add(host3);
        order.add(host1);
        assert dsnitch.getSortedListByProximity(self, order).equals(order);

        // make host2 as bad as host1
        dsnitch.receiveTiming(host2, 2.0);
        Thread.sleep(sleeptime);

        order.clear();
        order.add(host3);
        order.add(host1);
        order.add(host2);
        assert dsnitch.getSortedListByProximity(self, order).equals(order);

        // make host3 the worst
        for (int i = 0; i < 2; i++)
        {
            dsnitch.receiveTiming(host3, 2.0);
        }
        Thread.sleep(sleeptime);

        order.clear();
        order.add(host1);
        order.add(host2);
        order.add(host3);
        assert dsnitch.getSortedListByProximity(self, order).equals(order);

        // make host3 equal to the others
        for (int i = 0; i < 2; i++)
        {
            dsnitch.receiveTiming(host3, 1.0);
        }
        Thread.sleep(sleeptime);

        order.clear();
        order.add(host1);
        order.add(host2);
        order.add(host3);
        assert dsnitch.getSortedListByProximity(self, order).equals(order);
    }
}
