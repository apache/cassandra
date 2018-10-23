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

package org.apache.cassandra.locator.dynamicsnitch;

import java.net.UnknownHostException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.Util;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.EndpointsForRange;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaUtils;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.net.LatencyMeasurementType;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.*;

public class DynamicEndpointSnitchLegacyHistogramTest
{
    private static DynamicEndpointSnitchLegacyHistogram dsnitch;
    private static InetAddressAndPort[] hosts;

    private final static int PROBE_INTERVAL = 1000;

    @BeforeClass
    public static void setupDD() throws UnknownHostException
    {
        SimpleSnitch ss = new SimpleSnitch();
        hosts = new InetAddressAndPort[] {
            FBUtilities.getBroadcastAddressAndPort(),
            InetAddressAndPort.getByName("127.0.0.2"),
            InetAddressAndPort.getByName("127.0.0.3"),
        };
        dsnitch = new DynamicEndpointSnitchLegacyHistogram(ss, String.valueOf(ss.hashCode()));
        dsnitch.applyConfigChanges(10, PROBE_INTERVAL, 0);
    }

    @Before
    public void prepareDES()
    {
        dsnitch.reset();
    }

    private static EndpointsForRange full(InetAddressAndPort... endpoints)
    {
        EndpointsForRange.Builder rlist = EndpointsForRange.builder(ReplicaUtils.FULL_RANGE, endpoints.length);
        for (InetAddressAndPort endpoint: endpoints)
        {
            rlist.add(ReplicaUtils.full(endpoint));
        }
        return rlist.build();
    }

    @Test
    public void testResets()
    {
        dsnitch.receiveTiming(hosts[1], 2, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[2], 2, LatencyMeasurementType.READ);

        for (int i = 0; i < (DynamicEndpointSnitch.MAX_PROBE_INTERVAL_MS / PROBE_INTERVAL); i ++)
        {
            dsnitch.updateSamples();
            assertTrue(dsnitch.getMeasurementsWithPort().containsKey(hosts[1]));
            assertTrue(dsnitch.getMeasurementsWithPort().containsKey(hosts[2]));
            dsnitch.receiveTiming(hosts[1], 1, LatencyMeasurementType.READ);
        }

        dsnitch.updateSamples();

        assertFalse(dsnitch.getMeasurementsWithPort().containsKey(hosts[1]));
        assertFalse(dsnitch.getMeasurementsWithPort().containsKey(hosts[2]));
        EndpointsForRange order = dsnitch.sortedByProximity(hosts[0], full(hosts[1], hosts[2]));

        Util.assertRCEquals(order, full(hosts[1], hosts[2]));
    }
}