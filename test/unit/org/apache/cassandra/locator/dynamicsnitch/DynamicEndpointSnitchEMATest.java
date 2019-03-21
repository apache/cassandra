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

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.*;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.SimpleSnitch;
import org.apache.cassandra.net.LatencyMeasurementType;

import static org.junit.Assert.assertEquals;

public class DynamicEndpointSnitchEMATest
{
    private static DynamicEndpointSnitchEMA dsnitch;
    private static InetAddressAndPort[] hosts;

    @BeforeClass
    public static void setupDD() throws UnknownHostException
    {
        SimpleSnitch ss = new SimpleSnitch();

        hosts = new InetAddressAndPort[] {
            InetAddressAndPort.getByName("127.0.0.2"),
        };

        dsnitch = new DynamicEndpointSnitchEMA(ss, String.valueOf(ss.hashCode()));
    }

    @Before
    public void prepareDES()
    {
        dsnitch.reset();
    }

    @Test
    public void testReceiveTiming() throws IOException, ConfigurationException
    {
        List<Double> timings;

        dsnitch.receiveTiming(hosts[0], 1, LatencyMeasurementType.IGNORE);
        dsnitch.receiveTiming(hosts[0], 2, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[0], 3, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[0], 4, LatencyMeasurementType.PROBE);
        timings = dsnitch.dumpTimings(hosts[0].getHostAddress(false));
        Collections.sort(timings);
        assertEquals(2.2, timings.get(0), 0.1);

        dsnitch.reset();
        dsnitch.receiveTiming(hosts[0], 1, LatencyMeasurementType.PROBE);
        dsnitch.receiveTiming(hosts[0], 2, LatencyMeasurementType.PROBE);
        dsnitch.receiveTiming(hosts[0], 3, LatencyMeasurementType.READ);
        dsnitch.receiveTiming(hosts[0], 4, LatencyMeasurementType.PROBE);
        timings = dsnitch.dumpTimings(hosts[0].getHostAddress(false));
        Collections.sort(timings);
        assertEquals(1.5, timings.get(0), 0.1);
    }
}
