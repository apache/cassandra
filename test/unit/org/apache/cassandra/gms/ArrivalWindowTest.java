package org.apache.cassandra.gms;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import static org.junit.Assert.*;

import org.junit.Test;

import java.lang.RuntimeException;
import java.net.InetAddress;
import java.net.UnknownHostException;

public class ArrivalWindowTest
{
    @Test
    public void testWithNanoTime()
    {
        final ArrivalWindow windowWithNano = new ArrivalWindow(4);
        final long toNano = 1000000L;
        InetAddress ep;
        try
        {
            ep = InetAddress.getLocalHost();
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
        windowWithNano.add(111 * toNano, ep);
        windowWithNano.add(222 * toNano, ep);
        windowWithNano.add(333 * toNano, ep);
        windowWithNano.add(444 * toNano, ep);
        windowWithNano.add(555 * toNano, ep);

        //all good
        assertEquals(1.0, windowWithNano.phi(666 * toNano), 0.01);
        //oh noes, a much higher timestamp, something went wrong!
        assertEquals(22.03, windowWithNano.phi(3000 * toNano), 0.01);
    }
}
