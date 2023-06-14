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

package org.apache.cassandra.cql3;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.junit.Test;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.junit.Assert.*;

public class CIDRTest
{
    @Test
    public void testIpv4StartingAndEndingIP()
    {
        CIDR cidr = CIDR.getInstance("255.255.255.25/24");
        assertEquals("255.255.255.0/24", cidr.toString());
        assertEquals("255.255.255.0", cidr.getStartIpAddress().getHostAddress());
        assertEquals("255.255.255.255", cidr.getEndIpAddress().getHostAddress());

        cidr = CIDR.getInstance("255.12.143.255/21");
        assertEquals("255.12.136.0/21", cidr.toString());
        assertEquals("255.12.136.0", cidr.getStartIpAddress().getHostAddress());
        assertEquals("255.12.143.255", cidr.getEndIpAddress().getHostAddress());

        cidr = CIDR.getInstance("255.25.55.255/31");
        assertEquals("255.25.55.254/31", cidr.toString());
        assertEquals("255.25.55.254", cidr.getStartIpAddress().getHostAddress());
        assertEquals("255.25.55.255", cidr.getEndIpAddress().getHostAddress());

        cidr = CIDR.getInstance("255.25.55.255/32");
        assertEquals("255.25.55.255/32", cidr.toString());
        assertEquals("255.25.55.255", cidr.getStartIpAddress().getHostAddress());
        assertEquals("255.25.55.255", cidr.getEndIpAddress().getHostAddress());
    }

    @Test
    public void testIpv6AdjustedToStartingIP()
    {
        CIDR cidr = CIDR.getInstance("2222:3333:4444:5555:6666:7777:8888:9999/16");
        assertEquals("2222:0:0:0:0:0:0:0/16", cidr.toString());
        assertEquals("2222:0:0:0:0:0:0:0", cidr.getStartIpAddress().getHostAddress());
        assertEquals("2222:ffff:ffff:ffff:ffff:ffff:ffff:ffff", cidr.getEndIpAddress().getHostAddress());

        cidr = CIDR.getInstance("2222:3333:4444:5555::/24");
        assertEquals("2222:3300:0:0:0:0:0:0/24", cidr.toString());
        assertEquals("2222:3300:0:0:0:0:0:0", cidr.getStartIpAddress().getHostAddress());
        assertEquals("2222:33ff:ffff:ffff:ffff:ffff:ffff:ffff", cidr.getEndIpAddress().getHostAddress());

        cidr = CIDR.getInstance("2222:3333::4444:550f/127");
        assertEquals("2222:3333:0:0:0:0:4444:550e/127", cidr.toString());
        assertEquals("2222:3333:0:0:0:0:4444:550e", cidr.getStartIpAddress().getHostAddress());
        assertEquals("2222:3333:0:0:0:0:4444:550f", cidr.getEndIpAddress().getHostAddress());
    }

    @Test
    public void testIpv4MappedIpv6()
    {
        CIDR cidr = CIDR.getInstance("::ffff:152.153.154.155/128");
        assertEquals("152.153.154.155/32", cidr.toString());
        assertEquals(32, cidr.getNetMask());
        assertFalse(cidr.isIPv6());
        assertTrue(cidr.isIPv4());
        assertTrue(cidr.equals(CIDR.getInstance("152.153.154.155/32")));

        cidr = CIDR.getInstance("::ffff:152.153.154.155/120");
        assertEquals("152.153.154.0/24", cidr.toString());
        assertEquals(24, cidr.getNetMask());
        assertFalse(cidr.isIPv6());
        assertTrue(cidr.isIPv4());
        assertTrue(cidr.equals(CIDR.getInstance("152.153.154.0/24")));
    }

    @Test()
    public void testCidrWithInvalidNetMask()
    {
        assertThatThrownBy(() -> CIDR.getInstance("100.100.100.100/33"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("100.100.100.100/33 is not a valid CIDR String");

        assertThatThrownBy(() -> new CIDR(InetAddress.getByName("100.100.100.100"), (short)33))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid netmask 33 for IP 100.100.100.100");
    }

    @Test()
    public void testInvalidIpv4MappedIpv6Mask()
    {
        assertThatThrownBy((() -> CIDR.getInstance("::ffff:152.153.154.155/90").toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("::ffff:152.153.154.155/90 is not a valid CIDR String");
    }

    @Test
    public void testNullOrEmptyCidrString()
    {
        assertThatThrownBy((() -> CIDR.getInstance(null).toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("null is not a valid CIDR String");

        assertThatThrownBy((() -> CIDR.getInstance("").toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(" is not a valid CIDR String");
    }

    @Test
    public void testCidrStringWithoutNetMask()
    {
        assertThatThrownBy((() -> CIDR.getInstance("10.20.30.40").toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("10.20.30.40 is not a valid CIDR String");
    }

    @Test
    public void testCidrStringWithInvalidIp()
    {
        assertThatThrownBy((() -> CIDR.getInstance("10.20.300.40/24").toString()))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("10.20.300.40/24 is not a valid CIDR String");
    }

    @Test
    public void testCidrAsTupleString()
    {
        CIDR cidr = CIDR.getInstance("10.20.30.40/24");
        assertEquals("('10.20.30.0', 24)", cidr.asCqlTupleString());
    }

    @Test
    public void testCompareIPs() throws UnknownHostException
    {
        assertTrue(CIDR.compareIPs(InetAddress.getByName("10.20.30.40"), InetAddress.getByName("10.20.30.41")) < 0);
        assertTrue(CIDR.compareIPs(InetAddress.getByName("10.20.30.40"), InetAddress.getByName("10.20.30.39")) > 0);
        assertTrue(CIDR.compareIPs(InetAddress.getByName("10.20.30.40"), InetAddress.getByName("10.20.30.40")) == 0);

        assertTrue(CIDR.compareIPs(InetAddress.getByName("2222:3333::4444:550f"),
                                   InetAddress.getByName("2222:5555::4444:550f")) < 0);
        assertTrue(CIDR.compareIPs(InetAddress.getByName("2222:ffff:ffff:ffff:ffff:ffff:ffff:ffff"),
                                   InetAddress.getByName("2222:3333::4444:550f")) > 0);
        assertTrue(CIDR.compareIPs(InetAddress.getByName("2222:3333::4444:550f"),
                                   InetAddress.getByName("2222:3333:0000:0000::4444:550f")) == 0);

        assertTrue(CIDR.compareIPs(InetAddress.getByName("10.20.30.40"), InetAddress.getByName("10.22.40.40")) < 0);
        assertTrue(CIDR.compareIPs(InetAddress.getByName("10.24.40.40"), InetAddress.getByName("10.20.30.40")) > 0);

        assertTrue(CIDR.compareIPs(InetAddress.getByName("0.20.30.40"), InetAddress.getByName("255.20.40.40")) < 0);
        assertTrue(CIDR.compareIPs(InetAddress.getByName("255.20.40.40"), InetAddress.getByName("128.20.30.40")) > 0);

        assertTrue(CIDR.compareIPs(InetAddress.getByName("::ffff:152.153.154.155"),
                                   InetAddress.getByName("152.154.154.155")) < 0);
        assertTrue(CIDR.compareIPs(InetAddress.getByName("::ffff:152.154.154.155"),
                                   InetAddress.getByName("152.153.154.155")) > 0);
        assertTrue(CIDR.compareIPs(InetAddress.getByName("::ffff:152.153.154.155"),
                                   InetAddress.getByName("152.153.154.155")) == 0);
    }

    @Test
    public void testCidrOverlaps()
    {
        assertFalse(CIDR.overlaps(CIDR.getInstance("10.20.30.0/24"), CIDR.getInstance("20.20.30.0/24")));
        assertFalse(CIDR.overlaps(CIDR.getInstance("10.20.63.0/24"), CIDR.getInstance("10.20.79.0/24")));

        assertTrue(CIDR.overlaps(CIDR.getInstance("10.20.30.0/24"), CIDR.getInstance("10.20.30.0/25")));
        assertTrue(CIDR.overlaps(CIDR.getInstance("10.20.30.0/24"), CIDR.getInstance("10.20.30.0/24")));

        assertTrue(CIDR.overlaps(CIDR.getInstance("::ffff:152.153.154.155/120"),
                                 CIDR.getInstance("152.153.154.155/24")));
        assertFalse(CIDR.overlaps(CIDR.getInstance("::ffff:152.153.154.155/120"),
                                  CIDR.getInstance("::ffff:152.153.153.155/120")));

        assertTrue(CIDR.overlaps(CIDR.getInstance("2222:3333:4444:5555:6666:7777:8888:9999/16"),
                                 CIDR.getInstance("2222:3333:4444:5555:6666:7777:8888:9999/36")));
        assertFalse(CIDR.overlaps(CIDR.getInstance("2222:3333:4444:5555:6666:7777:8888:9999/16"),
                                  CIDR.getInstance("1010:3333:4444:5555:6666:7777:8888:9999/16")));
    }
}
