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

package org.apache.cassandra.auth;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Sets;
import org.junit.Test;

import org.apache.cassandra.cql3.CIDR;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class CIDRGroupsMappingTableTest
{
    private final CIDRGroupsMappingTable.Builder<String> ipv4CidrGroupsMappingTableBuilder;
    private final CIDRGroupsMappingTable.Builder<String> ipv6CidrGroupsMappingTableBuilder;

    public CIDRGroupsMappingTableTest()
    {
        ipv4CidrGroupsMappingTableBuilder = CIDRGroupsMappingTable.builder(false);
        ipv6CidrGroupsMappingTableBuilder = CIDRGroupsMappingTable.builder(true);
    }

    private CIDRGroupsMappingTable<String> buildCidrGroupsMappingTable(List<String> cidrs,
                                                                       CIDRGroupsMappingTable.Builder<String> cidrGroupsMappingTableBuilder)
    {
        for (String cidr : cidrs)
        {
            cidrGroupsMappingTableBuilder.add(CIDR.getInstance(cidr), cidr);
        }

        return cidrGroupsMappingTableBuilder.build();
    }

    private Set<String> longestMatchForIP(CIDRGroupsMappingTable<String> cidrGroupsMappingTable, String ip)
    {
        try
        {
            InetAddress ipAddr = InetAddress.getByName(ip);
            return cidrGroupsMappingTable.lookupLongestMatchForIP(ipAddr);
        }
        catch (UnknownHostException e)
        {
            throw new IllegalArgumentException(String.format("%s is not a valid IP String", ip));
        }
    }

    // expects one and single one result
    private String singleMatchForIp(CIDRGroupsMappingTable<String> cidrGroupsMappingTable, String ip)
    {
        return longestMatchForIP(cidrGroupsMappingTable, ip).iterator().next();
    }

    private String ip(String cidr)
    {
        return cidr.split("/")[0];
    }

    @Test
    public void insertIpv4CIDRs()
    {
        List<String> validCIDRs = Arrays.asList(
        "128.10.120.2/10",
        "128.20.120.2/20",
        "0.0.0.0/0",
        "10.1.1.2/10",
        "255.0.0.0/8"
        );

        CIDRGroupsMappingTable<String> ipv4CidrGroupsMappingTable =
            buildCidrGroupsMappingTable(validCIDRs, ipv4CidrGroupsMappingTableBuilder);

        for (String cidr : validCIDRs)
        {
            assertThat(singleMatchForIp(ipv4CidrGroupsMappingTable, ip(cidr))).isEqualTo(cidr);
        }
    }

    private void testIpv6CIDRs()
    {
        List<String> validCIDRs = Arrays.asList("2001:0db8:0:0:0:0:1234:5678/127",
                                                "2001:0db8:3333:4444:CCCC:DDDD:EEEE:FFFF/128",
                                                "2001:0db8:3333:4444:5555:6666:7777:8800/120",
                                                "2001:0db8::/30",
                                                "::1234:5678/40",
                                                "f000::/4",
                                                "ffff::/16"
        );

        Collections.shuffle(validCIDRs);

        CIDRGroupsMappingTable<String> ipv6CidrGroupsMappingTable =
            buildCidrGroupsMappingTable(validCIDRs, ipv6CidrGroupsMappingTableBuilder);

        for (String cidr : validCIDRs)
        {
            assertThat(singleMatchForIp(ipv6CidrGroupsMappingTable, ip(cidr))).isEqualTo(cidr);
        }

        assertThat(longestMatchForIP(ipv6CidrGroupsMappingTable, "0:0:0100::")).isNull();
        assertThat(longestMatchForIP(ipv6CidrGroupsMappingTable, "1001::5678")).isNull();
        assertThat(singleMatchForIp(ipv6CidrGroupsMappingTable, "ffff::5678")).isEqualTo("ffff::/16");

        assertThat(singleMatchForIp(ipv6CidrGroupsMappingTable, "2001:0db8:3333:4444:5555::"))
        .isEqualTo("2001:0db8::/30");
        assertThat(singleMatchForIp(ipv6CidrGroupsMappingTable, "2001:0db8:3333:4444:CCCC::"))
        .isEqualTo("2001:0db8::/30");
        assertThat(singleMatchForIp(ipv6CidrGroupsMappingTable, "2001:0db8:3333:4444:5555:6666:7777:8822"))
        .isEqualTo("2001:0db8:3333:4444:5555:6666:7777:8800/120");
        assertThat(singleMatchForIp(ipv6CidrGroupsMappingTable, "2001:0db8:3333:4444:5555:6666:7777:8282"))
        .isEqualTo("2001:0db8::/30");
        assertThat(singleMatchForIp(ipv6CidrGroupsMappingTable, "2001:0db8::1234:5679"))
        .isEqualTo("2001:0db8:0:0:0:0:1234:5678/127");
        assertThat(singleMatchForIp(ipv6CidrGroupsMappingTable, "2001:0db8::1234:5698"))
        .isEqualTo("2001:0db8::/30");
    }

    @Test
    public void testIpv6CIDRsInRandomOrder()
    {
        for (int i = 0; i < 20; i++)
            testIpv6CIDRs();
    }

    @Test
    public void ipv4MappedCIDRsInIpv6Tree()
    {
        // IPv4 mapped IPv6 gets converted to IPv4
        ipv6CidrGroupsMappingTableBuilder.add(CIDR.getInstance("0:0:0:0:0:ffff:192.1.56.10/96"),
                                              "0:0:0:0:0:ffff:192.1.56.10/96");
        assertThatThrownBy(ipv6CidrGroupsMappingTableBuilder::build)
        .hasMessage("Invalid CIDR format, expecting IPv6, received IPv4")
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void duplicateKeys()
    {
        List<String> validCIDRs = Arrays.asList("0:0:0:0:0:ffff:192.1.56.10/100", "::ffff:192.1.56.10/100");
        CIDRGroupsMappingTable<String> ipv4CidrGroupsMappingTable =
            buildCidrGroupsMappingTable(validCIDRs, ipv4CidrGroupsMappingTableBuilder);

        Set<String> result = longestMatchForIP(ipv4CidrGroupsMappingTable, "0:0:0:0:0:ffff:192.1.56.10");
        Set<String> expected = Sets.newHashSet(validCIDRs);
        assertThat(result).isEqualTo(expected);
    }

    @Test
    public void withOverlappingCIDRs()
    {
        List<String> validCIDRs = Arrays.asList("127.0.0.0/24", "127.0.1.0/24", "0.0.0.0/24");
        CIDRGroupsMappingTable<String> ipv4CidrGroupsMappingTable =
            buildCidrGroupsMappingTable(validCIDRs, ipv4CidrGroupsMappingTableBuilder);

        assertThat(longestMatchForIP(ipv4CidrGroupsMappingTable, "127.1.1.5")).isNull();
        assertThat(singleMatchForIp(ipv4CidrGroupsMappingTable, "127.0.0.5")).isEqualTo("127.0.0.0/24");
        assertThat(singleMatchForIp(ipv4CidrGroupsMappingTable, "127.0.1.5")).isEqualTo("127.0.1.0/24");
        assertThat(singleMatchForIp(ipv4CidrGroupsMappingTable, "0.0.0.5")).isEqualTo("0.0.0.0/24");
        assertThat(longestMatchForIP(ipv4CidrGroupsMappingTable, "0.0.1.5")).isNull();
    }

    @Test
    public void withNetMaskZero()
    {
        List<String> validCIDRs = Arrays.asList("0.0.0.0/0",
                                                "127.0.0.0/24",
                                                "127.0.0.112/26",
                                                "112.0.0.0/6");
        CIDRGroupsMappingTable<String> ipv4CidrGroupsMappingTable =
            buildCidrGroupsMappingTable(validCIDRs, ipv4CidrGroupsMappingTableBuilder);
        assertThat(singleMatchForIp(ipv4CidrGroupsMappingTable, "127.1.1.5")).isEqualTo("0.0.0.0/0");
        assertThat(singleMatchForIp(ipv4CidrGroupsMappingTable, "127.0.0.114")).isEqualTo("127.0.0.112/26");
        assertThat(singleMatchForIp(ipv4CidrGroupsMappingTable, "113.0.0.5")).isEqualTo("112.0.0.0/6");
        assertThat(singleMatchForIp(ipv4CidrGroupsMappingTable, "127.0.0.5")).isEqualTo("127.0.0.0/24");
        assertThat(singleMatchForIp(ipv4CidrGroupsMappingTable, "120.0.0.5")).isEqualTo("0.0.0.0/0");
        assertThat(singleMatchForIp(ipv4CidrGroupsMappingTable, "127.0.0.64")).isEqualTo("127.0.0.112/26");
    }

    @Test
    public void testLongestMatchWithInvalidIP()
    {
        CIDRGroupsMappingTable<String> ipv4CidrGroupsMappingTable =
            buildCidrGroupsMappingTable(Collections.singletonList("20.20.20.20/20"), ipv4CidrGroupsMappingTableBuilder);
        assertThatThrownBy(() -> longestMatchForIP(ipv4CidrGroupsMappingTable, "2000.20"))
        .hasMessage("2000.20 is not a valid IP String")
        .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testEmptyCidrsList()
    {
        CIDRGroupsMappingTable<String> ipv4CidrGroupsMappingTable =
            buildCidrGroupsMappingTable(Collections.emptyList(), ipv4CidrGroupsMappingTableBuilder);
        Set<String> cidrGroups = longestMatchForIP(ipv4CidrGroupsMappingTable, "20.20.20.20");
        assertThat(cidrGroups).isEmpty();
    }
}
