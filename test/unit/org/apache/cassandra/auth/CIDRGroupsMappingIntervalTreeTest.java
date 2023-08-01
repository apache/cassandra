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
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import org.apache.cassandra.auth.CIDRGroupsMappingIntervalTree.IPIntervalNode;
import org.apache.cassandra.cql3.CIDR;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Test the internal data structure used for the interval tree.
 * The test cases for CIDRGroupsMappingTable functionality can be found at {@link CIDRGroupsMappingTableTest}
 */
public class CIDRGroupsMappingIntervalTreeTest
{
    private IPIntervalNode<Void> createInternalNode(String parentCidr, IPIntervalNode<Void>[] children)
    {
        return new IPIntervalNode<>(CIDR.getInstance(parentCidr), null, children);
    }

    @SuppressWarnings("unchecked")
    private IPIntervalNode<Void>[] createLeaves(List<String> cidrs)
    {
        return cidrs.stream().map(this::createLeafNode).toArray(IPIntervalNode[]::new);
    }

    private IPIntervalNode<Void> createLeafNode(String cidr)
    {
        return new IPIntervalNode<>(CIDR.getInstance(cidr), null, null);
    }

    private List<CIDR> toCidrs(IPIntervalNode<Void>[] nodes)
    {
        return Arrays.stream(nodes).map(IPIntervalNode::cidr).collect(Collectors.toList());
    }

    @Test
    public void testInternalNodeAllLeft()
    {
        List<String> cidrs = Arrays.asList("10.0.0.1/24",
                                           "10.0.1.1/24",
                                           "10.0.2.1/24",
                                           "10.0.3.1/24");
        String parentCidr = "11.0.0.1/30";

        IPIntervalNode<Void>[] children = createLeaves(cidrs);
        IPIntervalNode<Void> internalNode = createInternalNode(parentCidr, children);

        assertThat(internalNode.left()).isEqualTo(children);
        assertThat(internalNode.right()).isNull();
    }

    @Test
    public void testInternalNodeAllRight()
    {
        List<String> cidrs = Arrays.asList("10.0.0.1/24",
                                           "10.0.1.1/24",
                                           "10.0.2.1/24",
                                           "10.0.3.1/24");
        String parentCidr = "9.0.0.1/30";

        IPIntervalNode<Void>[] children = createLeaves(cidrs);
        IPIntervalNode<Void> internalNode = createInternalNode(parentCidr, children);

        assertThat(internalNode.left()).isNull();
        assertThat(internalNode.right()).isEqualTo(children);
    }

    @Test
    public void testInternalNodeNoOverlapWithParent() // parent node does not overlap with closest
    {
        List<String> cidrs = Arrays.asList("10.0.0.1/24",
                                           "10.0.1.1/24",
                                           "10.0.3.1/24",
                                           "10.0.4.1/24");
        String parentCidr = "10.0.2.1/30";

        IPIntervalNode<Void>[] children = createLeaves(cidrs);
        IPIntervalNode<Void> internalNode = new IPIntervalNode<>(CIDR.getInstance(parentCidr), null, children);

        assertThat(toCidrs(internalNode.left())).describedAs("Check left nodes")
                                                .isEqualTo(toCidrs(createLeaves(Arrays.asList("10.0.0.1/24",
                                                                                              "10.0.1.1/24"))));

        assertThat(toCidrs(internalNode.right())).describedAs("Check right nodes")
                                                 .isEqualTo(toCidrs(createLeaves(Arrays.asList("10.0.3.1/24",
                                                                                               "10.0.4.1/24"))));
    }

    @Test
    public void testInternalNodeNoOverlapWithParentIpv6() // parent node does not overlap with closest
    {
        List<String> cidrs = Arrays.asList("aaaa::0001:ffff/112",
                                           "bbbb::0001:ffff/112",
                                           "dddd::0001:ffff/112",
                                           "eeee::0001:ffff/112");
        String parentCidr = "cccc::0001:ffff/113";

        IPIntervalNode<Void>[] children = createLeaves(cidrs);
        IPIntervalNode<Void> internalNode = new IPIntervalNode<>(CIDR.getInstance(parentCidr), null, children);

        assertThat(toCidrs(internalNode.left())).describedAs("Check left nodes")
                                                .isEqualTo(toCidrs(createLeaves(Arrays.asList("aaaa::0001:ffff/112",
                                                                                              "bbbb::0001:ffff/112"))));

        assertThat(toCidrs(internalNode.right())).describedAs("Check right nodes")
                                                 .isEqualTo(toCidrs(createLeaves(
                                                 Arrays.asList("dddd::0001:ffff/112", "eeee::0001:ffff/112"))));
    }

    @Test
    public void testInternalNodeOverlapWithParent() // parent node overlaps with closest
    {
        List<String> cidrs = Arrays.asList("10.0.0.1/24",
                                           "10.0.1.1/24",
                                           "10.0.2.1/24",
                                           "10.0.3.1/24");
        String parentCidr = "10.0.2.1/25"; // overlaps with "10.0.2.1/24"

        IPIntervalNode<Void>[] children = createLeaves(cidrs);
        IPIntervalNode<Void> internalNode = new IPIntervalNode<>(CIDR.getInstance(parentCidr), null, children);

        // The overlapping node "10.0.2.1/24" should be included in both left and right
        assertThat(toCidrs(internalNode.left())).describedAs("Check left nodes")
                                                .isEqualTo(toCidrs(createLeaves(Arrays.asList("10.0.0.1/24",
                                                                                              "10.0.1.1/24",
                                                                                              "10.0.2.1/24"))));

        assertThat(toCidrs(internalNode.right())).describedAs("Check right nodes")
                                                 .isEqualTo(toCidrs(createLeaves(Arrays.asList("10.0.2.1/24",
                                                                                               "10.0.3.1/24"))));
    }

    @Test
    public void testInternalNodeOverlapWithParentIpv6() // parent node overlaps with closest - IPv6
    {
        List<String> cidrs = Arrays.asList("aaaa::0001:ffff/112",
                                           "bbbb::0001:ffff/112",
                                           "cccc::0001:ffff/112",
                                           "dddd::0001:ffff/112",
                                           "eeee::0001:ffff/112");
        String parentCidr = "cccc::0001:ffff/113"; // overlaps with "cccc::0001:ffff/112"

        IPIntervalNode<Void>[] children = createLeaves(cidrs);
        IPIntervalNode<Void> internalNode = new IPIntervalNode<>(CIDR.getInstance(parentCidr), null, children);

        // The overlapping node "10.0.2.1/24" should be included in both left and right
        assertThat(toCidrs(internalNode.left())).describedAs("Check left nodes")
                                                .isEqualTo(toCidrs(createLeaves(Arrays.asList("aaaa::0001:ffff/112",
                                                                                              "bbbb::0001:ffff/112",
                                                                                              "cccc::0001:ffff/112"))));

        assertThat(toCidrs(internalNode.right())).describedAs("Check right nodes")
                                                 .isEqualTo(toCidrs(createLeaves(
                                                 Arrays.asList("cccc::0001:ffff/112",
                                                               "dddd::0001:ffff/112",
                                                               "eeee::0001:ffff/112"))));
    }

    @Test
    public void testSearchReturnsExactMatch() throws UnknownHostException
    {
        List<String> cidrs = Arrays.asList("10.0.0.1/24",
                                           "10.0.1.1/24",
                                           "10.0.2.1/24",
                                           "10.0.3.1/24");

        IPIntervalNode<Void>[] nodes = createLeaves(cidrs);

        // exhaust all IPs covered by the CIDRs list
        for (int i = 0; i < cidrs.size(); i++)
        {
            for (int j = 1; j < 256; j++)
            {
                String ip = String.format("10.0.%d.%d", i, j);
                int index = IPIntervalNode.binarySearchNodesIndex(nodes, InetAddress.getByName(ip));
                assertThat(index).isEqualTo(i);
            }
        }
    }

    @Test
    public void testSearchReturnsClosest() throws UnknownHostException
    {
        List<String> cidrs = Arrays.asList("10.0.0.1/24",
                                           "10.0.2.1/24",
                                           "10.0.4.1/24",
                                           "10.0.6.1/24",
                                           "10.0.8.1/24"
        );

        IPIntervalNode<Void>[] nodes = createLeaves(cidrs);

        for (int i = 1, expected = 0; i < 10; i++)
        {
            if (i % 2 == 0) // skip the even values as they are in CIDRs
                continue;

            for (int j = 1; j < 256; j++)
            {
                String ip = String.format("10.0.%d.%d", i, j);
                int index = IPIntervalNode.binarySearchNodesIndex(nodes, InetAddress.getByName(ip));
                assertThat(index).describedAs("The closest node is left to the ip").isEqualTo(expected);
            }
            expected++;
        }
    }
}
