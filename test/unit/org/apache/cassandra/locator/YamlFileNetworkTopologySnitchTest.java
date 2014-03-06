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
package org.apache.cassandra.locator;

import java.net.InetAddress;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.net.InetAddresses;

/**
 * Unit tests for {@link YamlFileNetworkTopologySnitch}.
 */
public class YamlFileNetworkTopologySnitchTest
{

    /**
     * Testing variant of {@link YamlFileNetworkTopologySnitch}.
     * 
     */
    private class TestYamlFileNetworkTopologySnitch
            extends YamlFileNetworkTopologySnitch
    {

        /**
         * Constructor.
         * 
         * @throws ConfigurationException
         *             on configuration error
         */
        public TestYamlFileNetworkTopologySnitch(
                final String topologyConfigFilename)
                throws ConfigurationException
        {
            super(topologyConfigFilename);
        }
    }

    /**
     * A basic test case.
     * 
     * @throws Exception
     *             on failure
     */
    @Test
    public void testBasic() throws Exception
    {
        final TestYamlFileNetworkTopologySnitch snitch = new TestYamlFileNetworkTopologySnitch(
                "cassandra-topology.yaml");
        checkEndpoint(snitch, FBUtilities.getBroadcastAddress()
                .getHostAddress(), "DC1", "RAC1");
        checkEndpoint(snitch, "192.168.1.100", "DC1", "RAC1");
        checkEndpoint(snitch, "10.0.0.12", "DC1", "RAC2");
        checkEndpoint(snitch, "127.0.0.3", "DC1", "RAC3");
        checkEndpoint(snitch, "10.20.114.10", "DC2", "RAC1");
        checkEndpoint(snitch, "127.0.0.8", "DC3", "RAC8");
        checkEndpoint(snitch, "6.6.6.6", "DC1", "r1");

    }

    /**
     * Asserts that a snitch's determination of data center and rack for an endpoint match what we expect.
     * 
     * @param snitch
     *            snitch
     * @param endpointString
     *            endpoint address as a string
     * @param expectedDatacenter
     *            expected data center
     * @param expectedRack
     *            expected rack
     */
    public static void checkEndpoint(final AbstractNetworkTopologySnitch snitch,
            final String endpointString, final String expectedDatacenter,
            final String expectedRack)
    {
        final InetAddress endpoint = InetAddresses.forString(endpointString);
        Assert.assertEquals(expectedDatacenter, snitch.getDatacenter(endpoint));
        Assert.assertEquals(expectedRack, snitch.getRack(endpoint));
    }

}
