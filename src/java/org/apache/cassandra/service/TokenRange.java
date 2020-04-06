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
package org.apache.cassandra.service;

import java.util.*;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Holds token range informations for the sake of {@link StorageService#describeRing}.
 *
 * This class mostly exists for the sake of {@link StorageService#describeRing},
 * which used to rely on a thrift class which this is the equivalent of. This is
 * the reason this class behave how it does and the reason for the format
 * of {@code toString()} in particular (used by
 * {@link StorageService#describeRingJMX}). This class probably have no other
 * good uses than providing backward compatibility.
 */
public class TokenRange
{
    private final Token.TokenFactory tokenFactory;

    public final Range<Token> range;
    public final List<EndpointDetails> endpoints;

    private TokenRange(Token.TokenFactory tokenFactory, Range<Token> range, List<EndpointDetails> endpoints)
    {
        this.tokenFactory = tokenFactory;
        this.range = range;
        this.endpoints = endpoints;
    }

    private String toStr(Token tk)
    {
        return tokenFactory.toString(tk);
    }

    public static TokenRange create(Token.TokenFactory tokenFactory, Range<Token> range, List<InetAddressAndPort> endpoints, boolean withPorts)
    {
        List<EndpointDetails> details = new ArrayList<>(endpoints.size());
        IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
        for (InetAddressAndPort ep : endpoints)
            details.add(new EndpointDetails(ep,
                                            StorageService.instance.getNativeaddress(ep, withPorts),
                                            snitch.getDatacenter(ep),
                                            snitch.getRack(ep)));
        return new TokenRange(tokenFactory, range, details);
    }

    @Override
    public String toString()
    {
        return toString(false);
    }

    public String toString(boolean withPorts)
    {
        StringBuilder sb = new StringBuilder("TokenRange(");

        sb.append("start_token:").append(toStr(range.left));
        sb.append(", end_token:").append(toStr(range.right));

        List<String> hosts = new ArrayList<>(endpoints.size());
        List<String> rpcs = new ArrayList<>(endpoints.size());
        List<String> endpointDetails = new ArrayList<>(endpoints.size());
        for (EndpointDetails ep : endpoints)
        {
            hosts.add(ep.host.getHostAddress(withPorts));
            rpcs.add(ep.nativeAddress);
            endpointDetails.add(ep.toString(withPorts));
        }

        sb.append(", endpoints:").append(hosts);
        sb.append(", rpc_endpoints:").append(rpcs);
        sb.append(", endpoint_details:").append(endpointDetails);

        sb.append(")");
        return sb.toString();
    }

    public static class EndpointDetails
    {
        public final InetAddressAndPort host;
        public final String nativeAddress;
        public final String datacenter;
        public final String rack;

        private EndpointDetails(InetAddressAndPort host, String nativeAddress, String datacenter, String rack)
        {
            // dc and rack can be null, but host shouldn't
            assert host != null;
            this.host = host;
            this.nativeAddress = nativeAddress;
            this.datacenter = datacenter;
            this.rack = rack;
        }

        @Override
        public String toString()
        {
            return toString(false);
        }

        public String toString(boolean withPorts)
        {
            // Format matters for backward compatibility with describeRing()
            String dcStr = datacenter == null ? "" : String.format(", datacenter:%s", datacenter);
            String rackStr = rack == null ? "" : String.format(", rack:%s", rack);
            return String.format("EndpointDetails(host:%s%s%s)", host.getHostAddress(withPorts), dcStr, rackStr);
        }
    }
}
