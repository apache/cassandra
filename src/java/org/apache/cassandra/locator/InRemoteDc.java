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

import java.util.function.Predicate;

import static org.apache.cassandra.config.DatabaseDescriptor.getEndpointSnitch;
import static org.apache.cassandra.utils.FBUtilities.getTargetRemoteDcOrLocal;

public class InRemoteDc
{
    private static ReplicaTester replicas;
    private static EndpointTester endpoints;

    final String remoteDc;
    final IEndpointSnitch snitch;

    private InRemoteDc(String remoteDc, IEndpointSnitch snitch)
    {
        this.remoteDc = remoteDc;
        this.snitch = snitch;
    }

    boolean stale()
    {
        return remoteDc == null
                || !remoteDc.equals(getTargetRemoteDcOrLocal())
                || snitch != getEndpointSnitch();
    }

    private static final class ReplicaTester extends InRemoteDc implements Predicate<Replica>
    {
        private ReplicaTester(String remoteDc, IEndpointSnitch snitch)
        {
            super(remoteDc, snitch);
        }

        @Override
        public boolean test(Replica replica)
        {
            return remoteDc != null && remoteDc.equals(snitch.getDatacenter(replica.endpoint()));
        }
    }

    private static final class EndpointTester extends InRemoteDc implements Predicate<InetAddressAndPort>
    {
        private EndpointTester(String remoteDc, IEndpointSnitch snitch)
        {
            super(remoteDc, snitch);
        }

        @Override
        public boolean test(InetAddressAndPort endpoint)
        {
            return remoteDc != null && remoteDc.equals(snitch.getDatacenter(endpoint));
        }
    }

    public static Predicate<Replica> replicas()
    {
        ReplicaTester cur = replicas;
        if (cur == null || cur.stale())
            replicas = cur = new ReplicaTester(getTargetRemoteDcOrLocal(), getEndpointSnitch());
        return cur;
    }

    public static Predicate<InetAddressAndPort> endpoints()
    {
        EndpointTester cur = endpoints;
        if (cur == null || cur.stale())
            endpoints = cur = new EndpointTester(getTargetRemoteDcOrLocal(), getEndpointSnitch());
        return cur;
    }

    public static boolean isInRemoteDc(Replica replica)
    {
        return replicas().test(replica);
    }

    public static boolean isInRemoteDc(InetAddressAndPort endpoint)
    {
        return endpoints().test(endpoint);
    }
}
