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

import static org.apache.cassandra.config.DatabaseDescriptor.getLocalDataCenter;
import static org.apache.cassandra.config.DatabaseDescriptor.getLocator;

public class InOurDc
{
    private static ReplicaTester replicas;
    private static EndpointTester endpoints;

    final String dc;
    final Locator locator;

    private InOurDc(String dc, Locator locator)
    {
        this.dc = dc;
        this.locator = locator;
    }

    boolean stale()
    {
        return !dc.equals(getLocalDataCenter());
    }

    private static final class ReplicaTester extends InOurDc implements Predicate<Replica>
    {
        private ReplicaTester(String dc, Locator locator)
        {
            super(dc, locator);
        }

        @Override
        public boolean test(Replica replica)
        {
            return dc.equals(locator.location(replica.endpoint()).datacenter);
        }
    }

    private static final class EndpointTester extends InOurDc implements Predicate<InetAddressAndPort>
    {
        private EndpointTester(String dc, Locator locator)
        {
            super(dc, locator);
        }

        @Override
        public boolean test(InetAddressAndPort endpoint)
        {
            return dc.equals(locator.location(endpoint).datacenter);
        }
    }

    public static Predicate<Replica> replicas()
    {
        ReplicaTester cur = replicas;
        if (cur == null || cur.stale())
            replicas = cur = new ReplicaTester(getLocalDataCenter(), getLocator());
        return cur;
    }

    public static Predicate<InetAddressAndPort> endpoints()
    {
        EndpointTester cur = endpoints;
        if (cur == null || cur.stale())
            endpoints = cur = new EndpointTester(getLocalDataCenter(), getLocator());
        return cur;
    }

    public static boolean isInOurDc(Replica replica)
    {
        return replicas().test(replica);
    }

    public static boolean isInOurDc(InetAddressAndPort endpoint)
    {
        return endpoints().test(endpoint);
    }

}
