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

package org.apache.cassandra.simulator.systems;

import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.locator.ReplicaCollection;
import org.apache.cassandra.simulator.cluster.NodeLookup;

public class SimulatedSnitch extends NodeLookup
{
    public static class Instance implements IEndpointSnitch
    {
        private static volatile Function<InetSocketAddress, String> LOOKUP_DC;

        public String getRack(InetAddressAndPort endpoint)
        {
            return LOOKUP_DC.apply(endpoint);
        }

        public String getDatacenter(InetAddressAndPort endpoint)
        {
            return LOOKUP_DC.apply(endpoint);
        }

        public <C extends ReplicaCollection<? extends C>> C sortedByProximity(InetAddressAndPort address, C addresses)
        {
            return addresses.sorted(Comparator.comparingInt(SimulatedSnitch::asInt));
        }

        public int compareEndpoints(InetAddressAndPort target, Replica r1, Replica r2)
        {
            return Comparator.comparingInt(SimulatedSnitch::asInt).compare(r1, r2);
        }

        public void gossiperStarting()
        {
        }

        public boolean isWorthMergingForRangeQuery(ReplicaCollection<?> merged, ReplicaCollection<?> l1, ReplicaCollection<?> l2)
        {
            return false;
        }

        public static void setup(Function<InetSocketAddress, String> lookupDc)
        {
            LOOKUP_DC = lookupDc;
        }
    }

    final int[] numInDcs;
    final String[] nameOfDcs;

    public SimulatedSnitch(int[] nodeToDc, int[] numInDcs)
    {
        super(nodeToDc);
        this.nameOfDcs = IntStream.range(0, numInDcs.length).mapToObj(i -> "dc" + i).toArray(String[]::new);
        this.numInDcs = numInDcs;
    }

    public int dcCount()
    {
        return nameOfDcs.length;
    }

    public String nameOfDc(int i)
    {
        return nameOfDcs[i];
    }

    public Cluster.Builder setup(Cluster.Builder builder)
    {
        for (int i = 0 ; i < numInDcs.length ; ++i)
            builder.withRack(nameOfDcs[i], nameOfDcs[i], numInDcs[i]);
        Consumer<IInstanceConfig> prev = builder.getConfigUpdater();
        return builder.withConfig(config -> {
            if (prev != null)
                prev.accept(config);
            config.set("endpoint_snitch", SimulatedSnitch.Instance.class.getName())
                  .set("dynamic_snitch", false);
        });
    }

    public Instance get()
    {
        return new Instance();
    }

    public void setup(Cluster cluster)
    {
        Function<InetSocketAddress, String> lookup = Cluster.getUniqueAddressLookup(cluster, i -> nameOfDcs[dcOf(i.config().num())])::get;
        cluster.forEach(i -> i.unsafeAcceptOnThisThread(Instance::setup, lookup));
        Instance.setup(lookup);
    }

    private static int asInt(Replica address)
    {
        byte[] bytes = address.endpoint().addressBytes;
        return bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24);
    }
}
