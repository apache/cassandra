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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.FBUtilities;

public class ReplicationFactor
{
    public static final ReplicationFactor ZERO = new ReplicationFactor(0);

    public final int allReplicas;
    public final int fullReplicas;

    private ReplicationFactor(int allReplicas, int witnessReplicas)
    {
        validate(allReplicas, witnessReplicas);
        this.allReplicas = allReplicas;
        this.fullReplicas = allReplicas - witnessReplicas;
    }

    public int witnessReplicas()
    {
        return allReplicas - fullReplicas;
    }

    public boolean hasWitnessReplicas()
    {
        return allReplicas != fullReplicas;
    }

    private ReplicationFactor(int allReplicas)
    {
        this(allReplicas, 0);
    }

    static void validate(int totalRF, int witnessRF)
    {
        Preconditions.checkArgument(witnessRF == 0 || DatabaseDescriptor.isWitnessReplicationEnabled(),
                                    "Witness replication is not enabled on this node");
        Preconditions.checkArgument(totalRF >= 0,
                                    "Replication factor must be non-negative, found %s", totalRF);
        Preconditions.checkArgument(witnessRF == 0 || witnessRF < totalRF,
                                    "Witness replicas must be zero, or less than total replication factor. For %s/%s", totalRF, witnessRF);
        if (witnessRF > 0)
        {
            Preconditions.checkArgument(DatabaseDescriptor.getNumTokens() == 1,
                                        "Witness nodes are not allowed with multiple tokens");
            Stream<InetAddressAndPort> endpoints = Stream.concat(Gossiper.instance.getLiveMembers().stream(), Gossiper.instance.getUnreachableMembers().stream());
            List<InetAddressAndPort> badVersionEndpoints = endpoints.filter(Predicates.not(FBUtilities.getBroadcastAddressAndPort()::equals))
                                                                    .filter(endpoint -> Gossiper.instance.getReleaseVersion(endpoint) != null && Gossiper.instance.getReleaseVersion(endpoint).major < 4)
                                                                    .collect(Collectors.toList());
            if (!badVersionEndpoints.isEmpty())
                throw new IllegalArgumentException("Witness replication is not supported in mixed version clusters with nodes < 4.0. Bad nodes: " + badVersionEndpoints);
        }
        else if (witnessRF < 0)
        {
            throw new IllegalArgumentException(String.format("Amount of witness nodes should be strictly positive, but was: '%d'", witnessRF));
        }
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReplicationFactor that = (ReplicationFactor) o;
        return allReplicas == that.allReplicas && fullReplicas == that.fullReplicas;
    }

    public int hashCode()
    {
        return Objects.hash(allReplicas, fullReplicas);
    }

    public static ReplicationFactor fullOnly(int totalReplicas)
    {
        return new ReplicationFactor(totalReplicas);
    }

    public static ReplicationFactor withWitness(int totalReplicas, int witnessReplicas)
    {
        return new ReplicationFactor(totalReplicas, witnessReplicas);
    }

    public static ReplicationFactor fromString(String s)
    {
        if (s.contains("/"))
        {
            String[] parts = s.split("/");
            Preconditions.checkArgument(parts.length == 2,
                                        "Replication factor format is <replicas> or <replicas>/<witness>");
            return new ReplicationFactor(Integer.parseInt(parts[0]), Integer.parseInt(parts[1]));
        }
        else
        {
            return new ReplicationFactor(Integer.parseInt(s), 0);
        }
    }

    public String toParseableString()
    {
        return allReplicas + (hasWitnessReplicas() ? "/" + witnessReplicas() : "");
    }

    @Override
    public String toString()
    {
        return "rf(" + toParseableString() + ')';
    }
}
