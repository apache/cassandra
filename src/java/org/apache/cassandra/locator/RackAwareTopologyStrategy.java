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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.locator.TokenMetadata.Topology;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;

public class RackAwareTopologyStrategy extends AbstractReplicationStrategy
{
    private static final Logger logger = LoggerFactory.getLogger(RackAwareTopologyStrategy.class);

    public static final String REPLICATION_FACTOR = "replication_factor";

    private final Map<String, ReplicationFactor> datacenters;
    private final ReplicationFactor aggregateRf;

    public RackAwareTopologyStrategy(String keyspaceName,
                                     TokenMetadata tokenMetadata,
                                     IEndpointSnitch snitch,
                                     Map<String, String> configOptions) throws ConfigurationException
    {
        super(keyspaceName, tokenMetadata, snitch, configOptions);

        int replicas = 0;
        int trans = 0;
        Map<String, ReplicationFactor> newDatacenters = new HashMap<>();

        if (configOptions != null)
        {
            for (Entry<String, String> entry : configOptions.entrySet())
            {
                String dc = entry.getKey();
                // prepareOptions should have transformed any "replication_factor" options by now
                if (dc.equalsIgnoreCase(REPLICATION_FACTOR))
                    throw new ConfigurationException(REPLICATION_FACTOR + " should not appear as an option at construction time for RackAwareTopologyStrategy");

                ReplicationFactor rf = ReplicationFactor.fromString(entry.getValue());
                replicas += rf.allReplicas;
                trans += rf.transientReplicas();
                newDatacenters.put(dc, rf);
            }
        }

        datacenters = Collections.unmodifiableMap(newDatacenters);
        aggregateRf = ReplicationFactor.withTransient(replicas, trans);
        logger.trace("Configured datacenter replicas are {}", FBUtilities.toString(datacenters));
    }

    /**
     * calculate endpoints in one pass through the tokens by tracking our progress in each DC.
     */
    @Override
    public EndpointsForRange calculateNaturalReplicas(Token searchToken, TokenMetadata tokenMetadata)
    {
        // we want to preserve insertion order so that the first added endpoint becomes primary
        ArrayList<Token> sortedTokens = tokenMetadata.sortedTokens();
        Token replicaEnd = TokenMetadata.firstToken(sortedTokens, searchToken);
        Token replicaStart = tokenMetadata.getPredecessor(replicaEnd);
        Range<Token> replicatedRange = new Range<>(replicaStart, replicaEnd);

        EndpointsForRange.Builder builder = new EndpointsForRange.Builder(replicatedRange);

        Map<Pair<String, String>, Integer> seenRacks = new HashMap<>();

        Topology topology = tokenMetadata.getTopology();
        // all endpoints in each DC, so we can check when we have exhausted all the members of a DC
        Multimap<String, InetAddressAndPort> allEndpoints = topology.getDatacenterEndpoints();
        // all racks in a DC so we can check when we have exhausted all racks in a DC
        Map<String, ImmutableMultimap<String, InetAddressAndPort>> racks = topology.getDatacenterRacks();
        assert !allEndpoints.isEmpty() && !racks.isEmpty() : "not aware of any cluster members";

        int dcsToFill = 0;
        Map<String, DatacenterEndpoints> dcs = new HashMap<>(datacenters.size() * 2);

        // Create a DatacenterEndpoints object for each non-empty DC.
        for (Map.Entry<String, ReplicationFactor> entry : datacenters.entrySet())
        {
            String dc = entry.getKey();
            ReplicationFactor rf = entry.getValue();
            int nodeCount = sizeOrZero(allEndpoints.get(dc));

            if (rf.allReplicas <= 0 || nodeCount <= 0)
                continue;

            DatacenterEndpoints dcEndpoints = new DatacenterEndpoints(rf, sizeOrZero(racks.get(dc)), nodeCount, builder, seenRacks);
            dcs.put(dc, dcEndpoints);
            ++dcsToFill;
        }

        Iterator<Token> tokenIterator = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), searchToken, false);

        while (dcsToFill > 0 && tokenIterator.hasNext())
        {
            Token token = tokenIterator.next();
            InetAddressAndPort endpointOfToken = tokenMetadata.getEndpoint(token);
            Pair<String, String> location = topology.getLocation(endpointOfToken);
            DatacenterEndpoints dcEndpoints = dcs.get(location.left);
            if (dcEndpoints != null && dcEndpoints.addEndpointAndCheckIfDone(endpointOfToken, location, replicatedRange))
                --dcsToFill;
        }

        return builder.build();
    }

    /**
     * Endpoint adder applying the replication rules for a given DC.
     */
    private static final class DatacenterEndpoints
    {
        /**
         * List accepted endpoints get pushed into.
         */
        EndpointsForRange.Builder replicas;

        /**
         * Racks encountered so far. Replicas are put into separate racks while possible.
         * For efficiency the set is shared between the instances, using the location pair (dc, rack) to make sure
         * clashing names are not a problem.
         */
        Map<Pair<String, String>, Integer> racks;

        /**
         * Number of replicas left to fill from this DC.
         */
        int rfLeft;
        int replicasPerRack;
        int transients;

        DatacenterEndpoints(ReplicationFactor rf,
                            int rackCount,
                            int nodeCount,
                            EndpointsForRange.Builder replicas,
                            Map<Pair<String, String>, Integer> racks)
        {
            this.replicas = replicas;
            this.racks = racks;
            // If there aren't enough nodes in this DC to fill the RF, the number of nodes is the effective RF.
            this.rfLeft = Math.min(rf.allReplicas, nodeCount);

            // if we have fewer replicas than rf calls for, reduce transients accordingly
            int reduceTransients = rf.allReplicas - this.rfLeft;
            transients = Math.max(rf.transientReplicas() - reduceTransients, 0);
            ReplicationFactor.validate(rfLeft, transients);

            // Number of replicas to fill in a rack.
            if (rackCount == 0)
            {
                replicasPerRack = 1;
            }
            else if (rf.allReplicas % rackCount == 0)
            {
                replicasPerRack = rf.allReplicas / rackCount;
            }
            else
            {
                replicasPerRack = (rf.allReplicas / rackCount) + 1;
            }
        }

        /**
         * Attempts to add an endpoint to the replicas for this datacenter, adding to the endpoints set if successful.
         * Returns true if the endpoint was added, and this datacenter does not require further replicas.
         */
        boolean addEndpointAndCheckIfDone(InetAddressAndPort ep, Pair<String, String> location, Range<Token> replicatedRange)
        {
            if (done())
                return false;

            Replica replica = new Replica(ep, replicatedRange, rfLeft > transients);

            Integer rackReplicaCount = racks.get(location);
            if (rackReplicaCount == null)
                rackReplicaCount = 0;
            if (rackReplicaCount < replicasPerRack)
            {
                try
                {
                    replicas.add(replica, ReplicaCollection.Builder.Conflict.NONE);
                }
                catch (Exception ex)
                {
                    return false;
                }
                racks.put(location, rackReplicaCount + 1);
                --rfLeft;
            }
            return done();
        }

        boolean done()
        {
            assert rfLeft >= 0;
            return rfLeft == 0;
        }
    }

    private int sizeOrZero(Multimap<?, ?> collection)
    {
        return collection != null ? collection.asMap().size() : 0;
    }

    private int sizeOrZero(Collection<?> collection)
    {
        return collection != null ? collection.size() : 0;
    }

    public ReplicationFactor getReplicationFactor()
    {
        return aggregateRf;
    }

    public ReplicationFactor getReplicationFactor(String dc)
    {
        ReplicationFactor replicas = datacenters.get(dc);
        return replicas == null ? ReplicationFactor.ZERO : replicas;
    }

    public Set<String> getDatacenters()
    {
        return datacenters.keySet();
    }

    public void validateOptions() throws ConfigurationException
    {
        for (Entry<String, String> e : this.configOptions.entrySet())
        {
            // prepareOptions should have transformed any "replication_factor" by now
            if (e.getKey().equalsIgnoreCase(REPLICATION_FACTOR))
                throw new ConfigurationException(REPLICATION_FACTOR + " should not appear as an option to RackAwareTopologyStrategy");
            validateReplicationFactor(e.getValue());
        }
    }

    @Override
    public void maybeWarnOnOptions()
    {

    }

    @Override
    public boolean hasSameSettings(AbstractReplicationStrategy other)
    {
        if (!super.hasSameSettings(other))
            return false;

        if (other instanceof RackAwareTopologyStrategy)
            return ((RackAwareTopologyStrategy) other).datacenters.equals(datacenters);

        return true;
    }
}
