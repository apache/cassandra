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
import java.util.Collections;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.dht.Token;

/**
 * This Replication Strategy returns the nodes responsible for a given
 * key but respects rack awareness. It places one replica in a
 * different data center from the first (if there is any such data center),
 * the third replica in a different rack in the first datacenter, and
 * any remaining replicas on the first unused nodes on the ring.
 */
public class OldNetworkTopologyStrategy extends AbstractReplicationStrategy
{
    private final ReplicationFactor rf;
    public OldNetworkTopologyStrategy(String keyspaceName, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions)
    {
        super(keyspaceName, tokenMetadata, snitch, configOptions);
        this.rf = ReplicationFactor.fromString(this.configOptions.get("replication_factor"));
    }

    public EndpointsForRange calculateNaturalReplicas(Token token, TokenMetadata metadata)
    {
        ArrayList<Token> tokens = metadata.sortedTokens();
        if (tokens.isEmpty())
            return EndpointsForRange.empty(new Range<>(metadata.partitioner.getMinimumToken(), metadata.partitioner.getMinimumToken()));

        Iterator<Token> iter = TokenMetadata.ringIterator(tokens, token, false);
        Token primaryToken = iter.next();
        Token previousToken = metadata.getPredecessor(primaryToken);
        Range<Token> tokenRange = new Range<>(previousToken, primaryToken);

        EndpointsForRange.Builder replicas = new EndpointsForRange.Builder(tokenRange, rf.allReplicas);

        assert !rf.hasTransientReplicas() : "support transient replicas";
        replicas.add(new Replica(metadata.getEndpoint(primaryToken), previousToken, primaryToken, true));

        boolean bDataCenter = false;
        boolean bOtherRack = false;
        while (replicas.size() < rf.allReplicas && iter.hasNext())
        {
            // First try to find one in a different data center
            Token t = iter.next();
            if (!snitch.getDatacenter(metadata.getEndpoint(primaryToken)).equals(snitch.getDatacenter(metadata.getEndpoint(t))))
            {
                // If we have already found something in a diff datacenter no need to find another
                if (!bDataCenter)
                {
                    replicas.add(new Replica(metadata.getEndpoint(t), previousToken, primaryToken, true));
                    bDataCenter = true;
                }
                continue;
            }
            // Now  try to find one on a different rack
            if (!snitch.getRack(metadata.getEndpoint(primaryToken)).equals(snitch.getRack(metadata.getEndpoint(t))) &&
                snitch.getDatacenter(metadata.getEndpoint(primaryToken)).equals(snitch.getDatacenter(metadata.getEndpoint(t))))
            {
                // If we have already found something in a diff rack no need to find another
                if (!bOtherRack)
                {
                    replicas.add(new Replica(metadata.getEndpoint(t), previousToken, primaryToken, true));
                    bOtherRack = true;
                }
            }

        }

        // If we found N number of nodes we are good. This loop wil just exit. Otherwise just
        // loop through the list and add until we have N nodes.
        if (replicas.size() < rf.allReplicas)
        {
            iter = TokenMetadata.ringIterator(tokens, token, false);
            while (replicas.size() < rf.allReplicas && iter.hasNext())
            {
                Token t = iter.next();
                Replica replica = new Replica(metadata.getEndpoint(t), previousToken, primaryToken, true);
                if (!replicas.endpoints().contains(replica.endpoint()))
                    replicas.add(replica);
            }
        }

        return replicas.build();
    }

    public ReplicationFactor getReplicationFactor()
    {
        return rf;
    }

    public void validateOptions() throws ConfigurationException
    {
        if (configOptions == null || configOptions.get("replication_factor") == null)
        {
            throw new ConfigurationException("SimpleStrategy requires a replication_factor strategy option.");
        }
        validateReplicationFactor(configOptions.get("replication_factor"));
    }

    public Collection<String> recognizedOptions()
    {
        return Collections.<String>singleton("replication_factor");
    }
}
