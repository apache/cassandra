/*
 * 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 * 
 */

package org.apache.cassandra.locator;

import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Multimap;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * This Replication Strategy takes a property file that gives the intended
 * replication factor in each datacenter.  The sum total of the datacenter
 * replication factor values should be equal to the keyspace replication
 * factor.
 * <p/>
 * So for example, if the keyspace replication factor is 6, the
 * datacenter replication factors could be 3, 2, and 1 - so 3 replicas in
 * one datacenter, 2 in another, and 1 in another - totalling 6.
 * <p/>
 * This class also caches the Endpoints and invalidates the cache if there is a
 * change in the number of tokens.
 */
public class NetworkTopologyStrategy extends AbstractReplicationStrategy
{
    private AbstractNetworkTopologySnitch snitch;
    private Map<String, Integer> datacenters;
    private static final Logger logger = LoggerFactory.getLogger(NetworkTopologyStrategy.class);

    public NetworkTopologyStrategy(String table, TokenMetadata tokenMetadata, IEndpointSnitch snitch, Map<String, String> configOptions) throws ConfigurationException
    {
        super(table, tokenMetadata, snitch, configOptions);
        this.snitch = (AbstractNetworkTopologySnitch)snitch;

        Map<String, Integer> newDatacenters = new HashMap<String, Integer>();
        for (Entry entry : configOptions.entrySet())
        {
            newDatacenters.put((String) entry.getKey(), Integer.parseInt((String) entry.getValue()));
        }

        datacenters = Collections.unmodifiableMap(newDatacenters);
    }

    public Set<InetAddress> calculateNaturalEndpoints(Token searchToken, TokenMetadata tokenMetadata)
    {
        int totalReplicas = getReplicationFactor();
        Map<String, Integer> remainingReplicas = new HashMap<String, Integer>(datacenters);
        Map<String, Set<String>> dcUsedRacks = new HashMap<String, Set<String>>();
        Set<InetAddress> endpoints = new HashSet<InetAddress>(totalReplicas);

        // first pass: only collect replicas on unique racks
        for (Iterator<Token> iter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), searchToken);
             endpoints.size() < totalReplicas && iter.hasNext();)
        {
            Token token = iter.next();
            InetAddress endpoint = tokenMetadata.getEndpoint(token);
            String datacenter = snitch.getDatacenter(endpoint);
            int remaining = remainingReplicas.containsKey(datacenter) ? remainingReplicas.get(datacenter) : 0;
            if (remaining > 0)
            {
                Set<String> usedRacks = dcUsedRacks.get(datacenter);
                if (usedRacks == null)
                {
                    usedRacks = new HashSet<String>();
                    dcUsedRacks.put(datacenter, usedRacks);
                }
                String rack = snitch.getRack(endpoint);
                if (!usedRacks.contains(rack))
                {
                    endpoints.add(endpoint);
                    usedRacks.add(rack);
                    remainingReplicas.put(datacenter, remaining - 1);
                }
            }
        }

        // second pass: if replica count has not been achieved from unique racks, add nodes from the same racks
        for (Iterator<Token> iter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), searchToken);
             endpoints.size() < totalReplicas && iter.hasNext();)
        {
            Token token = iter.next();
            InetAddress endpoint = tokenMetadata.getEndpoint(token);
            if (endpoints.contains(endpoint))
                continue;

            String datacenter = snitch.getDatacenter(endpoint);
            int remaining = remainingReplicas.containsKey(datacenter) ? remainingReplicas.get(datacenter) : 0;
            if (remaining > 0)
            {
                endpoints.add(endpoint);
                remainingReplicas.put(datacenter, remaining - 1);
            }
        }

        for (Map.Entry<String, Integer> entry : remainingReplicas.entrySet())
        {
            if (entry.getValue() > 0)
                throw new IllegalStateException(String.format("datacenter (%s) has no more endpoints, (%s) replicas still needed", entry.getKey(), entry.getValue()));
        }

        return endpoints;
    }

    public int getReplicationFactor()
    {
        int total = 0;
        for (int repFactor : datacenters.values())
            total += repFactor;
        return total;
    }

    public int getReplicationFactor(String dc)
    {
        return datacenters.get(dc);
    }

    public Set<String> getDatacenters()
    {
        return datacenters.keySet();
    }

    /**
     * This method will generate the QRH object and returns. If the Consistency
     * level is DCQUORUM then it will return a DCQRH with a map of local rep
     * factor alone. If the consistency level is DCQUORUMSYNC then it will
     * return a DCQRH with a map of all the DC rep factor.
     */
    @Override
    public IWriteResponseHandler getWriteResponseHandler(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, ConsistencyLevel consistency_level)
    {
        if (consistency_level == ConsistencyLevel.DCQUORUM)
        {
            // block for in this context will be localnodes block.
            return DatacenterWriteResponseHandler.create(writeEndpoints, hintedEndpoints, consistency_level, table);
        }
        else if (consistency_level == ConsistencyLevel.DCQUORUMSYNC)
        {
            return DatacenterSyncWriteResponseHandler.create(writeEndpoints, hintedEndpoints, consistency_level, table);
        }
        return super.getWriteResponseHandler(writeEndpoints, hintedEndpoints, consistency_level);
    }

    /**
     * This method will generate the WRH object and returns. If the Consistency
     * level is DCQUORUM/DCQUORUMSYNC then it will return a DCQRH.
     */
    @Override
    public QuorumResponseHandler getQuorumResponseHandler(IResponseResolver responseResolver, ConsistencyLevel consistencyLevel)
    {
        if (consistencyLevel.equals(ConsistencyLevel.DCQUORUM) || consistencyLevel.equals(ConsistencyLevel.DCQUORUMSYNC))
        {
            return new DatacenterQuorumResponseHandler(responseResolver, consistencyLevel, table);
        }
        return super.getQuorumResponseHandler(responseResolver, consistencyLevel);
    }
}
