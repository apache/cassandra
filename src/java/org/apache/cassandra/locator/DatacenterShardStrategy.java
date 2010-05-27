package org.apache.cassandra.locator;

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


import java.net.InetAddress;
import java.util.*;
import java.util.Map.Entry;

import com.google.common.collect.Multimap;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ResourceWatcher;
import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.WrappedRunnable;

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
public class DatacenterShardStrategy extends AbstractReplicationStrategy
{
    private static final String DATACENTER_PROPERTY_FILENAME = "datacenters.properties";
    private Map<String, List<Token>> dcTokens;
    private AbstractRackAwareSnitch snitch;
    private Map<String, Map<String, Integer>> datacenters = new HashMap<String, Map<String, Integer>>();

    public DatacenterShardStrategy(TokenMetadata tokenMetadata, IEndpointSnitch snitch) throws ConfigurationException
    {
        super(tokenMetadata, snitch);
        if ((!(snitch instanceof AbstractRackAwareSnitch)))
            throw new IllegalArgumentException("DatacenterShardStrategy requires a rack-aware endpointsnitch");
        this.snitch = (AbstractRackAwareSnitch)snitch;
        
        reloadConfiguration();
        Runnable runnable = new WrappedRunnable()
        {
            protected void runMayThrow() throws ConfigurationException
            {
                reloadConfiguration();
            }
        };
        ResourceWatcher.watch(DATACENTER_PROPERTY_FILENAME, runnable, 60 * 1000);

        loadEndpoints(tokenMetadata);
    }

    public void reloadConfiguration() throws ConfigurationException
    {
        Properties props = PropertyFileSnitch.resourceToProperties(DATACENTER_PROPERTY_FILENAME);
        for (Object key : props.keySet())
        {
            String[] keys = ((String)key).split(":");
            Map<String, Integer> map = datacenters.get(keys[0]);
            if (null == map)
            {
                map = new HashMap<String, Integer>();
            }
            map.put(keys[1], Integer.parseInt((String)props.get(key)));
            datacenters.put(keys[0], map);
        }
    }

    private synchronized void loadEndpoints(TokenMetadata metadata) throws ConfigurationException
    {
        String localDC = snitch.getDatacenter(DatabaseDescriptor.getListenAddress());
        if (localDC == null)
            throw new ConfigurationException("Invalid datacenter configuration; couldn't find local host " + FBUtilities.getLocalAddress());

        dcTokens = new HashMap<String, List<Token>>();
        for (Token token : metadata.sortedTokens())
        {
            InetAddress endPoint = metadata.getEndpoint(token);
            String dataCenter = snitch.getDatacenter(endPoint);
            // add tokens to dcmap.
            List<Token> lst = dcTokens.get(dataCenter);
            if (lst == null)
            {
                lst = new ArrayList<Token>();
            }
            lst.add(token);
            dcTokens.put(dataCenter, lst);
        }
        for (Entry<String, List<Token>> entry : dcTokens.entrySet())
        {
            List<Token> valueList = entry.getValue();
            Collections.sort(valueList);
            dcTokens.put(entry.getKey(), valueList);
        }

        // TODO verify that each DC has enough endpoints for the desired RF
    }

    public ArrayList<InetAddress> getNaturalEndpoints(Token searchToken, TokenMetadata metadata, String table)
    {
        ArrayList<InetAddress> endpoints = new ArrayList<InetAddress>();

        if (metadata.sortedTokens().isEmpty())
            return endpoints;

        for (String dc : dcTokens.keySet())
        {
            List<Token> tokens = dcTokens.get(dc);
            Set<String> racks = new HashSet<String>();
            // Add the node at the index by default
            Iterator<Token> iter = TokenMetadata.ringIterator(tokens, searchToken);
            InetAddress initialDCHost = metadata.getEndpoint(iter.next());
            assert initialDCHost != null;
            endpoints.add(initialDCHost);
            racks.add(snitch.getRack(initialDCHost));

            // find replicas on unique racks
            int replicas = getReplicationFactor(dc, table);
            int localEndpoints = 1;
            while (localEndpoints < replicas && iter.hasNext())
            {
                Token t = iter.next();
                InetAddress endpoint = metadata.getEndpoint(t);
                if (!racks.contains(snitch.getRack(endpoint)))
                {
                    endpoints.add(endpoint);
                    localEndpoints++;
                }
            }

            if (localEndpoints == replicas)
                continue;

            // if not enough unique racks were found, re-loop and add other endpoints
            iter = TokenMetadata.ringIterator(tokens, searchToken);
            iter.next(); // skip the first one since we already know it's used
            while (localEndpoints < replicas && iter.hasNext())
            {
                Token t = iter.next();
                if (!endpoints.contains(metadata.getEndpoint(t)))
                {
                    localEndpoints++;
                    endpoints.add(metadata.getEndpoint(t));
                }
            }
        }

        return endpoints;
    }

    public int getReplicationFactor(String dc, String table)
    {
        return datacenters.get(table).get(dc);
    }

    public Set<String> getDatacenters(String table)
    {
        return datacenters.get(table).keySet();
    }

    /**
     * This method will generate the QRH object and returns. If the Consistency
     * level is DCQUORUM then it will return a DCQRH with a map of local rep
     * factor alone. If the consistency level is DCQUORUMSYNC then it will
     * return a DCQRH with a map of all the DC rep factor.
     */
    @Override
    public AbstractWriteResponseHandler getWriteResponseHandler(Collection<InetAddress> writeEndpoints, Multimap<InetAddress, InetAddress> hintedEndpoints, ConsistencyLevel consistency_level, String table)
    {
        if (consistency_level == ConsistencyLevel.DCQUORUM)
        {
            // block for in this context will be localnodes block.
            return new DatacenterWriteResponseHandler(writeEndpoints, hintedEndpoints, consistency_level, table);
        }
        else if (consistency_level == ConsistencyLevel.DCQUORUMSYNC)
        {
            return new DatacenterSyncWriteResponseHandler(writeEndpoints, hintedEndpoints, consistency_level, table);
        }
        return super.getWriteResponseHandler(writeEndpoints, hintedEndpoints, consistency_level, table);
    }

    /**
     * This method will generate the WRH object and returns. If the Consistency
     * level is DCQUORUM/DCQUORUMSYNC then it will return a DCQRH.
     */
    @Override
    public QuorumResponseHandler getQuorumResponseHandler(IResponseResolver responseResolver, ConsistencyLevel consistencyLevel, String table)
    {
        if (consistencyLevel.equals(ConsistencyLevel.DCQUORUM) || consistencyLevel.equals(ConsistencyLevel.DCQUORUMSYNC))
        {
            return new DatacenterQuorumResponseHandler(responseResolver, consistencyLevel, table);
        }
        return super.getQuorumResponseHandler(responseResolver, consistencyLevel, table);
    }
}
