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


import java.io.IOException;
import java.io.IOError;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.Map.Entry;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.service.*;
import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * This Stategy is little diffrent than the Rack aware Statergy. If there is
 * replication factor is N. We will make sure that (N-1)%2 of the nodes are in
 * other Datacenter.... For example if we have 5 nodes this stategy will make
 * sure to make 2 copies out of 5 in other dataceneter.
 * <p/>
 * This class also caches the Endpoints and invalidates the cache if there is a
 * change in the number of tokens.
 */
public class DatacenterShardStategy extends AbstractReplicationStrategy
{
    private static Map<String, List<Token>> dcMap = new HashMap<String, List<Token>>();
    private static Map<String, Integer> dcReplicationFactor = new HashMap<String, Integer>();
    private static Map<String, Integer> quorumRepFactor = new HashMap<String, Integer>();
    private static int locQFactor = 0;
    ArrayList<Token> tokens;

    private List<InetAddress> localEndpoints = new ArrayList<InetAddress>();

    private List<InetAddress> getLocalEndpoints()
    {
        return new ArrayList<InetAddress>(localEndpoints);
    }

    private Map<String, Integer> getQuorumRepFactor()
    {
        return new HashMap<String, Integer>(quorumRepFactor);
    }

    /**
     * This Method will get the required information of the Endpoint from the
     * DataCenterEndpointSnitch and poopulates this singleton class.
     */
    private synchronized void loadEndpoints(TokenMetadata metadata) throws IOException
    {
        this.tokens = new ArrayList<Token>(metadata.sortedTokens());
        String localDC = ((DatacenterEndpointSnitch)snitch_).getDatacenter(InetAddress.getLocalHost());
        dcMap = new HashMap<String, List<Token>>();
        for (Token token : this.tokens)
        {
            InetAddress endpoint = metadata.getEndpoint(token);
            String dataCenter = ((DatacenterEndpointSnitch)snitch_).getDatacenter(endpoint);
            if (dataCenter.equals(localDC))
            {
                localEndpoints.add(endpoint);
            }
            List<Token> lst = dcMap.get(dataCenter);
            if (lst == null)
            {
                lst = new ArrayList<Token>();
            }
            lst.add(token);
            dcMap.put(dataCenter, lst);
        }
        for (Entry<String, List<Token>> entry : dcMap.entrySet())
        {
            List<Token> valueList = entry.getValue();
            Collections.sort(valueList);
            dcMap.put(entry.getKey(), valueList);
        }
        dcReplicationFactor = ((DatacenterEndpointSnitch)snitch_).getMapReplicationFactor();
        for (Entry<String, Integer> entry : dcReplicationFactor.entrySet())
        {
            String datacenter = entry.getKey();
            int qFactor = (entry.getValue() / 2 + 1);
            quorumRepFactor.put(datacenter, qFactor);
            if (datacenter.equals(localDC))
            {
                locQFactor = qFactor;
            }
        }
    }

    public DatacenterShardStategy(TokenMetadata tokenMetadata, IEndpointSnitch snitch)
    throws UnknownHostException
    {
        super(tokenMetadata, snitch);
        if ((!(snitch instanceof DatacenterEndpointSnitch)))
        {
            throw new IllegalArgumentException("DatacenterShardStrategy requires DatacenterEndpointSnitch");
        }
    }

    public ArrayList<InetAddress> getNaturalEndpoints(Token token, TokenMetadata metadata, String table)
    {
        try
        {
            return getNaturalEndpointsInternal(token, metadata);
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    private ArrayList<InetAddress> getNaturalEndpointsInternal(Token searchToken, TokenMetadata metadata) throws IOException
    {
        ArrayList<InetAddress> endpoints = new ArrayList<InetAddress>();

        if (metadata.sortedTokens().size() == 0)
            return endpoints;

        if (null == tokens || tokens.size() != metadata.sortedTokens().size())
        {
            loadEndpoints(metadata);
        }

        for (String dc : dcMap.keySet())
        {
            int replicas_ = dcReplicationFactor.get(dc);
            ArrayList<InetAddress> forloopReturn = new ArrayList<InetAddress>(replicas_);
            List<Token> tokens = dcMap.get(dc);
            boolean bOtherRack = false;
            boolean doneDataCenterItr;
            // Add the node at the index by default
            Iterator<Token> iter = TokenMetadata.ringIterator(tokens, searchToken);
            InetAddress primaryHost = metadata.getEndpoint(iter.next());
            forloopReturn.add(primaryHost);

            while (forloopReturn.size() < replicas_ && iter.hasNext())
            {
                Token t = iter.next();
                InetAddress endpointOfInterest = metadata.getEndpoint(t);
                if (forloopReturn.size() < replicas_ - 1)
                {
                    forloopReturn.add(endpointOfInterest);
                    continue;
                }
                else
                {
                    doneDataCenterItr = true;
                }

                // Now try to find one on a different rack
                if (!bOtherRack)
                {
                    AbstractRackAwareSnitch snitch = (AbstractRackAwareSnitch)snitch_;
                    if (!snitch.getRack(primaryHost).equals(snitch.getRack(endpointOfInterest)))
                    {
                        forloopReturn.add(metadata.getEndpoint(t));
                        bOtherRack = true;
                    }
                }
                // If both already found exit loop.
                if (doneDataCenterItr && bOtherRack)
                {
                    break;
                }
            }

            /*
            * If we found N number of nodes we are good. This loop wil just
            * exit. Otherwise just loop through the list and add until we
            * have N nodes.
            */
            if (forloopReturn.size() < replicas_)
            {
                iter = TokenMetadata.ringIterator(tokens, searchToken);
                while (forloopReturn.size() < replicas_ && iter.hasNext())
                {
                    Token t = iter.next();
                    if (!forloopReturn.contains(metadata.getEndpoint(t)))
                    {
                        forloopReturn.add(metadata.getEndpoint(t));
                    }
                }
            }
            endpoints.addAll(forloopReturn);
        }

        return endpoints;
    }

    /**
     * This method will generate the QRH object and returns. If the Consistency
     * level is DCQUORUM then it will return a DCQRH with a map of local rep
     * factor alone. If the consistency level is DCQUORUMSYNC then it will
     * return a DCQRH with a map of all the DC rep facor.
     */
    @Override
    public WriteResponseHandler getWriteResponseHandler(int blockFor, ConsistencyLevel consistency_level, String table)
    {
        if (consistency_level == ConsistencyLevel.DCQUORUM)
        {
            return new DatacenterWriteResponseHandler(locQFactor, table);
        }
        else if (consistency_level == ConsistencyLevel.DCQUORUMSYNC)
        {
            return new DatacenterSyncWriteResponseHandler(getQuorumRepFactor(), table);
        }
        return super.getWriteResponseHandler(blockFor, consistency_level, table);
    }
}
