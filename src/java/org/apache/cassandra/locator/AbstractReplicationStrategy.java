/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.locator;

import java.util.*;

import org.apache.log4j.Logger;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;

/**
 * This class contains a helper method that will be used by
 * all abstraction that implement the IReplicaPlacementStrategy
 * interface.
*/
public abstract class AbstractReplicationStrategy
{
    protected static final Logger logger_ = Logger.getLogger(AbstractReplicationStrategy.class);

    protected TokenMetadata tokenMetadata_;
    protected IPartitioner partitioner_;
    protected int replicas_;
    protected int storagePort_;

    AbstractReplicationStrategy(TokenMetadata tokenMetadata, IPartitioner partitioner, int replicas, int storagePort)
    {
        tokenMetadata_ = tokenMetadata;
        partitioner_ = partitioner;
        replicas_ = replicas;
        storagePort_ = storagePort;
    }

    public abstract EndPoint[] getWriteStorageEndPoints(Token token);
    public abstract EndPoint[] getReadStorageEndPoints(Token token, Map<Token, EndPoint> tokenToEndPointMap);
    public abstract EndPoint[] getReadStorageEndPoints(Token token);

    /*
     * This method returns the hint map. The key is the endpoint
     * on which the data is being placed and the value is the
     * endpoint which is in the top N.
     * Get the map of top N to the live nodes currently.
     */
    public Map<EndPoint, EndPoint> getHintedStorageEndPoints(Token token)
    {
        return getHintedMapForEndpoints(getWriteStorageEndPoints(token));
    }

    /*
     * This method changes the ports of the endpoints from
     * the control port to the storage ports.
    */
    public void retrofitPorts(List<EndPoint> eps)
    {
        for ( EndPoint ep : eps )
        {
            ep.setPort(storagePort_);
        }
    }

    private Map<EndPoint, EndPoint> getHintedMapForEndpoints(EndPoint[] topN)
    {
        Set<EndPoint> usedEndpoints = new HashSet<EndPoint>();
        Map<EndPoint, EndPoint> map = new HashMap<EndPoint, EndPoint>();

        for (EndPoint ep : topN)
        {
            if (FailureDetector.instance().isAlive(ep))
            {
                map.put(ep, ep);
                usedEndpoints.add(ep);
            }
            else
            {
                // find another endpoint to store a hint on.  prefer endpoints that aren't already in use
                EndPoint hintLocation = null;
                Map<Token, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
                List tokens = new ArrayList(tokenToEndPointMap.keySet());
                Collections.sort(tokens);
                Token token = tokenMetadata_.getToken(ep);
                int index = Collections.binarySearch(tokens, token);
                if (index < 0)
                {
                    index = (index + 1) * (-1);
                    if (index >= tokens.size()) // handle wrap
                        index = 0;
                }
                int totalNodes = tokens.size();
                int startIndex = (index + 1) % totalNodes;
                for (int i = startIndex, count = 1; count < totalNodes; ++count, i = (i + 1) % totalNodes)
                {
                    EndPoint tmpEndPoint = tokenToEndPointMap.get(tokens.get(i));
                    if (FailureDetector.instance().isAlive(tmpEndPoint) && !Arrays.asList(topN).contains(tmpEndPoint) && !usedEndpoints.contains(tmpEndPoint))
                    {
                        hintLocation = tmpEndPoint;
                        break;
                    }
                }
                // if all endpoints are already in use, might as well store it locally to save the network trip
                if (hintLocation == null)
                    hintLocation = StorageService.getLocalControlEndPoint();

                map.put(hintLocation, ep);
                usedEndpoints.add(hintLocation);
            }
        }
        return map;
    }
}
