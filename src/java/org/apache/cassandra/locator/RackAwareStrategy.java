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

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.net.EndPoint;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.LogUtil;

/*
 * This class returns the nodes responsible for a given
 * key but does respects rack awareness. It makes a best
 * effort to get a node from a different data center and
 * a node in a different rack in the same datacenter as
 * the primary.
 */
public class RackAwareStrategy extends AbstractStrategy
{
    public RackAwareStrategy(TokenMetadata tokenMetadata, IPartitioner partitioner, int replicas, int storagePort)
    {
        super(tokenMetadata, partitioner, replicas, storagePort);
    }

    public EndPoint[] getStorageEndPoints(Token token)
    {
        int startIndex;
        List<EndPoint> list = new ArrayList<EndPoint>();
        boolean bDataCenter = false;
        boolean bOtherRack = false;
        int foundCount = 0;
        Map<Token, EndPoint> tokenToEndPointMap = tokenMetadata_.cloneTokenEndPointMap();
        List tokens = new ArrayList(tokenToEndPointMap.keySet());
        Collections.sort(tokens);
        int index = Collections.binarySearch(tokens, token);
        if(index < 0)
        {
            index = (index + 1) * (-1);
            if (index >= tokens.size())
                index = 0;
        }
        int totalNodes = tokens.size();
        // Add the node at the index by default
        list.add(tokenToEndPointMap.get(tokens.get(index)));
        foundCount++;
        if( replicas_ == 1 )
        {
            return list.toArray(new EndPoint[list.size()]);
        }
        startIndex = (index + 1)%totalNodes;
        IEndPointSnitch endPointSnitch = StorageService.instance().getEndPointSnitch();
        
        for (int i = startIndex, count = 1; count < totalNodes && foundCount < replicas_; ++count, i = (i+1)%totalNodes)
        {
            try
            {
                // First try to find one in a different data center
                if(!endPointSnitch.isInSameDataCenter(tokenToEndPointMap.get(tokens.get(index)), tokenToEndPointMap.get(tokens.get(i))))
                {
                    // If we have already found something in a diff datacenter no need to find another
                    if( !bDataCenter )
                    {
                        list.add(tokenToEndPointMap.get(tokens.get(i)));
                        bDataCenter = true;
                        foundCount++;
                    }
                    continue;
                }
                // Now  try to find one on a different rack
                if(!endPointSnitch.isOnSameRack(tokenToEndPointMap.get(tokens.get(index)), tokenToEndPointMap.get(tokens.get(i))) &&
                        endPointSnitch.isInSameDataCenter(tokenToEndPointMap.get(tokens.get(index)), tokenToEndPointMap.get(tokens.get(i))))
                {
                    // If we have already found something in a diff rack no need to find another
                    if( !bOtherRack )
                    {
                        list.add(tokenToEndPointMap.get(tokens.get(i)));
                        bOtherRack = true;
                        foundCount++;
                    }
                }
            }
            catch (UnknownHostException e)
            {
                if (logger_.isDebugEnabled())
                  logger_.debug(LogUtil.throwableToString(e));
            }

        }
        // If we found N number of nodes we are good. This loop wil just exit. Otherwise just
        // loop through the list and add until we have N nodes.
        for (int i = startIndex, count = 1; count < totalNodes && foundCount < replicas_; ++count, i = (i+1)%totalNodes)
        {
            if( ! list.contains(tokenToEndPointMap.get(tokens.get(i))))
            {
                list.add(tokenToEndPointMap.get(tokens.get(i)));
                foundCount++;
            }
        }
        retrofitPorts(list);
        return list.toArray(new EndPoint[list.size()]);
    }
    
    public Map<String, EndPoint[]> getStorageEndPoints(String[] keys)
    {
    	Map<String, EndPoint[]> results = new HashMap<String, EndPoint[]>();

        for ( String key : keys )
        {
            results.put(key, getStorageEndPoints(partitioner_.getInitialToken(key)));
        }

        return results;
    }

    public EndPoint[] getStorageEndPoints(Token token, Map<Token, EndPoint> tokenToEndPointMap)
    {
        throw new UnsupportedOperationException("This operation is not currently supported");
    }
}