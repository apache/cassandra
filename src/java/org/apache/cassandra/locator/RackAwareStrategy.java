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
import java.util.List;

import org.apache.cassandra.dht.Token;
import java.net.InetAddress;
import org.apache.cassandra.service.StorageService;

/*
 * This class returns the nodes responsible for a given
 * key but does respects rack awareness. It makes a best
 * effort to get a node from a different data center and
 * a node in a different rack in the same datacenter as
 * the primary.
 */
public class RackAwareStrategy extends AbstractReplicationStrategy
{
    public RackAwareStrategy(TokenMetadata tokenMetadata, int replicas)
    {
        super(tokenMetadata, replicas);
    }

    public ArrayList<InetAddress> getNaturalEndpoints(Token token, TokenMetadata metadata)
    {
        int startIndex;
        ArrayList<InetAddress> endpoints = new ArrayList<InetAddress>();
        boolean bDataCenter = false;
        boolean bOtherRack = false;
        int foundCount = 0;
        List tokens = metadata.sortedTokens();

        if (tokens.isEmpty())
            return endpoints;

        int index = Collections.binarySearch(tokens, token);
        if(index < 0)
        {
            index = (index + 1) * (-1);
            if (index >= tokens.size())
                index = 0;
        }
        int totalNodes = tokens.size();
        // Add the node at the index by default
        Token primaryToken = (Token) tokens.get(index);
        endpoints.add(metadata.getEndPoint(primaryToken));
        foundCount++;
        if (replicas_ == 1)
        {
            return endpoints;
        }
        startIndex = (index + 1)%totalNodes;
        EndPointSnitch endPointSnitch = (EndPointSnitch) StorageService.instance.getEndPointSnitch();

        for (int i = startIndex, count = 1; count < totalNodes && foundCount < replicas_; ++count, i = (i + 1) % totalNodes)
        {
            try
            {
                // First try to find one in a different data center
                Token t = (Token) tokens.get(i);
                if (!endPointSnitch.isInSameDataCenter(metadata.getEndPoint(primaryToken), metadata.getEndPoint(t)))
                {
                    // If we have already found something in a diff datacenter no need to find another
                    if (!bDataCenter)
                    {
                        endpoints.add(metadata.getEndPoint(t));
                        bDataCenter = true;
                        foundCount++;
                    }
                    continue;
                }
                // Now  try to find one on a different rack
                if (!endPointSnitch.isOnSameRack(metadata.getEndPoint(primaryToken), metadata.getEndPoint(t)) &&
                    endPointSnitch.isInSameDataCenter(metadata.getEndPoint(primaryToken), metadata.getEndPoint(t)))
                {
                    // If we have already found something in a diff rack no need to find another
                    if (!bOtherRack)
                    {
                        endpoints.add(metadata.getEndPoint(t));
                        bOtherRack = true;
                        foundCount++;
                    }
                }
            }
            catch (UnknownHostException e)
            {
                throw new RuntimeException(e);
            }

        }
        // If we found N number of nodes we are good. This loop wil just exit. Otherwise just
        // loop through the list and add until we have N nodes.
        for (int i = startIndex, count = 1; count < totalNodes && foundCount < replicas_; ++count, i = (i+1)%totalNodes)
        {
            Token t = (Token) tokens.get(i);
            if (!endpoints.contains(metadata.getEndPoint(t)))
            {
                endpoints.add(metadata.getEndPoint(t));
                foundCount++;
            }
        }
        return endpoints;
    }
}
