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
import java.util.Iterator;
import java.util.List;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;
import java.net.InetAddress;

/*
 * This class returns the nodes responsible for a given
 * key but does respects rack awareness. It makes a best
 * effort to get a node from a different data center and
 * a node in a different rack in the same datacenter as
 * the primary.
 */
public class RackAwareStrategy extends AbstractReplicationStrategy
{
    public RackAwareStrategy(TokenMetadata tokenMetadata, IEndpointSnitch snitch)
    {
        super(tokenMetadata, snitch);
        if (!(snitch instanceof AbstractRackAwareSnitch))
            throw new IllegalArgumentException(("RackAwareStrategy requires AbstractRackAwareSnitch."));
    }

    public ArrayList<InetAddress> getNaturalEndpoints(Token token, TokenMetadata metadata, String table)
    {
        int replicas = DatabaseDescriptor.getReplicationFactor(table);
        ArrayList<InetAddress> endpoints = new ArrayList<InetAddress>(replicas);
        List<Token> tokens = metadata.sortedTokens();

        if (tokens.isEmpty())
            return endpoints;

        Iterator<Token> iter = TokenMetadata.ringIterator(tokens, token);
        Token primaryToken = iter.next();
        endpoints.add(metadata.getEndpoint(primaryToken));

        boolean bDataCenter = false;
        boolean bOtherRack = false;
        while (endpoints.size() < replicas && iter.hasNext())
        {
            try
            {
                // First try to find one in a different data center
                Token t = iter.next();
                if (!((AbstractRackAwareSnitch)snitch_).isInSameDataCenter(metadata.getEndpoint(primaryToken), metadata.getEndpoint(t)))
                {
                    // If we have already found something in a diff datacenter no need to find another
                    if (!bDataCenter)
                    {
                        endpoints.add(metadata.getEndpoint(t));
                        bDataCenter = true;
                    }
                    continue;
                }
                // Now  try to find one on a different rack
                if (!((AbstractRackAwareSnitch)snitch_).isOnSameRack(metadata.getEndpoint(primaryToken), metadata.getEndpoint(t)) &&
                    ((AbstractRackAwareSnitch)snitch_).isInSameDataCenter(metadata.getEndpoint(primaryToken), metadata.getEndpoint(t)))
                {
                    // If we have already found something in a diff rack no need to find another
                    if (!bOtherRack)
                    {
                        endpoints.add(metadata.getEndpoint(t));
                        bOtherRack = true;
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
        if (endpoints.size() < replicas)
        {
            iter = TokenMetadata.ringIterator(tokens, token);
            while (endpoints.size() < replicas && iter.hasNext())
            {
                Token t = iter.next();
                if (!endpoints.contains(metadata.getEndpoint(t)))
                    endpoints.add(metadata.getEndpoint(t));
            }
        }

        return endpoints;
    }
}
