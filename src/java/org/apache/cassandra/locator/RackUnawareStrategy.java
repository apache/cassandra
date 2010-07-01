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

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Token;

/**
 * This class returns the nodes responsible for a given
 * key but does not respect rack awareness. Basically
 * returns the RF nodes that lie right next to each other
 * on the ring.
 */
public class RackUnawareStrategy extends AbstractReplicationStrategy
{
    public RackUnawareStrategy(TokenMetadata tokenMetadata, IEndpointSnitch snitch)
    {
        super(tokenMetadata, snitch);
    }

    public Set<InetAddress> calculateNaturalEndpoints(Token token, TokenMetadata metadata, String table)
    {
        int replicas = getReplicationFactor(table);
        ArrayList<Token> tokens = metadata.sortedTokens();
        Set<InetAddress> endpoints = new HashSet<InetAddress>(replicas);

        if (tokens.isEmpty())
            return endpoints;

        // Add the token at the index by default
        Iterator<Token> iter = TokenMetadata.ringIterator(tokens, token);
        while (endpoints.size() < replicas && iter.hasNext())
        {
            endpoints.add(metadata.getEndpoint(iter.next()));
        }

        if (endpoints.size() < replicas)
            throw new IllegalStateException(String.format("replication factor (%s) exceeds number of endpoints (%s)", replicas, endpoints.size()));
        
        return endpoints;
    }
}
