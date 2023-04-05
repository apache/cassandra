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

package org.apache.cassandra.service.reads.range;

import java.net.UnknownHostException;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import org.apache.cassandra.Util;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

/**
 * Test utility class to set the partitioning tokens in the cluster.
 *
 * The per-endpoint tokens to be set can be specified with the {@code withTokens} and {@code withKeys} methods.
 * The {@link #update()} method will apply the changes, cleaning the previous token metadata info. It will also
 * initialize Gossip in all the endpoints but the local node.
 */
public class TokenUpdater
{
    private final Multimap<InetAddressAndPort, Token> endpointTokens = HashMultimap.create();

    public TokenUpdater withTokens(long... values)
    {
        return withTokens(localEndpoint(), values);
    }

    public TokenUpdater withTokens(String endpoint, long... values)
    {
        return withTokens(endpointByName(endpoint), values);
    }

    public TokenUpdater withTokens(InetAddressAndPort endpoint, long... values)
    {
        for (long val : values)
            endpointTokens.put(endpoint, new Murmur3Partitioner.LongToken(val));
        return this;
    }

    public TokenUpdater withKeys(int... keys)
    {
        return withKeys(localEndpoint(), keys);
    }

    public TokenUpdater withKeys(String endpoint, int... values)
    {
        return withKeys(endpointByName(endpoint), values);
    }

    public TokenUpdater withKeys(InetAddressAndPort endpoint, int... keys)
    {
        for (int key : keys)
            endpointTokens.put(endpoint, Util.token(key));
        return this;
    }

    public TokenUpdater withKeys(String... keys)
    {
        return withKeys(localEndpoint(), keys);
    }

    public TokenUpdater withKeys(InetAddressAndPort endpoint, String... keys)
    {
        for (String key : keys)
            endpointTokens.put(endpoint, Util.token(key));
        return this;
    }

    public TokenUpdater update()
    {
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.clearUnsafe();
        tmd.updateNormalTokens(endpointTokens);
        endpointTokens.keySet()
                      .stream()
                      .filter(e -> !e.equals(localEndpoint()))
                      .forEach(e -> Gossiper.instance.initializeNodeUnsafe(e, UUID.randomUUID(), 1));
        return this;
    }

    public List<Token> getTokens()
    {
        return getTokens(localEndpoint());
    }

    public List<Token> getTokens(InetAddressAndPort endpoint)
    {
        return ImmutableList.copyOf(endpointTokens.get(endpoint));
    }

    private static InetAddressAndPort localEndpoint()
    {
        return FBUtilities.getBroadcastAddressAndPort();
    }

    private static InetAddressAndPort endpointByName(String name)
    {
        try
        {
            return InetAddressAndPort.getByName(name);
        }
        catch (UnknownHostException e)
        {
            throw new RuntimeException(e);
        }
    }
}
