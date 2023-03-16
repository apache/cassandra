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

package org.apache.cassandra.service.accord;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import accord.topology.Shard;
import accord.topology.Topology;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.SentinelKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.tcm.ClusterMetadata;

public class AccordTopologyUtils
{
    private static Shard createShard(TokenRange range, EndpointsForToken natural, EndpointsForToken pending)
    {
        return new Shard(range,
                         natural.stream().map(EndpointMapping::getId).collect(Collectors.toList()),
                         natural.stream().map(EndpointMapping::getId).collect(Collectors.toSet()),
                         pending.stream().map(EndpointMapping::getId).collect(Collectors.toSet()));
    }

    private static TokenRange minRange(String keyspace, Token token)
    {
        return new TokenRange(SentinelKey.min(keyspace), new TokenKey(keyspace, token));
    }

    private static TokenRange maxRange(String keyspace, Token token)
    {
        return new TokenRange(new TokenKey(keyspace, token), SentinelKey.max(keyspace));
    }

    private static TokenRange range(String keyspace, Token left, Token right)
    {
        return new TokenRange(new TokenKey(keyspace, left), new TokenKey(keyspace, right));
    }

    public static List<Shard> createShards(String keyspace, ClusterMetadata clusterMetadata)
    {
        KeyspaceMetadata keyspaceMetadata = Keyspace.open(keyspace).getMetadata();
        List<Token> tokens = new ArrayList<>(clusterMetadata.tokenMap.tokens());
        tokens.sort(Comparator.naturalOrder());

        List<Shard> shards = new ArrayList<>(tokens.size() + 1);
        Shard finalShard = null;
        for (int i = 0, mi = tokens.size(); i < mi; i++)
        {
            Token token = tokens.get(i);
            EndpointsForToken natural = clusterMetadata.placements.get(keyspaceMetadata.params.replication).reads.forToken(token);
            EndpointsForToken pending = clusterMetadata.pendingEndpointsFor(keyspaceMetadata, token);
            if (i == 0)
            {
                shards.add(createShard(minRange(keyspace, token), natural, pending));
                finalShard = createShard(maxRange(keyspace, tokens.get(mi - 1)), natural, pending);
            }
            else
            {
                Token prev = tokens.get(i - 1);
                shards.add(createShard(range(keyspace, prev, token), natural, pending));
            }
        }
        shards.add(finalShard);

        return shards;
    }

    public static Topology createTopology(long epoch)
    {
        List<String> keyspaces = new ArrayList<>(Schema.instance.distributedKeyspaces().names());
        keyspaces.sort(String::compareTo);

        List<Shard> shards = new ArrayList<>();
        for (String keyspace : keyspaces)
            shards.addAll(createShards(keyspace, ClusterMetadata.current()));

        return new Topology(epoch, shards.toArray(new Shard[0]));
    }
}
