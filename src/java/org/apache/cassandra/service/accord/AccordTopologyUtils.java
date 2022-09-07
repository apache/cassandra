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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import accord.topology.Shard;
import accord.topology.Topology;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.TokenMetadata;

public class AccordTopologyUtils
{
    private static Shard createShard(Range<Token> range, EndpointsForToken natural, EndpointsForToken pending)
    {
        return new Shard(new TokenRange(range),
                         natural.stream().map(EndpointMapping::getId).collect(Collectors.toList()),
                         natural.stream().map(EndpointMapping::getId).collect(Collectors.toSet()),
                         pending.stream().map(EndpointMapping::getId).collect(Collectors.toSet()));
    }

    public static Topology createTopology(long epoch, String keyspace, AbstractReplicationStrategy replication, TokenMetadata metadata)
    {
        Set<Token> tokenSet = new HashSet<>(metadata.sortedTokens());
        tokenSet.addAll(metadata.getBootstrapTokens().keySet());
        metadata.getMovingEndpoints().forEach(p -> tokenSet.add(p.left));

        List<Token> tokens = new ArrayList<>(tokenSet);
        tokens.sort(Comparator.naturalOrder());

        List<Range<Token>> ranges = new ArrayList<>(tokens.size());

        for (int i=0, mi=tokens.size(); i<mi; i++)
        {
            ranges.add(new Range<>(tokens.get(i > 0 ? i - 1 : mi - 1),
                                   tokens.get(i)));
        }

        ranges = Range.normalize(ranges);

        Shard[] shards = new Shard[ranges.size()];
        for (int i=0, mi=ranges.size(); i<mi; i++)
        {
            Range<Token> range = ranges.get(i);
            EndpointsForToken natural = replication.getNaturalReplicasForToken(range.right);
            EndpointsForToken pending = metadata.pendingEndpointsForToken(range.right, keyspace);
            shards[i] = createShard(range, natural, pending);
        }

        return new Topology(epoch, shards);
    }
}
