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

import com.google.common.base.Preconditions;

import accord.topology.Shard;
import accord.topology.Topology;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.StorageService;

public class AccordTopologyUtils
{
    private static Shard createShard(TableId tableId, Range<Token> range, EndpointsForToken natural, EndpointsForToken pending)
    {
        return new Shard(new TokenRange(tableId, range),
                         natural.stream().map(EndpointMapping::getId).collect(Collectors.toList()),
                         natural.stream().map(EndpointMapping::getId).collect(Collectors.toSet()),
                         pending.stream().map(EndpointMapping::getId).collect(Collectors.toSet()));
    }

    public static List<Shard> createShards(TableMetadata tableMetadata, TokenMetadata tokenMetadata)
    {
        String keyspace = tableMetadata.keyspace;
        AbstractReplicationStrategy replication = Keyspace.open(keyspace).getReplicationStrategy();
        Set<Token> tokenSet = new HashSet<>(tokenMetadata.sortedTokens());
        tokenSet.addAll(tokenMetadata.getBootstrapTokens().keySet());
        tokenMetadata.getMovingEndpoints().forEach(p -> tokenSet.add(p.left));
        IPartitioner partitioner = tableMetadata.partitioner;

        List<Token> tokens = new ArrayList<>(tokenSet);
        tokens.sort(Comparator.naturalOrder());

        List<Range<Token>> ranges = new ArrayList<>(tokens.size());

        Range<Token> finalRange = null;
        for (int i=0, mi=tokens.size(); i<mi; i++)
        {
            Range<Token> range = new Range<>(tokens.get(i > 0 ? i - 1 : mi - 1), tokens.get(i));
            if (range.isWrapAround())
            {
                Preconditions.checkArgument(finalRange == null);
                // FIXME: this will exclude the min token with the current range logic,
                //  with a set of unwrapped token ranges, the minimum token is actually owned by the highest rang (x, minToken]
                ranges.add(new Range<>(partitioner.getMinimumToken(), range.right));
                // FIXME: max token isn't supported by all partitioners
                finalRange = new Range<>(range.left, partitioner.getMaximumToken());
            }
            else
            {
                ranges.add(range);
            }

        }

        if (finalRange != null)
            ranges.add(finalRange);

        List<Shard> shards = new ArrayList<>(ranges.size());
        for (int i=0, mi=ranges.size(); i<mi; i++)
        {
            Range<Token> range = ranges.get(i);
            EndpointsForToken natural = replication.getNaturalReplicasForToken(range.right);
            EndpointsForToken pending = tokenMetadata.pendingEndpointsForToken(range.right, keyspace);
            shards.add(createShard(tableMetadata.id, range, natural, pending));
        }

        return shards;
    }

    public static Topology createTopology(long epoch)
    {
        List<TableId> tableIds = new ArrayList<>();
        TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
        for (String ksname: Schema.instance.getKeyspaces())
        {
            // TODO: add a table metadata flag to enable and enforce accord use
            if (SchemaConstants.LOCAL_SYSTEM_KEYSPACE_NAMES.contains(ksname))
                continue;
            if (SchemaConstants.REPLICATED_SYSTEM_KEYSPACE_NAMES.contains(ksname))
                continue;

            Keyspace keyspace = Keyspace.open(ksname);
            for (TableMetadata tableMetadata : keyspace.getMetadata().tables)
            {
                tableIds.add(tableMetadata.id);
            }
        }

        tableIds.sort(Comparator.naturalOrder());

        List<Shard> shards = new ArrayList<>();
        for (TableId tableId : tableIds)
        {
            TableMetadata tableMetadata = Schema.instance.getTableMetadata(tableId);
            Preconditions.checkNotNull(tableId);
            shards.addAll(createShards(tableMetadata, tokenMetadata));
        }

        return new Topology(epoch, shards.toArray(Shard[]::new));
    }
}
