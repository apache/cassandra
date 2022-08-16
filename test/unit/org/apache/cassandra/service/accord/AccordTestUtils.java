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

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import accord.api.Data;
import accord.api.KeyRange;
import accord.api.Write;
import accord.local.Command;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.Status;
import accord.topology.KeyRanges;
import accord.topology.Shard;
import accord.topology.Topology;
import accord.txn.Ballot;
import accord.txn.Keys;
import accord.txn.Timestamp;
import accord.txn.Txn;
import accord.txn.TxnId;
import accord.txn.Writes;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.utils.FBUtilities;

public class AccordTestUtils
{
    public static Id localNodeId()
    {
        return EndpointMapping.endpointToId(FBUtilities.getBroadcastAddressAndPort());
    }

    public static Topology simpleTopology(TableId... tables)
    {
        Arrays.sort(tables, Comparator.naturalOrder());
        Id node = localNodeId();
        Shard[] shards = new Shard[tables.length];

        List<Id> nodes = Lists.newArrayList(node);
        Set<Id> fastPath = Sets.newHashSet(node);
        for (int i=0; i<tables.length; i++)
        {
            KeyRange range = TokenRange.fullRange(tables[i]);
            shards[i] = new Shard(range, nodes, fastPath, Collections.emptySet());
        }

        return new Topology(1, shards);
    }

    public static AccordTxnBuilder txnBuilder()
    {
        return new AccordTxnBuilder();
    }

    public static TxnId txnId(long epoch, long real, int logical, long node)
    {
        return new TxnId(epoch, real, logical, new Node.Id(node));
    }

    public static Timestamp timestamp(long epoch, long real, int logical, long node)
    {
        return new Timestamp(epoch, real, logical, new Node.Id(node));
    }

    public static Ballot ballot(long epoch, long real, int logical, long node)
    {
        return new Ballot(epoch, real, logical, new Node.Id(node));
    }

    /**
     * Return a KeyRanges containing the full range for each unique tableId contained in keys
     */
    public static KeyRanges fullRangesFromKeys(Keys keys)
    {
        TokenRange[] ranges = keys.stream()
                                  .map(AccordKey::of)
                                  .map(AccordKey::tableId)
                                  .distinct()
                                  .map(TokenRange::fullRange)
                                  .toArray(TokenRange[]::new);
        return new KeyRanges(ranges);
    }

    /**
     * does the reads, writes, and results for a command without the consensus
     */
    public static void processCommandResult(Command command)
    {

        Txn txn = command.txn();
        KeyRanges ranges = fullRangesFromKeys(txn.keys);
        Data read = txn.read.read(ranges, command.executeAt(), null);
        Write write = txn.update.apply(read);
        command.writes(new Writes(command.executeAt(), txn.keys(), write));
        command.result(txn.query.compute(read));
    }
}
