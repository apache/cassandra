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

package org.apache.cassandra.service.accord.txn;

import java.util.HashMap;
import java.util.List;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

import accord.api.DataResolver;
import accord.api.Read;
import accord.api.ResolveResult;
import accord.api.UnresolvedData;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.service.accord.txn.TxnUnresolvedData.UnresolvedDataEntry;
import org.apache.cassandra.service.reads.repair.ReadRepair;
import org.apache.cassandra.service.reads.repair.ReadRepairStrategy;

import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class TxnDataResolver implements DataResolver
{
    @Override
    public ResolveResult resolve(Read read, UnresolvedData data)
    {
        // TODO get the real value
        long queryStartNanos = nanoTime();
        TxnRead txnRead = (TxnRead)read;
        TxnUnresolvedData unresolvedData = (TxnUnresolvedData)data;
        TxnData txnData = new TxnData(new HashMap<>(unresolvedData.size()));
        ListMultimap<TxnDataName, UnresolvedDataEntry> readToUnresolvedEntries = ArrayListMultimap.create(unresolvedData.size(), 3);
        for (UnresolvedDataEntry e : unresolvedData)
        {
            TxnNamedRead txnNamedRead = getTxnNamedRead(txnRead, e.txnDataName);
            readToUnresolvedEntries.put(e.txnDataName, e);
            SinglePartitionReadCommand command = ((SinglePartitionReadCommand)txnNamedRead.get()).withNowInSec(e.nowInSec);
            try (UnfilteredPartitionIterator partition = e.readResponse.makeIterator(command);
                 PartitionIterator iterator = UnfilteredPartitionIterators.filter(partition, e.nowInSec))
            {
                if (iterator.hasNext())
                {
                    FilteredPartition filtered = FilteredPartition.create(iterator.next());
                    if (filtered.hasRows() || command.selectsFullPartition())
                        txnData.put(e.txnDataName, filtered);
                }
            }
        }

        for (TxnDataName txnDataName : readToUnresolvedEntries.keys())
        {
            TxnNamedRead txnNamedRead = getTxnNamedRead(txnRead, txnDataName);
            List<UnresolvedDataEntry> entries = readToUnresolvedEntries.get(txnDataName);
            // TODO Should be fine to use nowInSec from any Accord command store?
            SinglePartitionReadCommand command = ((SinglePartitionReadCommand)txnNamedRead.get()).withNowInSec(entries.get(0).nowInSec);
            ReadRepair readRepair = ReadRepairStrategy.BLOCKING.create(command, null, queryStartNanos);
            org.apache.cassandra.service.reads.DataResolver dataResolver = new org.apache.cassandra.service.reads.DataResolver(command, () -> null, readRepair, queryStartNanos);
        }
        return new ResolveResult(txnData, null);
    }

    private static TxnNamedRead getTxnNamedRead(TxnRead txnRead, TxnDataName name)
    {
        for (TxnNamedRead read : txnRead)
        {
            if (read.txnDataName().equals(name))
            {
                return read;
            }
        }
        return null;
    }
}
