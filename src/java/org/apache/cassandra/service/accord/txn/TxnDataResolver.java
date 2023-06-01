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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.DataResolver;
import accord.api.Read;
import accord.api.ResolveResult;
import accord.api.UnresolvedData;
import accord.local.Node.Id;
import accord.messages.Callback;
import accord.primitives.DataConsistencyLevel;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.locator.ReplicaPlans;
import org.apache.cassandra.service.accord.EndpointMapping;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.txn.TxnUnresolvedReadResponses.UnresolvedDataEntry;
import org.apache.cassandra.service.reads.CassandraFollowupReader;
import org.apache.cassandra.service.reads.DigestResolver;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.service.reads.repair.CollectingReadRepair;
import org.apache.cassandra.service.reads.repair.ReadRepair;

import static accord.utils.Invariants.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.cassandra.utils.Clock.Global.nanoTime;

public class TxnDataResolver implements DataResolver
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TxnDataResolver.class);

    // TODO Review: Should this do a better job of processing messages as they arrive vs waiting for everything before beginning resolution?
    @Override
    public AsyncChain<ResolveResult> resolve(Timestamp executeAt, Read read, UnresolvedData unresolvedData, FollowupReader followupReader)
    {
        TxnRead txnRead = (TxnRead)read;
        if (txnRead.readDataCL() == DataConsistencyLevel.UNSPECIFIED)
        {
            return AsyncChains.success(new ResolveResult((Data) unresolvedData, null));
        }
        else
        {
            // TODO Review: get the real value, not as simple as it sounds because recovery coordinators don't want the value from the command
            long queryStartNanos = nanoTime();
            return resolveWithRepair(executeAt, txnRead, (TxnUnresolvedReadResponses)unresolvedData, followupReader, queryStartNanos);
        }
    }

    private AsyncChain<ResolveResult> resolveWithRepair(Timestamp executeAt, TxnRead txnRead, TxnUnresolvedReadResponses unresolvedData, FollowupReader followupReader, long queryStartNanos)
    {
        ListMultimap<TxnNamedRead, UnresolvedDataEntry> readToUnresolvedEntries = ArrayListMultimap.create(unresolvedData.size(), 3);
        for (UnresolvedDataEntry e : unresolvedData)
        {
            TxnNamedRead txnNamedRead = getTxnNamedRead(txnRead, e.txnDataName);
            readToUnresolvedEntries.put(txnNamedRead, e);
        }

        Map<TxnDataName, ReadRepair> repairs = null;
        List<Mutation> repairMutations = null;
        TxnData resolvedData = new TxnData(Maps.newHashMapWithExpectedSize(txnRead.size()));
        for (Map.Entry<TxnNamedRead, Collection<UnresolvedDataEntry>> entry : readToUnresolvedEntries.asMap().entrySet())
        {
            TxnNamedRead txnNamedRead = entry.getKey();
            TxnDataName txnDataName = txnNamedRead.txnDataName();
            List<UnresolvedDataEntry> entries = (List<UnresolvedDataEntry>)entry.getValue();
            // TODO Review: Should be fine to use nowInSec from any Accord command store that was a replica?
            SinglePartitionReadCommand command = ((SinglePartitionReadCommand)txnNamedRead.get()).withNowInSec(entries.get(0).nowInSec);
            Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
            ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
            SpeculativeRetryPolicy retry = cfs.metadata().params.speculativeRetry;

            List<InetAddressAndPort> contacted = entries.stream().map(UnresolvedDataEntry::from).collect(toImmutableList());
            // TODO This should be done using the executeAt epoch, but can't without proper TCM
            ReplicaPlan.SharedForTokenRead replicaPlan = ReplicaPlan.shared(ReplicaPlans.forFollowupRead(keyspace, command.partitionKey().getToken(), txnRead.cassandraConsistencyLevel(), retry, contacted));
            CassandraFollowupReader cassandraFollowupReader = getCassandraFollowupReader(followupReader, txnRead, txnNamedRead);
            DigestResolver digestResolver = new DigestResolver(command, replicaPlan, queryStartNanos, cassandraFollowupReader);
            entries.forEach(digestResolver::preprocess);
            if (digestResolver.responsesMatch())
            {
                processPartitionIterator(digestResolver.getData(), command, resolvedData, txnDataName);
            }
            else
            {
                if (repairMutations == null)
                {
                    repairMutations = new ArrayList<>();
                    repairs = new HashMap<>();
                }
                CollectingReadRepair collectingReadRepair = new CollectingReadRepair(command, replicaPlan, queryStartNanos, repairMutations, cassandraFollowupReader);
                repairs.put(txnNamedRead.txnDataName(), collectingReadRepair);
            }
        }

        if (repairs == null)
        {
            return AsyncChains.success(new ResolveResult(resolvedData, null));
        }
        checkState(!repairs.isEmpty());

        Map<TxnDataName, ReadRepair> repairsFinal = repairs;
        List<Mutation> repairMutationsFinal = repairMutations;
        // TODO Review: Need to pick a thread pool to run this in. Normally it would be the request thread pool
        return AsyncChains.ofCallable(Stage.ACCORD_MIGRATION.executor(), () -> {
            repairsFinal.values().forEach(ReadRepair::startRepair);
            repairsFinal.values().forEach(ReadRepair::maybeSendAdditionalReads);
            for (Map.Entry<TxnDataName, ReadRepair> e : repairsFinal.entrySet()) {
                ReadRepair readRepair = e.getValue();
                processPartitionIterator(readRepair.awaitReads(), readRepair.command(), resolvedData, e.getKey());
            }

            if (repairMutationsFinal.isEmpty())
                return new ResolveResult(resolvedData, null);

            Mutation repairMutation = Mutation.merge(repairMutationsFinal);
            Collection<PartitionUpdate> partitionUpdates = repairMutation.getPartitionUpdates();
            return new ResolveResult(resolvedData, new TxnRepairWrites(partitionUpdates));
        });
    }

    // Very important to close the iterators so read repair works and generates repair mutations
   private static void processPartitionIterator(PartitionIterator partitionIterator, ReadCommand command, TxnData resolvedData, TxnDataName txnDataName)
    {
        try (PartitionIterator closeIt = partitionIterator)
        {
            if (partitionIterator.hasNext())
            {
                try (RowIterator rowIterator = partitionIterator.next())
                {
                    FilteredPartition filtered = FilteredPartition.create(rowIterator);
                    if (filtered.hasRows() || command.selectsFullPartition())
                        resolvedData.put(txnDataName, filtered);
                }
            }
        }
    }

    private static CassandraFollowupReader getCassandraFollowupReader(FollowupReader followupReader, TxnRead txnRead, TxnNamedRead txnNamedRead)
    {
        return (repairReadCommand, replica, callback, trackRepairedStatus) -> {
            TxnNamedRead repairTxnNamedRead = new TxnNamedRead(txnNamedRead.txnDataName(), (SinglePartitionReadCommand) repairReadCommand);
            checkState(txnRead.cassandraConsistencyLevel() != null, "If repair is occuring in Accord then the Cassandra consistency level should not be null");
            // TODO Review: Providing a consistency level here is mildly misleading, but it also happens to be harmless, probably should just be INVALID to indicate that?
            TxnRead repairTxnRead = new TxnRead(new TxnNamedRead[]{ repairTxnNamedRead }, Keys.of(PartitionKey.of((SinglePartitionReadCommand) repairReadCommand)), txnRead.cassandraConsistencyLevel());
            followupReader.read(repairTxnRead, EndpointMapping.getId(replica), new Callback<UnresolvedData>()
            {

                @Override
                public void onSuccess(Id from, UnresolvedData reply)
                {
                    TxnUnresolvedReadResponses unresolvedData = (TxnUnresolvedReadResponses) reply;
                    checkState(unresolvedData.size() == 1, "Expect a single result");
                    callback.onResponse(unresolvedData.get(0));
                }

                @Override
                public void onFailure(Id from, Throwable failure)
                {
                    callback.onFailure(EndpointMapping.getEndpoint(from), RequestFailureReason.forException(failure));
                }

                @Override
                public void onCallbackFailure(Id from, Throwable failure)
                {
                    failure.printStackTrace();
                }
            });
        };
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
        throw new IllegalStateException("Should always have a matching entry for every TxnDataName");
    }
}
