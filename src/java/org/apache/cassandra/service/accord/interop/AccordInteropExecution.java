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

package org.apache.cassandra.service.accord.interop;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiConsumer;

import accord.api.Data;
import accord.api.Result;
import accord.coordinate.CoordinationAdapter;
import accord.coordinate.ExecuteFlag.CoordinationFlags;
import accord.coordinate.Timeout;
import accord.local.Node;
import accord.local.Node.Id;
import accord.local.SequentialAsyncExecutor;
import accord.messages.Commit;
import accord.messages.Commit.Kind;
import accord.primitives.AbstractRanges;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Keys;
import accord.primitives.Participants;
import accord.primitives.Timestamp;
import accord.primitives.TimestampWithUniqueHlc;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
import accord.utils.Invariants;
import accord.utils.UnhandledEnum;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.SinglePartitionReadCommand.Group;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.metrics.AccordClientRequestMetrics;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.RequestCallback;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.accord.AccordEndpointMapper;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.txn.AccordUpdate;
import org.apache.cassandra.service.accord.txn.TxnData;
import org.apache.cassandra.service.accord.txn.TxnDataKeyValue;
import org.apache.cassandra.service.accord.txn.TxnDataRangeValue;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.UnrecoverableRepairUpdate;
import org.apache.cassandra.service.consensus.migration.ConsensusRequestRouter;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.transport.Dispatcher;

import static accord.coordinate.CoordinationAdapter.Factory.Kind.Standard;
import static accord.primitives.Txn.Kind.Write;
import static accord.topology.Topologies.SelectNodeOwnership.SHARE;
import static accord.utils.Invariants.requireArgument;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordReadMetrics;
import static org.apache.cassandra.metrics.ClientRequestsMetricsHolder.accordWriteMetrics;

/*
 * The core interoperability problem between Accord and C* writes (regular, and read repair)
 * is that when the writes don't go through Accord then Accord can read data that is not yet committed
 * because Accord replicas can lag behind and multiple coordinators can be attempting to compute the result of a
 * transaction and they can compute different results depending on what they consider to be the inputs to the Accord
 * transaction.
 *
 * We generally solve this by forcing non-Accord writes through Accord as well as by having Accord perform read repair
 * on its inputs.
 *
 */
public class AccordInteropExecution implements ReadCoordinator
{
    private static final AtomicLongFieldUpdater<AccordInteropExecution> UNIQUE_HLC_UPDATER = AtomicLongFieldUpdater.newUpdater(AccordInteropExecution.class, "uniqueHlc");

    private final Node node;
    private final TxnId txnId;
    private final Txn txn;
    private final FullRoute<?> route;
    private final Ballot ballot;
    private final Timestamp executeAt;
    private final Deps deps;
    private final BiConsumer<? super Result, Throwable> callback;
    private final SequentialAsyncExecutor executor;
    private final ConsistencyLevel consistencyLevel;
    private final AccordEndpointMapper endpointMapper;

    private final Topologies executes;
    private final Topologies allTopologies;
    private final Topology executeTopology;
    private final Topology coordinateTopology;

    private final AtomicInteger readsCurrentlyUnderConstruction;

    private final Set<InetAddressAndPort> contacted;
    private final AccordUpdate.Kind updateKind;
    private volatile long uniqueHlc;

    public AccordInteropExecution(Node node, TxnId txnId, Txn txn, AccordUpdate.Kind updateKind, FullRoute<?> route, Ballot ballot, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback,
                                  SequentialAsyncExecutor executor, ConsistencyLevel consistencyLevel, AccordEndpointMapper endpointMapper)
    {
        requireArgument(!txn.read().keys().isEmpty() || updateKind == AccordUpdate.Kind.UNRECOVERABLE_REPAIR);
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        this.ballot = ballot;
        this.executeAt = executeAt;
        this.deps = deps;
        this.callback = callback;
        this.executor = executor;

        requireArgument(updateKind == AccordUpdate.Kind.UNRECOVERABLE_REPAIR || consistencyLevel == ConsistencyLevel.QUORUM || consistencyLevel == ConsistencyLevel.ALL || consistencyLevel == ConsistencyLevel.SERIAL);
        this.consistencyLevel = consistencyLevel;
        this.endpointMapper = endpointMapper;

        // TODO (required): compare this to latest logic in Accord, make sure it makes sense
        this.executes = node.topology().forEpoch(route, executeAt.epoch(), SHARE);
        this.allTopologies = txnId.epoch() != executeAt.epoch()
                             ? node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch(), SHARE)
                             : executes;
        this.executeTopology = executes.getEpoch(executeAt.epoch());
        this.coordinateTopology = allTopologies.getEpoch(txnId.epoch());
        if (consistencyLevel != ConsistencyLevel.ALL)
        {
            readsCurrentlyUnderConstruction = new AtomicInteger(txn.read().keys().size());
            contacted = Collections.newSetFromMap(new ConcurrentHashMap<>());
        }
        else
        {
            readsCurrentlyUnderConstruction = null;
            contacted = null;
        }
        this.updateKind = updateKind;
    }

    @Override
    public boolean localReadSupported()
    {
        return false;
    }

    @Override
    public EndpointsForToken forNonLocalStrategyTokenRead(ClusterMetadata doNotUse, KeyspaceMetadata keyspace, TableId tableId, Token token)
    {
        TokenKey key = new TokenKey(tableId, token);
        Shard shard = executeTopology.forKey(key);
        Range<Token> range = ((TokenRange) shard.range).toKeyspaceRange();

        Replica[] replicas = new Replica[shard.nodes.size()];
        for (int i=0; i<replicas.length; i++)
        {
            Node.Id id = shard.nodes.get(i);
            replicas[i] = new Replica(endpointMapper.mappedEndpoint(id), range, true);
        }

        return EndpointsForToken.of(token, replicas);
    }

    @Override
    public void sendReadCommand(Message<ReadCommand> message, InetAddressAndPort to, RequestCallback<ReadResponse> callback)
    {
        Node.Id id = endpointMapper.mappedId(to);
        // TODO (desired): It would be better to use the re-use the command from the transaction but it's fragile
        //  to try and figure out exactly what changed for things like read repair and short read protection
        //  Also this read scope doesn't reflect the contents of this particular read and is larger than it needs to be
        // TODO (required): understand interop and whether StableFastPath is appropriate
        AccordInteropStableThenRead commit = new AccordInteropStableThenRead(id, allTopologies, txnId, Kind.StableFastPath, executeAt, txn, deps, route, message.payload);
        node.send(id, commit, executor, new AccordInteropRead.ReadCallback(id, to, message, callback, this));
    }

    @Override
    public void sendReadRepairMutation(Message<Mutation> message, InetAddressAndPort to, RequestCallback<Object> callback)
    {
        requireArgument(message.payload.potentialTxnConflicts().allowed);
        requireArgument(message.payload.getTableIds().size() == 1);
        Node.Id id = endpointMapper.mappedId(to);
        Participants<?> readScope = Participants.singleton(txn.read().keys().domain(), new TokenKey(message.payload.getTableIds().iterator().next(), message.payload.key().getToken()));
        AccordInteropReadRepair readRepair = new AccordInteropReadRepair(id, executes, txnId, readScope, executeAt.epoch(), message.payload);
        node.send(id, readRepair, executor, new AccordInteropReadRepair.ReadRepairCallback(id, to, message, callback, this));
    }

    private List<AsyncChain<Data>> readChains(Dispatcher.RequestTime requestTime)
    {
        switch (txnId.domain())
        {
            case Key:
                return keyReadChains((Txn.InMemory)txn, requestTime);
            case Range:
                return rangeReadChains((Txn.InMemory)txn, requestTime);
            default:
                throw UnhandledEnum.unknown(txnId.domain());
        }
    }

    private List<AsyncChain<Data>> keyReadChains(Txn.InMemory txn, Dispatcher.RequestTime requestTime)
    {
        TxnRead read = (TxnRead) txn.read();
        Keys keys = (Keys) read.keys();
        TableMetadatasAndKeys tablesAndKeys = (TableMetadatasAndKeys) txn.implementationDefined;
        ClusterMetadata cm = ClusterMetadata.current();
        List<AsyncChain<Data>> results = new ArrayList<>();
        keys.forEach(key -> {
                         read.forEachWithKey(key, fragment -> {
                             SinglePartitionReadCommand command = (SinglePartitionReadCommand) fragment.command(tablesAndKeys.tables);

                             // This should only rarely occur when coordinators start a transaction in a migrating range
                             // because they haven't yet updated their cluster metadata.
                             // It would be harmless to do the read, because it will be rejected in `TxnQuery` anyways,
                             // but it's faster to skip the read
                             AccordClientRequestMetrics metrics = txnId.isWrite() ? accordWriteMetrics : accordReadMetrics;
                             // TODO (required): This doesn't use the metadata from the correct epoch
                             if (!ConsensusRequestRouter.instance.isKeyManagedByAccordForReadAndWrite(cm, command.metadata().id, command.partitionKey()))
                             {
                                 metrics.migrationSkippedReads.mark();
                                 results.add(AsyncChains.success(TxnData.emptyPartition(fragment.txnDataName(), command)));
                                 return;
                             }

                             Group group = Group.one(command);
                             results.add(AsyncChains.ofCallable(Stage.ACCORD_MIGRATION.executor(), () -> {
                                 TxnData result = new TxnData();
                                 // Enforcing limits is redundant since we only have a group of size 1, but checking anyways
                                 // documents the requirement here
                                 try (PartitionIterator iterator = StorageProxy.maybeEnforceLimits(StorageProxy.fetchRows(group.queries, consistencyLevel, this, requestTime), group))
                                 {
                                     if (iterator.hasNext())
                                     {
                                         try (RowIterator partition = iterator.next())
                                         {
                                             TxnDataKeyValue value = new TxnDataKeyValue(partition);
                                             if (value.hasRows() || command.selectsFullPartition())
                                                 result.put(fragment.txnDataName(), value);
                                         }
                                     }
                                 }
                                 return result;
                             }));
                         });

                     });
        return results;
    }

    private List<AsyncChain<Data>> rangeReadChains(Txn.InMemory txn, Dispatcher.RequestTime requestTime)
    {
        TxnRead read = (TxnRead) txn.read();
        AbstractRanges ranges = (AbstractRanges) read.keys();
        TableMetadatasAndKeys tablesAndKeys = (TableMetadatasAndKeys) txn.implementationDefined;
        List<AsyncChain<Data>> results = new ArrayList<>();
        ranges.forEach(range -> {
            read.forEachWithKey(range, fragment -> {
                PartitionRangeReadCommand command = ((PartitionRangeReadCommand) fragment.command(tablesAndKeys.tables)).withTxnReadName(fragment.txnDataName());

                // TODO (required): To make migration work we need to validate that the range is all on Accord

                results.add(AsyncChains.ofCallable(Stage.ACCORD_MIGRATION.executor(), () -> {
                    TxnData result = new TxnData();
                    try (PartitionIterator iterator = StorageProxy.getRangeSlice(command, consistencyLevel, this, requestTime))
                    {
                        TxnDataRangeValue value = new TxnDataRangeValue();
                        while (iterator.hasNext())
                        {
                            try (RowIterator partition = iterator.next())
                            {
                                FilteredPartition filtered = FilteredPartition.create(partition);
                                if (filtered.hasRows() || command.selectsFullPartition())
                                    value.add(filtered);
                            }
                        }
                        result.put(fragment.txnDataName(), value);
                    }
                    return result;
                }));
            });

        });
        return results;
    }

    private AsyncChain<Data> readChains()
    {
        // TODO (expected): use normal query nano time
        Dispatcher.RequestTime requestTime = Dispatcher.RequestTime.forImmediateExecution();

        List<AsyncChain<Data>> results = readChains(requestTime);
        if (results.isEmpty())
            return AsyncChains.success(new TxnData());

        if (results.size() == 1)
            return results.get(0);

        return AsyncChains.reduce(results, Data::merge);
    }

    /*
     * Any nodes not contacted for read need to be sent commits
     */
    @Override
    public void notifyOfInitialContacts(EndpointsForToken fullDataRequests, EndpointsForToken transientRequests, EndpointsForToken digestRequests)
    {
        if (readsCurrentlyUnderConstruction == null)
            return;

        for (int i = 0; i < fullDataRequests.size(); i++)
            contacted.add(fullDataRequests.endpoint(i));
        for (int i = 0; i < transientRequests.size(); i++)
            contacted.add(transientRequests.endpoint(i));
        for (int i = 0; i < digestRequests.size(); i++)
            contacted.add(digestRequests.endpoint(i));
        if (readsCurrentlyUnderConstruction.decrementAndGet() == 0)
            sendStableToUncontacted();
    }

    private void sendStableToUncontacted()
    {
        for (Node.Id to : executeTopology.nodes())
            if (!contacted.contains(endpointMapper.mappedEndpoint(to)))
                node.send(to, new Commit(Kind.StableFastPath, to, allTopologies, txnId, txn, route, Ballot.ZERO, executeAt, deps));
    }

    public void start()
    {
        if (coordinateTopology != executeTopology)
        {
            for (Node.Id to : allTopologies.nodes())
            {
                if (!executeTopology.contains(to))
                    node.send(to, new Commit(Kind.StableFastPath, to, allTopologies, txnId, txn, route, Ballot.ZERO, executeAt, deps));
            }
        }
        AsyncChain<Data> result;
        if (updateKind == AccordUpdate.Kind.UNRECOVERABLE_REPAIR)
            result = executeUnrecoverableRepairUpdate();
        else
            result = readChains();

        result.begin((data, failure) -> {
            if (failure == null)
            {
                long uniqueHlc = this.uniqueHlc;
                Timestamp executeAt = this.executeAt;
                if (txnId.is(Txn.Kind.Write) && uniqueHlc != 0)
                {
                    Invariants.require(uniqueHlc > executeAt.hlc());
                    executeAt = new TimestampWithUniqueHlc(executeAt, uniqueHlc);
                }
                ((CoordinationAdapter)node.coordinationAdapter(txnId, Standard)).persist(node, executor, executes, route, ballot, CoordinationFlags.none(), txnId, txn, executeAt, deps, txnId.is(Write) ? txn.execute(txnId, executeAt, data) : null, txn.result(txnId, executeAt, data), callback);
            }
            else
            {
                callback.accept(null, maybeWrapRequestFailureException(failure));
            }
        });
    }

    /**
     * Interop should expose these exceptions as the appropriate Accord types so AccordService
     * knows how to handle them
     */
    private Throwable maybeWrapRequestFailureException(Throwable failure)
    {
        Throwable toCheck = failure;
        do
        {
            // TODO (required): There are probably more exceptions that will have this issue of wanting
            // to be turned into the top level exception sent back to the client
            if (toCheck instanceof ReadTimeoutException || toCheck instanceof ReadFailureException)
                return new Timeout(txnId, route.homeKey(), failure);
        } while ((toCheck = toCheck.getCause()) != null);
        return failure;
    }

    private AsyncChain<Data> executeUnrecoverableRepairUpdate()
    {
        return AsyncChains.ofCallable(Stage.ACCORD_MIGRATION.executor(), () -> {
            UnrecoverableRepairUpdate repairUpdate = (UnrecoverableRepairUpdate)txn.update();
            // TODO (expected): We should send the read in the same message as the commit. This requires refactor ReadData.Kind so that it doesn't specify the ordinal encoding
            // and can be extended similar to MessageType which allows additional types not from Accord to be added
            // This commit won't necessarily execute before the interop read repair message so there could be an insufficient which is fine
            for (Node.Id to : executeTopology.nodes())
                    node.send(to, new Commit(Kind.StableFastPath, to, allTopologies, txnId, txn, route, Ballot.ZERO, executeAt, deps));
            repairUpdate.runBRR(AccordInteropExecution.this);
            return new TxnData();
        });
    }

    @Override
    public boolean isEventuallyConsistent()
    {
        return false;
    }

    @Override
    public ReadCommand maybeAllowOutOfRangeReads(ReadCommand readCommand, ConsistencyLevel cl)
    {
        // Reading from a single coordinator so there is no reconciliation at the coordinator and filtering/limits
        // need to be pushed down to query execution
        boolean withoutReconciliation = cl == null || cl == ConsistencyLevel.ONE;
        // Really just want to enable allowPotentialTxnConflicts without changing anything else
        // but didn't want to add another method for constructing a modified read command
        if (readCommand instanceof SinglePartitionReadCommand)
            return ((SinglePartitionReadCommand)readCommand).withTransactionalSettings(withoutReconciliation, readCommand.nowInSec());
        else
        {
            PartitionRangeReadCommand rangeCommand = ((PartitionRangeReadCommand)readCommand);
            return rangeCommand.withTransactionalSettings(readCommand.nowInSec(), rangeCommand.dataRange().keyRange(), true, withoutReconciliation);
        }
    }

    // Provide request callbacks with a way to send maximal commits on Insufficient responses
    public void sendMaximalCommit(Id to)
    {
        node.send(to, new Commit(Kind.StableWithTxnAndDeps, to, allTopologies, txnId, txn, route, ballot, executeAt, deps));
    }

    public void maybeUpdateUniqueHlc(long uniqueHlc)
    {
        if (txnId.is(Txn.Kind.Write) && uniqueHlc > 0)
        {
            Invariants.require(uniqueHlc > executeAt.hlc());
            UNIQUE_HLC_UPDATER.accumulateAndGet(this, uniqueHlc, Math::max);
        }
    }
}
