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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Agent;
import accord.api.Data;
import accord.api.Result;
import accord.coordinate.CoordinationAdapter;
import accord.local.AgentExecutor;
import accord.local.CommandStore;
import accord.local.Node;
import accord.local.Node.Id;
import accord.messages.Commit;
import accord.messages.Commit.Kind;
import accord.primitives.Ballot;
import accord.primitives.Deps;
import accord.primitives.FullRoute;
import accord.primitives.Participants;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.topology.Shard;
import accord.topology.Topologies;
import accord.topology.Topology;
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
import org.apache.cassandra.service.accord.api.AccordAgent;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.interop.AccordInteropReadCallback.MaximalCommitSender;
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
public class AccordInteropExecution implements ReadCoordinator, MaximalCommitSender
{
    private static final Logger logger = LoggerFactory.getLogger(AccordInteropExecution.class);

    static class InteropExecutor implements AgentExecutor
    {
        private final AccordAgent agent;

        public InteropExecutor(AccordAgent agent)
        {
            this.agent = agent;
        }

        @Override
        public Agent agent()
        {
            return agent;
        }

        @Override
        public <T> AsyncChain<T> submit(Callable<T> task)
        {
            try
            {
                return AsyncChains.success(task.call());
            }
            catch (Throwable e)
            {
                return AsyncChains.failure(e);
            }
        }
    }

    private final Node node;
    private final TxnId txnId;
    private final Txn txn;
    private final FullRoute<?> route;
    private final Timestamp executeAt;
    private final Deps deps;
    private final BiConsumer<? super Result, Throwable> callback;
    private final AgentExecutor executor;
    private final ConsistencyLevel consistencyLevel;
    private final AccordEndpointMapper endpointMapper;

    private final Topologies executes;
    private final Topologies allTopologies;
    private final Topology executeTopology;
    private final Topology coordinateTopology;

    private final AtomicInteger readsCurrentlyUnderConstruction;

    private final Set<InetAddressAndPort> contacted;
    private final AccordUpdate.Kind updateKind;

    public AccordInteropExecution(Node node, TxnId txnId, Txn txn, AccordUpdate.Kind updateKind, FullRoute<?> route, Timestamp executeAt, Deps deps, BiConsumer<? super Result, Throwable> callback,
                                  AgentExecutor executor, ConsistencyLevel consistencyLevel, AccordEndpointMapper endpointMapper)
    {
        requireArgument(!txn.read().keys().isEmpty() || updateKind == AccordUpdate.Kind.UNRECOVERABLE_REPAIR);
        this.node = node;
        this.txnId = txnId;
        this.txn = txn;
        this.route = route;
        this.executeAt = executeAt;
        this.deps = deps;
        this.callback = callback;
        this.executor = executor;

        requireArgument(updateKind == AccordUpdate.Kind.UNRECOVERABLE_REPAIR || consistencyLevel == ConsistencyLevel.QUORUM || consistencyLevel == ConsistencyLevel.ALL || consistencyLevel == ConsistencyLevel.SERIAL);
        this.consistencyLevel = consistencyLevel;
        this.endpointMapper = endpointMapper;

        this.executes = node.topology().forEpoch(route, executeAt.epoch());
        this.allTopologies = txnId.epoch() != executeAt.epoch()
                             ? node.topology().preciseEpochs(route, txnId.epoch(), executeAt.epoch())
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
        AccordRoutingKey.TokenKey key = new AccordRoutingKey.TokenKey(tableId, token);
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
        // TODO (nicetohave): It would be better to use the re-use the command from the transaction but it's fragile
        // to try and figure out exactly what changed for things like read repair and short read protection
        // Also this read scope doesn't reflect the contents of this particular read and is larger than it needs to be
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
        TxnRead read = (TxnRead) txn.read();
        Seekables<?, ?> keys = txn.read().keys();
        switch (keys.domain())
        {
            case Key:
                return keyReadChains(read, keys, requestTime);
            case Range:
                return rangeReadChains(read, keys, requestTime);
            default:
                throw new IllegalStateException("Unhandled domain " + keys.domain());
        }
    }

    private List<AsyncChain<Data>> keyReadChains(TxnRead read, Seekables<?, ?> keys, Dispatcher.RequestTime requestTime)
    {
        ClusterMetadata cm = ClusterMetadata.current();
        List<AsyncChain<Data>> results = new ArrayList<>();
        keys.forEach(key -> {
                         read.forEachWithKey((PartitionKey) key, fragment -> {
                             SinglePartitionReadCommand command = (SinglePartitionReadCommand) fragment.command();

                             // This should only rarely occur when coordinators start a transaction in a migrating range
                             // because they haven't yet updated their cluster metadata.
                             // It would be harmless to do the read, because it will be rejected in `TxnQuery` anyways,
                             // but it's faster to skip the read
                             AccordClientRequestMetrics metrics = txn.kind().isWrite() ? accordWriteMetrics : accordReadMetrics;
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

    private List<AsyncChain<Data>> rangeReadChains(TxnRead read, Seekables<?, ?> keys, Dispatcher.RequestTime requestTime)
    {
        List<AsyncChain<Data>> results = new ArrayList<>();
        keys.forEach(key -> {
            read.forEachWithKey(key, fragment -> {
                PartitionRangeReadCommand command = ((PartitionRangeReadCommand) fragment.command()).withTxnReadName(fragment.txnDataName());

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

        CommandStore cs = node.commandStores().select(route.homeKey());
        result.beginAsResult().withExecutor(cs).begin((data, failure) -> {
            if (failure == null)
                ((CoordinationAdapter)node.coordinationAdapter(txnId, Standard)).persist(node, executes, route, txnId, txn, executeAt, deps, txnId.is(Write) ? txn.execute(txnId, executeAt, data) : null, txn.result(txnId, executeAt, data), callback);
            else
                callback.accept(null, failure);
        });
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
    @Override
    public void sendMaximalCommit(Id to)
    {
        Commit.stableMaximal(node, to, txn, txnId, executeAt, route, deps);
    }
}
