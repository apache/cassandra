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

package org.apache.cassandra.service.accord.async;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordPartialCommand;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.AccordState;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.store.StoredSet;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

import static accord.primitives.Routable.Domain.Range;

public class AsyncWriter
{
    private static final Logger logger = LoggerFactory.getLogger(AsyncWriter.class);

    enum State
    {
        INITIALIZED,
        SETUP,
        SAVING,
        FINISHED
    }

    private State state = State.INITIALIZED;
    protected Future<?> writeFuture;
    private final AccordCommandStore commandStore;
    final AccordStateCache.Instance<TxnId, AccordCommand> commandCache;
    final AccordStateCache.Instance<PartitionKey, AccordCommandsForKey> cfkCache;

    public AsyncWriter(AccordCommandStore commandStore)
    {
        this.commandStore = commandStore;
        this.commandCache = commandStore.commandCache();
        this.cfkCache = commandStore.commandsForKeyCache();
    }

    private interface StateMutationFunction<K, V extends AccordState<K>>
    {
        Mutation apply(AccordCommandStore commandStore, V state, long timestamp);
    }

    private static <K, V extends AccordState<K>> List<Future<?>> dispatchWrites(AsyncContext.Group<K, V> ctxGroup,
                                                                                AccordStateCache.Instance<K, V> cache,
                                                                                StateMutationFunction<K, V> mutationFunction,
                                                                                long timestamp,
                                                                                AccordCommandStore commandStore,
                                                                                List<Future<?>> futures,
                                                                                Object callback)
    {
        for (V item : ctxGroup.items.values())
        {
            if (!item.hasModifications())
            {
                if (logger.isTraceEnabled())
                    logger.trace("No modifications for {} for {}, {}", item.key(), callback, item);
                continue;
            }

            if (futures == null)
                futures = new ArrayList<>();
            K key = item.key();
            Mutation mutation = mutationFunction.apply(commandStore, item, timestamp);
            if (logger.isTraceEnabled())
                logger.trace("Dispatching mutation for {} for {}, {} -> {}", key, callback, item, mutation);
            Future<?> future = Stage.MUTATION.submit(() -> {
                try
                {
                    if (logger.isTraceEnabled())
                        logger.trace("Applying mutation for {} for {}: {}", key, callback, mutation);
                    mutation.apply();
                    if (logger.isTraceEnabled())
                        logger.trace("Completed applying mutation for {} for {}: {}", key, callback, mutation);
                }
                catch (Throwable t)
                {
                    logger.error(String.format("Exception applying mutation for %s for %s: %s", key, callback, mutation), t);
                    throw t;
                }
            });
            cache.addSaveFuture(item.key(), future);
            futures.add(future);
        }

        for (AccordState.WriteOnly<K, V> item : ctxGroup.writeOnly.values())
        {
            Preconditions.checkState(item.hasModifications());
            if (futures == null) futures = new ArrayList<>();
            Mutation mutation = mutationFunction.apply(commandStore, (V) item, timestamp);
            Future<?> future = Stage.MUTATION.submit((Runnable) mutation::apply);
            future.addListener(() -> cache.purgeWriteOnly(item.key()), commandStore.executor());
            item.future(future);
            futures.add(future);
        }

        return futures;
    }

    private Future<?> maybeDispatchWrites(AsyncContext context, Object callback) throws IOException
    {
        List<Future<?>> futures = null;

        long timestamp = commandStore.nextSystemTimestampMicros();
        futures = dispatchWrites(context.commands,
                                 commandStore.commandCache(),
                                 AccordKeyspace::getCommandMutation,
                                 timestamp,
                                 commandStore,
                                 futures,
                                 callback);

        futures = dispatchWrites(context.commandsForKey,
                                 commandStore.commandsForKeyCache(),
                                 AccordKeyspace::getCommandsForKeyMutation,
                                 timestamp,
                                 commandStore,
                                 futures,
                                 callback);

        return futures != null ? FutureCombiner.allOf(futures) : null;
    }

    private void denormalizeBlockedOn(AccordCommand command,
                                      AsyncContext context,
                                      Function<AccordCommand, StoredSet.Changes<TxnId>> waitingField,
                                      Function<AccordCommand, StoredSet.Navigable<TxnId>> blockingField)
    {
        StoredSet.Changes<TxnId> waitingOn = waitingField.apply(command);
        waitingOn.forEachDeletion(deletedId -> {
            AccordCommand blockedOn = commandForDenormalization(deletedId, context);
            blockingField.apply(blockedOn).blindRemove(command.txnId());
        });

        waitingOn.forEachAddition(addedId -> {
            AccordCommand blockedOn = commandForDenormalization(addedId, context);
            blockingField.apply(blockedOn).blindAdd(command.txnId());
        });
    }

    private void denormalizeWaitingOnSummaries(AccordCommand command,
                                               AsyncContext context,
                                               Function<AccordCommand, BiConsumer<TxnId, Timestamp>> waitingField,
                                               Function<AccordCommand, StoredSet.Navigable<TxnId>> blockingField)
    {
        blockingField.apply(command).getView().forEach(blockingId -> {
            AccordCommand blocking = commandForDenormalization(blockingId, context);
            waitingField.apply(blocking).accept(command.txnId(), command.executeAt());
        });
    }

    private static <K, V extends AccordState<K>>
            AccordState<K> getForDenormalization(K key,
                                                 AccordCommandStore commandStore,
                                                 AsyncContext.Group<K, V> ctxGroup,
                                                 AccordStateCache.Instance<K, V> cache,
                                                 BiFunction<AccordCommandStore, K, AccordState.WriteOnly<K, V>> factory)
    {
        V item = ctxGroup.get(key);
        if (item != null)
            return item;

        item = cache.getOrNull(key);
        if (item != null && !cache.hasLoadFuture(key))
        {
            ctxGroup.items.put(key, item);
            return item;
        }

        return ctxGroup.getOrCreateWriteOnly(key, factory, commandStore);
    }

    private AccordCommand commandForDenormalization(TxnId txnId, AsyncContext context)
    {
        return (AccordCommand) getForDenormalization(txnId, commandStore, context.commands, commandCache, (ignore, id) -> new AccordCommand.WriteOnly(id));
    }

    private AccordCommandsForKey cfkForDenormalization(PartitionKey key, AsyncContext context)
    {
        return (AccordCommandsForKey) getForDenormalization(key, commandStore, context.commandsForKey, cfkCache, AccordCommandsForKey.WriteOnly::new);
    }

    private void denormalize(AccordCommand command, AsyncContext context, Object callback)
    {
        if (!command.hasModifications())
            return;

        // notify commands we're waiting on that they need to update the summaries in our maps
        if (command.waitingOnCommit.hasModifications())
        {
            denormalizeBlockedOn(command, context, cmd -> cmd.waitingOnCommit, cmd -> cmd.blockingCommitOn);
        }
        if (command.waitingOnApply.hasModifications())
        {
            denormalizeBlockedOn(command, context, cmd -> new StoredSet.Changes<TxnId>()
            {
                @Override
                public void forEachAddition(Consumer<TxnId> consumer)
                {
                    cmd.waitingOnApply.forEachAddition((ignore, txnId) -> consumer.accept(txnId));
                }

                @Override
                public void forEachDeletion(Consumer<TxnId> consumer)
                {
                    cmd.waitingOnApply.forEachDeletion((ignore, txnId) -> consumer.accept(txnId));

                }
            }, cmd -> cmd.blockingApplyOn);
        }

        if (command.shouldUpdateDenormalizedWaitingOn())
        {
            denormalizeWaitingOnSummaries(command, context, cmd -> (txnId, ignore) -> cmd.waitingOnCommit.blindAdd(txnId), cmd -> cmd.blockingCommitOn);
            denormalizeWaitingOnSummaries(command, context, cmd -> (txnId, executeAt) -> cmd.waitingOnApply.blindPut(executeAt, txnId), cmd -> cmd.blockingApplyOn);
        }

        // There won't be a txn to denormalize against until the command has been preaccepted
        // TODO (now): this maybe insufficient for correctness? on Accept we use the explicitly provided keys to register
        //             the transaction here. It's possible a sequence of two Accept, with second taking a higher timestamp
        //             might not reflect the update timestamp in the map? Probably best addressed following Blake's refactor.
        if (command.known().isDefinitionKnown() && AccordPartialCommand.serializer.needsUpdate(command))
        {
            for (Seekable key : command.partialTxn().keys())
            {
                // TODO: implement
                if (key.domain() == Range)
                    throw new UnsupportedOperationException();
                PartitionKey partitionKey = (PartitionKey) key;
                AccordCommandsForKey cfk = cfkForDenormalization(partitionKey, context);
                cfk.updateSummaries(command);
            }
        }

        if (logger.isTraceEnabled())
        {
            context.commands.items.forEach((txnId, cmd) -> logger.trace("Denormalized command {} for {}: {}", txnId, callback, cmd));
            context.commandsForKey.items.forEach((key, cfk) -> logger.trace("Denormalized cfk {} for {}: {}", key, callback, cfk));
        }
    }

    private void denormalize(AsyncContext context, Object callback)
    {
        // need to clone "values" as denormalize will mutate it
        new ArrayList<>(context.commands.items.values()).forEach(command -> denormalize(command, context, callback));
    }

    @VisibleForTesting
    void setState(State state)
    {
        this.state = state;
    }

    public boolean save(AsyncContext context, BiConsumer<Object, Throwable> callback)
    {
        logger.trace("Running save for {} with state {}", callback, state);
        commandStore.checkInStoreThread();
        try
        {
            switch (state)
            {
                case INITIALIZED:
                    setState(State.SETUP);
                case SETUP:
                    denormalize(context, callback);
                    writeFuture = maybeDispatchWrites(context, callback);

                    setState(State.SAVING);
                case SAVING:
                    if (writeFuture != null && !writeFuture.isSuccess())
                    {
                        logger.trace("Adding callback for write future: {}", callback);
                        writeFuture.addCallback(callback, commandStore.executor());
                        break;
                    }
                    context.commands.items.keySet().forEach(commandStore.commandCache()::cleanupSaveFuture);
                    context.commandsForKey.items.keySet().forEach(commandStore.commandsForKeyCache()::cleanupSaveFuture);
                    setState(State.FINISHED);
                case FINISHED:
                    break;
                default:
                    throw new IllegalStateException("Unexpected state: " + state);
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        logger.trace("Exiting save for {} with state {}", callback, state);
        return state == State.FINISHED;
    }

}
