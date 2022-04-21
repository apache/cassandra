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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.google.common.base.Preconditions;

import accord.api.Key;
import accord.txn.TxnId;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordKeyspace;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.service.accord.serializers.CommandSummaries;
import org.apache.cassandra.service.accord.store.StoredNavigableMap;
import org.apache.cassandra.service.accord.store.StoredSet;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.FutureCombiner;

public class AsyncWriter
{
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
    AccordStateCache.Instance<TxnId, AccordCommand> commandCache;
    AccordStateCache.Instance<PartitionKey, AccordCommandsForKey> cfkCache;

    public AsyncWriter(AccordCommandStore commandStore)
    {
        this.commandStore = commandStore;
        this.commandCache = commandStore.commandCache();
        this.cfkCache = commandStore.commandsForKeyCache();
    }

    private static <K, V extends AccordStateCache.AccordState<K, V>> List<Future<?>> dispatchWrites(Iterable<V> items,
                                                                                                    AccordStateCache.Instance<K, V> cache,
                                                                                                    Function<V, Mutation> mutationFunction,
                                                                                                    List<Future<?>> futures)
    {
        for (V item : items)
        {
            if (!item.hasModifications())
                continue;

            if (futures == null) futures = new ArrayList<>();
            Mutation mutation = mutationFunction.apply(item);
            Future<?> future = Stage.MUTATION.submit((Runnable) mutation::apply);
            cache.setSaveFuture(item.key(), future);
            futures.add(future);
        }
        return futures;
    }

    private Future<?> maybeDispatchWrites(AsyncContext context) throws IOException
    {
        List<Future<?>> futures = null;

        futures = dispatchWrites(context.commands.values(),
                                 commandStore.commandCache(),
                                 AccordKeyspace::getCommandMutation,
                                 futures);

        futures = dispatchWrites(context.keyCommands.values(),
                                 commandStore.commandsForKeyCache(),
                                 AccordKeyspace::getCommandsForKeyMutation,
                                 futures);

        return futures != null ? FutureCombiner.allOf(futures) : null;
    }

    private void denormalizeBlockedOn(AccordCommand command,
                                      AsyncContext context,
                                      Map<TxnId, AccordCommand> addToCtx,
                                      Function<AccordCommand, StoredNavigableMap<TxnId, ByteBuffer>> waitingField,
                                      Function<AccordCommand, StoredSet.Navigable<TxnId>> blockingField)
    {
        StoredNavigableMap<TxnId, ?> waitingOn = waitingField.apply(command);
        waitingOn.forEachDeletion(deletedId -> {
            AccordCommand blockedOn = commandForDenormalization(deletedId, context, addToCtx);
            blockingField.apply(blockedOn).blindRemove(command.txnId());
        });

        waitingOn.forEachAddition((addedId, unused) -> {
            AccordCommand blockedOn = commandForDenormalization(addedId, context, addToCtx);
            blockingField.apply(blockedOn).blindAdd(command.txnId());
        });
    }

    private void denormalizeWaitingOnSummaries(AccordCommand command,
                                               AsyncContext context,
                                               Map<TxnId, AccordCommand> addToCtx,
                                               ByteBuffer summary,
                                               Function<AccordCommand, StoredNavigableMap<TxnId, ByteBuffer>> waitingField,
                                               Function<AccordCommand, StoredSet.Navigable<TxnId>> blockingField)
    {
        blockingField.apply(command).getView().forEach(blockingId -> {
            AccordCommand blocking = commandForDenormalization(blockingId, context, addToCtx);
            waitingField.apply(blocking).blindPut(command.txnId(), summary.duplicate());
        });
    }

    private AccordCommand commandForDenormalization(TxnId txnId, AsyncContext context, Map<TxnId, AccordCommand> addToCtx)
    {
        AccordCommand command = context.command(txnId);
        if (command != null)
            return command;

        command = commandCache.getOrNull(txnId);
        if (command != null)
        {
            addToCtx.put(txnId, command);
            return command;
        }
        return context.getOrCreateWriteOnlyCommand(txnId, commandStore);
    }

    private AccordCommandsForKey cfkForDenormalization(PartitionKey key, AsyncContext context, Map<PartitionKey, AccordCommandsForKey> addToCtx)
    {
        AccordCommandsForKey cfk = context.commandsForKey(key);
        if (cfk != null)
            return cfk;

        cfk = cfkCache.getOrNull(key);
        if (cfk != null)
        {
            addToCtx.put(key, cfk);
            return cfk;
        }
        return context.getOrCreateWriteOnlyCFK(key, commandStore);
    }

    private void denormalize(AccordCommand command, AsyncContext context)
    {
        Map<TxnId, AccordCommand> addCmdToCtx = new HashMap<>();

        // notify commands we're waiting on that they need to update the summaries in our maps
        if (command.waitingOnCommit.hasModifications())
            denormalizeBlockedOn(command, context, addCmdToCtx, cmd -> cmd.waitingOnCommit, cmd -> cmd.blockingCommitOn);
        if (command.waitingOnApply.hasModifications())
            denormalizeBlockedOn(command, context, addCmdToCtx, cmd -> cmd.waitingOnApply, cmd -> cmd.blockingApplyOn);

        if (command.shouldUpdateDenormalizedWaitingOn())
        {
            ByteBuffer summary = CommandSummaries.waitingOn.serialize(command);
            denormalizeWaitingOnSummaries(command, context, addCmdToCtx, summary, cmd -> cmd.waitingOnCommit, cmd -> cmd.blockingCommitOn);
            denormalizeWaitingOnSummaries(command, context, addCmdToCtx, summary, cmd -> cmd.waitingOnApply, cmd -> cmd.blockingApplyOn);
        }

        Map<PartitionKey, AccordCommandsForKey> addCfkToCtx = new HashMap<>();
        if (CommandSummaries.commandsPerKey.needsUpdate(command))
        {
            for (Key key : command.txn().keys())
            {
                PartitionKey partitionKey = (PartitionKey) key;
                AccordCommandsForKey cfk = cfkForDenormalization(partitionKey, context, addCfkToCtx);
                cfk.updateSummaries(command);
            }
        }

        context.commands.putAll(addCmdToCtx);
        context.keyCommands.putAll(addCfkToCtx);
    }

    private void denormalize(AsyncContext context)
    {
        context.commands.values().forEach(command -> denormalize(command, context));
    }

    public boolean save(AsyncContext context, BiConsumer<Object, Throwable> callback)
    {
        commandStore.checkInStoreThread();

        try
        {
            switch (state)
            {
                case INITIALIZED:
                    state = State.SETUP;
                case SETUP:
                    context.summaries.values().forEach(summary -> Preconditions.checkState(!summary.hasModifications(),
                                                                                           "Summaries cannot be modified"));
                    denormalize(context);
                    if (writeFuture == null)
                        writeFuture = maybeDispatchWrites(context);

                    state = State.SAVING;
                case SAVING:
                    if (writeFuture != null && !writeFuture.isDone())
                    {
                        writeFuture.addCallback(callback, commandStore.executor());
                        break;
                    }
                    state = State.FINISHED;
                case FINISHED:
                    break;
                default:
                    throw new IllegalStateException();
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }

        return state == State.FINISHED;
    }
}
