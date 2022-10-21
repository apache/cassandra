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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

import accord.local.PreLoadContext;
import accord.primitives.TxnId;
import org.apache.cassandra.service.accord.AccordCommand;
import org.apache.cassandra.service.accord.AccordCommandStore;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.AccordPartialCommand;
import org.apache.cassandra.service.accord.AccordState;
import org.apache.cassandra.service.accord.AccordStateCache;
import org.apache.cassandra.service.accord.AccordState.WriteOnly;
import org.apache.cassandra.service.accord.api.PartitionKey;

public class AsyncContext
{
    public static class Group<K, V extends AccordState<K>>
    {
        final Map<K, V> items = new HashMap<>();
        final Map<K, WriteOnly<K, V>> writeOnly = new HashMap<>();

        @VisibleForTesting
        public void add(V item)
        {
            items.put(item.key(), item);
        }

        public V get(K key)
        {
            return items.get(key);
        }

        void releaseResources(AccordStateCache.Instance<K, V> cache)
        {
            items.values().forEach(cache::release);
            items.clear();
            writeOnly.clear();
        }

        public WriteOnly<K, V> getOrCreateWriteOnly(K key, BiFunction<AccordCommandStore, K, WriteOnly<K, V>> factory, AccordCommandStore commandStore)
        {
            Preconditions.checkState(!items.containsKey(key));
            WriteOnly<K, V> command = writeOnly.get(key);
            if (command == null)
            {
                command = factory.apply(commandStore, key);
                writeOnly.put(key, command);
            }
            return command;
        }
    }

    public static class CommandGroup extends Group<TxnId, AccordCommand>
    {
        List<AccordPartialCommand> partials = new ArrayList<>();

        public void addPartialCommand(AccordPartialCommand partial)
        {
            partials.add(partial);
        }

        @Override
        void releaseResources(AccordStateCache.Instance<TxnId, AccordCommand> cache)
        {
            super.releaseResources(cache);
            partials.clear();
        }
    }

    public final CommandGroup commands = new CommandGroup();
    public final Group<PartitionKey, AccordCommandsForKey> commandsForKey = new Group<>();

    public boolean containsScopedItems(PreLoadContext loadContext)
    {
        return Iterables.all(loadContext.txnIds(), commands.items::containsKey) && Iterables.all(loadContext.keys(), commandsForKey.items::containsKey);
    }

    void verifyLoaded()
    {
        commands.items.forEach((key, command) -> Preconditions.checkState(command.isLoaded()));
        commandsForKey.items.forEach((key, cfk) -> Preconditions.checkState(cfk.isLoaded()));
    }

    void releaseResources(AccordCommandStore commandStore)
    {
        commands.releaseResources(commandStore.commandCache());
        commandsForKey.releaseResources(commandStore.commandsForKeyCache());
    }
}
