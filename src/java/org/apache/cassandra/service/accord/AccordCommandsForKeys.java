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

import java.nio.ByteBuffer;
import java.util.function.Function;

import accord.api.Key;
import accord.impl.CommandsForKey;
import accord.impl.CommandsForKeyGroupUpdater;
import accord.impl.CommandsForKeyUpdater;
import accord.primitives.RoutableKey;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import org.apache.cassandra.concurrent.ExecutorPlus;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.CommandsForKeySerializer;

/**
 * Ties together the separate commands for key and commands for key update classes to make loading,
 * saving, and evicting coherent
 */
public class AccordCommandsForKeys
{
    private final AccordCommandStore commandStore;

    public AccordCommandsForKeys(AccordCommandStore commandStore)
    {
        this.commandStore = commandStore;
    }

    AccordCachingState<RoutableKey, CommandsForKey> createDepsCommandsNode(RoutableKey key, int index)
    {
        return new DepsCommandsCachingState(key, index);
    }

    AccordCachingState<RoutableKey, CommandsForKey> createAllCommandsNode(RoutableKey key, int index)
    {
        return new AllCommandsCachingState(key, index);
    }

    AccordCachingState<RoutableKey, CommandsForKeyUpdate> createUpdatesNode(RoutableKey key, int index)
    {
        return new UpdateCachingState(key, index);
    }

    protected static boolean hasEvictableStatus(AccordCachingState<?,?> state)
    {
        if (state == null)
            return true;

        switch (state.status())
        {
            case LOADING:
            case SAVING:
                return false;
        }

        return true;
    }

    boolean canEvictKey(RoutableKey key)
    {
        return hasEvictableStatus(commandStore.depsCommandsForKeyCache().getUnsafe(key))
               && hasEvictableStatus(commandStore.allCommandsForKeyCache().getUnsafe(key))
               && hasEvictableStatus(commandStore.updatesForKeyCache().getUnsafe(key));
    }

    public abstract class CommandsCachingState extends AccordCachingState<RoutableKey, CommandsForKey>
    {
        protected CommandsCachingState(RoutableKey key, int index)
        {
            super(key, index);
        }

        private CommandsForKey initializeIfNull(CommandsForKey commands)
        {
            if (commands != null)
                return commands;
            return new CommandsForKey((Key) key(), CommandsForKeySerializer.loader);
        }

        private State<RoutableKey, CommandsForKey> maybeApplyUpdates(State<RoutableKey, CommandsForKey> state)
        {
            if (!(state instanceof Loaded))
                return state;

            Loaded<RoutableKey, CommandsForKey> loaded = (Loaded<RoutableKey, CommandsForKey>) state;
            CommandsForKey commands = loaded.get();
            UpdateCachingState updates = (UpdateCachingState) commandStore.updatesForKeyCache().getUnsafe(key());
            if (updates == null)
                return loaded;

            CommandsForKeyUpdate update = updates.getUpdateIfAvailable();
            if (update == null)
                return loaded;
            CommandsForKey updated = apply(initializeIfNull(commands), update);
            if (updated == commands)
                return loaded;

            return new Loaded<>(updated);
        }

        protected abstract CommandsForKey apply(CommandsForKey current, CommandsForKeyUpdate update);

        private void maybeApplyUpdates(CommandsForKeyUpdate update)
        {
            if (status() != Status.LOADED)
                return;

            CommandsForKey commands = get();
            CommandsForKey updated = apply(initializeIfNull(commands), update);
            if (commands != updated)
                super.state(new Loaded<>(updated));
        }

        protected State<RoutableKey, CommandsForKey> state(State<RoutableKey, CommandsForKey> next)
        {
            Status nextStatus = next.status();
            Invariants.checkState(nextStatus != Status.MODIFIED && nextStatus != Status.SAVING,
                                  "CommandsForKey cannot have state %s", nextStatus);

            return super.state(maybeApplyUpdates(next));
        }

        @Override
        public boolean canEvict()
        {
            return canEvictKey(key());
        }
    }

    public class DepsCommandsCachingState extends CommandsCachingState
    {
        public DepsCommandsCachingState(RoutableKey key, int index)
        {
            super(key, index);
        }

        protected CommandsForKey apply(CommandsForKey current, CommandsForKeyUpdate update)
        {
            return update.applyToDeps(current);
        }
    }

    public class AllCommandsCachingState extends CommandsCachingState
    {
        public AllCommandsCachingState(RoutableKey key, int index)
        {
            super(key, index);
        }

        protected CommandsForKey apply(CommandsForKey current, CommandsForKeyUpdate update)
        {
            return update.applyToAll(current);
        }
    }

    public class UpdateCachingState extends AccordCachingState<RoutableKey, CommandsForKeyUpdate> implements CommandsForKeyGroupUpdater.Immutable.Factory<ByteBuffer, CommandsForKeyUpdate>
    {
        public UpdateCachingState(RoutableKey key, int index)
        {
            super(key, index);
        }

        public AsyncChain<CommandsForKeyUpdate> load(ExecutorPlus executor, Function<RoutableKey, CommandsForKeyUpdate> loadFunction)
        {
            if (status() == Status.UNINITIALIZED)
            {
                CommandsForKeyUpdate initialized = CommandsForKeyUpdate.empty(key());
                state(state().initialize(initialized));
                return null;
            }

            return super.load(executor, loadFunction);
        }

        // update in memory cfk data with the update results
        protected void maybeUpdateCommands(CommandsForKeyUpdate update)
        {
            CommandsCachingState commands = (CommandsCachingState) commandStore.depsCommandsForKeyCache().getUnsafe(key());
            if (commands == null)
                return;

            commands.maybeApplyUpdates(update);
        }

        public CommandsForKeyUpdate create(CommandsForKeyUpdater.Immutable<ByteBuffer> deps, CommandsForKeyUpdater.Immutable<ByteBuffer> all, CommandsForKeyUpdater.Immutable<ByteBuffer> common)
        {
            return new CommandsForKeyUpdate((PartitionKey) key(), deps, all, common);
        }

        protected State<RoutableKey, CommandsForKeyUpdate> maybeProcessModification(State<RoutableKey, CommandsForKeyUpdate> next)
        {
            if (!(next instanceof Modified))
                return next;

            Modified<RoutableKey, CommandsForKeyUpdate> modified = (Modified<RoutableKey, CommandsForKeyUpdate>) next;

            CommandsForKeyUpdate current = modified.current;
            maybeUpdateCommands(current);

            // combine in memory updates
            current = CommandsForKeyGroupUpdater.Immutable.merge(modified.original, current, this);

            return new Modified<>(null, current);
        }

        protected State<RoutableKey, CommandsForKeyUpdate> state(State<RoutableKey, CommandsForKeyUpdate> next)
        {
            Status nextStatus = next.status();
            Invariants.checkState(nextStatus != Status.LOADING,
                                  "CommandsForKeyUpdate cannot have state %s", nextStatus);

            return super.state(maybeProcessModification(next));
        }

        CommandsForKeyUpdate getUpdateIfAvailable()
        {
            switch (status())
            {
                case LOADED:
                case MODIFIED:
                    return get();
            }

            return null;
        }

        @Override
        public boolean canEvict()
        {
            return canEvictKey(key());
        }
    }
}
