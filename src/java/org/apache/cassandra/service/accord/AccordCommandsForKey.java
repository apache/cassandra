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

import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import accord.local.Command;
import accord.local.CommandStore;
import accord.local.CommandsForKey;
import accord.txn.Timestamp;
import accord.txn.TxnId;
import org.apache.cassandra.service.accord.api.AccordKey;

public class AccordCommandsForKey extends CommandsForKey
{
    enum SeriesKind {
        UNCOMMITTED(Command::txnId),
        COMMITTED_BY_ID(Command::txnId),
        COMMITTED_BY_EXECUTE_AT(Command::executeAt);

        private final Function<Command, Timestamp> getTimestamp;

        SeriesKind(Function<Command, Timestamp> timestampFunction)
        {
            this.getTimestamp = timestampFunction;
        }
    }

    // TODO: should this just be in memory? If it's just (timestamp) pointers, it will be 56 bytes per entry. A little extra if we duplicate status into here as well
    // TODO: work out a compact timestamp serialization format. These should all be unsigned so it shouldn't be hard. Could just be byte arrays
    // TODO: the 2 by id only need a single 28b timestamp plus metadata
    // TODO: need txnId, isWrite, a lot of uses need deps, but that need a command
    private class Series implements CommandTimeseries
    {
        private final SeriesKind kind;

        public Series(SeriesKind kind)
        {
            this.kind = kind;
        }

        @Override
        public Command get(Timestamp timestamp)
        {
            TxnId txnId = AccordKeyspace.getCommandForKeySeriesEntry(commandStore, key, kind, timestamp);
            return commandStore.command(txnId);
        }

        @Override
        public void add(Timestamp timestamp, Command command)
        {
            AccordKeyspace.addCommandForKeySeriesEntry(commandStore, key, kind, timestamp, command.txnId());
        }

        @Override
        public void remove(Timestamp timestamp)
        {
            AccordKeyspace.removeCommandForKeySeriesEntry(commandStore, key, kind, timestamp);
        }

        private Stream<Command> idsToCommands(List<TxnId> ids)
        {
            return ids.stream().map(commandStore::command);

        }

        @Override
        public Stream<Command> before(Timestamp timestamp)
        {
            return idsToCommands(AccordKeyspace.commandsForKeysSeriesEntriesBefore(commandStore, key, kind, timestamp));
        }

        @Override
        public Stream<Command> after(Timestamp timestamp)
        {
            return idsToCommands(AccordKeyspace.commandsForKeysSeriesEntriesAfter(commandStore, key, kind, timestamp));
        }

        @Override
        public Stream<Command> between(Timestamp min, Timestamp max)
        {
            return idsToCommands(AccordKeyspace.commandsForKeysSeriesEntriesBetween(commandStore, key, kind, min, max));
        }

        @Override
        public Stream<Command> all()
        {
            return idsToCommands(AccordKeyspace.allCommandsForKeysSeriesEntries(commandStore, key, kind));
        }

        @Override
        public boolean isEmpty()
        {
            return AccordKeyspace.allCommandsForKeysSeriesEntries(commandStore, key, kind).isEmpty();
        }
    }

    private final CommandStore commandStore;
    private final AccordKey.PartitionKey key;
    private Timestamp max;
    private final Series uncommitted = new Series(SeriesKind.UNCOMMITTED);
    private final Series committedById = new Series(SeriesKind.COMMITTED_BY_ID);
    private final Series committedByExecuteAt = new Series(SeriesKind.COMMITTED_BY_EXECUTE_AT);

    public AccordCommandsForKey(CommandStore commandStore, AccordKey.PartitionKey key, Timestamp maxTimestamp)
    {
        this.commandStore = commandStore;
        this.key = key;
        max = maxTimestamp;
    }

    @Override
    public CommandTimeseries uncommitted()
    {
        return uncommitted;
    }

    @Override
    public CommandTimeseries committedById()
    {
        return committedById;
    }

    @Override
    public CommandTimeseries committedByExecuteAt()
    {
        return committedByExecuteAt;
    }

    @Override
    public Timestamp max()
    {
        return max;
    }

    @Override
    public void updateMax(Timestamp timestamp)
    {
        if (max.compareTo(timestamp) <= 0)
            return;
        max = timestamp;
        AccordKeyspace.updateCommandsForKeyMaxTimestamp(commandStore, key, max);
    }

    public void save()
    {
        // TODO: save accumulated updates
    }
}
