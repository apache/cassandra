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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import accord.impl.CommandTimeseries;
import accord.impl.CommandsForKey;
import accord.impl.CommandsForKeyGroupUpdater;
import accord.impl.CommandsForKeyUpdater;
import accord.primitives.RoutableKey;
import accord.primitives.Timestamp;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.service.accord.api.PartitionKey;

import static org.apache.cassandra.utils.ObjectSizes.measure;

public class CommandsForKeyUpdate extends CommandsForKeyGroupUpdater.Immutable<ByteBuffer> implements CommandsForKey.Update
{
    private static final CommandsForKeyUpdate EMPTY = new CommandsForKeyUpdate(null, null, null, null);

    static long EMPTY_SIZE = measure(EMPTY);

    private static long EMPTY_TIMESERIES_UPDATE_SIZE = measure(new CommandTimeseries.ImmutableUpdate(ImmutableMap.of(), ImmutableSet.of()));

    private static <T extends Timestamp> long immutableTimeseriesUpdate(CommandTimeseries.ImmutableUpdate<T, ByteBuffer> update)
    {
        long size = EMPTY_TIMESERIES_UPDATE_SIZE;
        for (Map.Entry<T, ByteBuffer> write : update.writes.entrySet())
        {
            size += AccordObjectSizes.timestamp(write.getKey());
            size += ByteBufferAccessor.instance.size(write.getValue());
        }

        for (T delete : update.deletes)
            size += AccordObjectSizes.timestamp(delete);

        return size;
    }

    private static long EMPTY_UPDATER_SIZE = measure(new CommandsForKeyUpdater.Immutable<>(null));

    private static long updaterSize(CommandsForKeyUpdater.Immutable<ByteBuffer> updater)
    {
        long size = EMPTY_UPDATER_SIZE;
        size += immutableTimeseriesUpdate(updater.commands());
        return size;
    }

    private final PartitionKey key;

    public CommandsForKeyUpdate(PartitionKey key, CommandsForKeyUpdater.Immutable<ByteBuffer> deps, CommandsForKeyUpdater.Immutable<ByteBuffer> all, CommandsForKeyUpdater.Immutable<ByteBuffer> common)
    {
        super(deps, all, common);
        this.key = key;
    }

    public static CommandsForKeyUpdate empty(RoutableKey key)
    {
        return new CommandsForKeyUpdate((PartitionKey) key,
                                        CommandsForKeyUpdater.Immutable.empty(),
                                        CommandsForKeyUpdater.Immutable.empty(),
                                        CommandsForKeyUpdater.Immutable.empty());

    }

    public PartitionKey key()
    {
        return key;
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        size += AccordObjectSizes.key(key.asKey());
        size += updaterSize(deps());
        size += updaterSize(all());
        size += updaterSize(common());
        return size;
    }
}
