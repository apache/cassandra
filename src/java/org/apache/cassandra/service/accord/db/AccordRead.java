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

package org.apache.cassandra.service.accord.db;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;

import accord.api.Data;
import accord.api.DataStore;
import accord.api.Key;
import accord.api.Read;
import accord.local.CommandStore;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

public class AccordRead extends AbstractKeyIndexed<SinglePartitionReadCommand> implements Read
{
    private static final long EMPTY_SIZE = ObjectSizes.measureDeep(new AccordRead(new TreeMap<>()));

    public AccordRead(List<SinglePartitionReadCommand> items)
    {
        super(items, AccordKey::of);
    }

    public AccordRead(NavigableMap<PartitionKey, ByteBuffer> serialized)
    {
        super(serialized);
    }

    @Override
    void serialize(SinglePartitionReadCommand command, DataOutputPlus out, int version) throws IOException
    {
        SinglePartitionReadCommand.serializer.serialize(command, out, version);
    }

    @Override
    SinglePartitionReadCommand deserialize(DataInputPlus in, int version) throws IOException
    {
        return (SinglePartitionReadCommand) SinglePartitionReadCommand.serializer.deserialize(in, version);
    }

    @Override
    long serializedSize(SinglePartitionReadCommand command, int version)
    {
        return SinglePartitionReadCommand.serializer.serializedSize(command, version);
    }

    @Override
    long emptySizeOnHeap()
    {
        return EMPTY_SIZE;
    }

    @Override
    public Keys keys()
    {
        return new Keys(serialized.keySet());
    }

    public String toString()
    {
        return "AccordRead{" + super.toString() + '}';
    }

    @Override
    public Future<Data> read(Key key, boolean isForWriteTxn, CommandStore commandStore, Timestamp executeAt, DataStore store)
    {
        SinglePartitionReadCommand command = getDeserialized((PartitionKey) key);
        if (command == null)
            return ImmediateFuture.success(new AccordData());

        AccordCommandsForKey cfk = (AccordCommandsForKey) commandStore.commandsForKey(key);
        int nowInSeconds = cfk.nowInSecondsFor(executeAt, isForWriteTxn);
        Future<Data> future = Stage.READ.submit(() -> {
            SinglePartitionReadCommand read = command.withNowInSec(nowInSeconds);
            try (ReadExecutionController controller = read.executionController();
                 UnfilteredPartitionIterator partition = read.executeLocally(controller);
                 PartitionIterator iterator = UnfilteredPartitionIterators.filter(partition, read.nowInSec()))
            {
                FilteredPartition filtered = FilteredPartition.create(PartitionIterators.getOnlyElement(iterator, read));
                AccordData result = new AccordData(filtered);
                return result;
            }
        });

        return future;
    }

    public static AccordRead forCommands(Collection<SinglePartitionReadCommand> commands)
    {
        List<SinglePartitionReadCommand> reads = new ArrayList<>(commands);
        reads.sort(Comparator.comparing(SinglePartitionReadCommand::partitionKey));
        return new AccordRead(reads);
    }

    public static final IVersionedSerializer<AccordRead> serializer = new Serializer<>(AccordRead::new);
}
