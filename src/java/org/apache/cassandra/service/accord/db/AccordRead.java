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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import accord.api.Data;
import accord.api.Key;
import accord.api.Read;
import accord.api.Store;
import accord.txn.Timestamp;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.PartitionIterators;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordTimestamps;
import org.apache.cassandra.service.accord.api.AccordKey;

public class AccordRead extends AbstractKeyIndexed<SinglePartitionReadCommand> implements Read
{
    public AccordRead(List<SinglePartitionReadCommand> items)
    {
        super(items);
    }

    public String toString()
    {
        return "AccordRead{" + super.toString() + '}';
    }

    @Override
    public Data read(Key key, Timestamp executeAt, Store store)
    {
        AccordData result = new AccordData();
        int nowInSeconds = AccordTimestamps.timestampToSeconds(executeAt);
        forEachIntersecting(((AccordKey) key), read -> {
            read = read.withNowInSec(nowInSeconds);
            try (ReadExecutionController controller = read.executionController();
                 UnfilteredPartitionIterator partition = read.executeLocally(controller))
            {
                PartitionIterator iterator = UnfilteredPartitionIterators.filter(partition, read.nowInSec());
                FilteredPartition filtered = FilteredPartition.create(PartitionIterators.getOnlyElement(iterator, read));
                result.put(filtered);
            }
        });
        return result;
    }

    public static AccordRead forCommands(Collection<SinglePartitionReadCommand> commands)
    {
        List<SinglePartitionReadCommand> reads = new ArrayList<>(commands);
        reads.sort(Comparator.comparing(SinglePartitionReadCommand::partitionKey));
        return new AccordRead(reads);
    }

    public static final IVersionedSerializer<AccordRead> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(AccordRead read, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(read.items.size());
            for (int i=0, mi=read.items.size(); i<mi; i++)
            {
                SinglePartitionReadCommand.serializer.serialize(read.items.get(i), out, version);
            }
        }

        @Override
        public AccordRead deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readInt();
            List<SinglePartitionReadCommand> commands = new ArrayList<>(size);
            for (int i=0; i<size; i++)
            {
                ReadCommand command = SinglePartitionReadCommand.serializer.deserialize(in, version);
                commands.add((SinglePartitionReadCommand) command);
            }
            return new AccordRead(commands);
        }

        @Override
        public long serializedSize(AccordRead read, int version)
        {
            long size = TypeSizes.sizeof(read.items.size());
            for (int i=0, mi=read.items.size(); i<mi; i++)
            {
                size += SinglePartitionReadCommand.serializer.serializedSize(read.items.get(i), version);
            }
            return size;
        }
    };
}
