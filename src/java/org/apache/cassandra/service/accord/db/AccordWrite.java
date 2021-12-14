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
import java.util.List;

import com.google.common.collect.ImmutableList;

import accord.api.Store;
import accord.api.Write;
import accord.topology.KeyRanges;
import accord.txn.Timestamp;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordTimestamps;

public class AccordWrite extends AbstractKeyIndexed<PartitionUpdate> implements Write
{
    public static final AccordWrite EMPTY = new AccordWrite(ImmutableList.of());

    public AccordWrite(List<PartitionUpdate> items)
    {
        super(items);
    }

    @Override
    public void apply(KeyRanges ranges, Timestamp executeAt, Store store)
    {
        // TODO: put into write stage
        long timestamp = AccordTimestamps.timestampToMicros(executeAt);
        forEachIntersecting(ranges, update -> {
            Mutation mutation = new Mutation(new PartitionUpdate.Builder(update, 0).updateAllTimestamp(timestamp).build());
            mutation.apply();
        });
    }

    public static final IVersionedSerializer<AccordWrite> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(AccordWrite write, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(write.items.size());
            for (PartitionUpdate update : write.items)
                PartitionUpdate.serializer.serialize(update, out, version);
        }

        @Override
        public AccordWrite deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readInt();
            List<PartitionUpdate> writes = new ArrayList<>(size);
            for (int i=0; i<size; i++)
                writes.add(PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.FROM_REMOTE));
            return new AccordWrite(writes);
        }

        @Override
        public long serializedSize(AccordWrite write, int version)
        {
            long size = TypeSizes.sizeof(write.items.size());
            for (PartitionUpdate update : write.items)
                size += PartitionUpdate.serializer.serializedSize(update, version);
            return size;
        }
    };
}
