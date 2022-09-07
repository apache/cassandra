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
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import com.google.common.collect.ImmutableList;

import accord.api.Key;
import accord.api.Store;
import accord.api.Write;
import accord.txn.Timestamp;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordTimestamps;
import org.apache.cassandra.service.accord.api.AccordKey;
import org.apache.cassandra.service.accord.api.AccordKey.PartitionKey;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

public class AccordWrite extends AbstractKeyIndexed<PartitionUpdate> implements Write
{
    private static final Future<Void> SUCCESS = ImmediateFuture.success(null);
    public static final AccordWrite EMPTY = new AccordWrite(ImmutableList.of());

    public AccordWrite(List<PartitionUpdate> items)
    {
        super(items, AccordKey::of);
    }

    public AccordWrite(NavigableMap<PartitionKey, ByteBuffer> items)
    {
        super(items);
    }

    @Override
    void serialize(PartitionUpdate update, DataOutputPlus out, int version) throws IOException
    {
        PartitionUpdate.serializer.serialize(update, out, version);
    }

    @Override
    PartitionUpdate deserialize(DataInputPlus in, int version) throws IOException
    {
        return PartitionUpdate.serializer.deserialize(in, version, DeserializationHelper.Flag.FROM_REMOTE);
    }

    @Override
    long serializedSize(PartitionUpdate update, int version)
    {
        return PartitionUpdate.serializer.serializedSize(update, version);
    }

    @Override
    public Future<?> apply(Key key, Timestamp executeAt, Store store)
    {
        PartitionUpdate update = getDeserialized((PartitionKey) key);
        if (update == null)
            return SUCCESS;
        long timestamp = AccordTimestamps.timestampToMicros(executeAt);
        Mutation mutation = new Mutation(new PartitionUpdate.Builder(update, 0).updateAllTimestamp(timestamp).build());
        return Stage.MUTATION.submit((Runnable) mutation::apply);
    }

    public static final IVersionedSerializer<AccordWrite> serializer = new Serializer<>(AccordWrite::new);
}
