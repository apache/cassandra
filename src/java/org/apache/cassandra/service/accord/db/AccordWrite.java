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

import com.google.common.collect.ImmutableList;

import accord.api.DataStore;
import accord.api.Key;
import accord.api.Write;
import accord.local.SafeCommandStore;
import accord.primitives.Keys;
import accord.primitives.Timestamp;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.DeserializationHelper;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordCommandsForKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.concurrent.Future;
import org.apache.cassandra.utils.concurrent.ImmediateFuture;

public class AccordWrite extends AbstractKeyIndexed<PartitionUpdate> implements Write
{
    private static final Future<Void> SUCCESS = ImmediateFuture.success(null);
    public static final AccordWrite EMPTY = new AccordWrite(ImmutableList.of());
    private static final long EMPTY_SIZE = ObjectSizes.measureDeep(EMPTY);

    public AccordWrite(List<PartitionUpdate> items)
    {
        super(items, PartitionKey::of);
    }

    public AccordWrite(Keys keys, ByteBuffer[] serialized)
    {
        super(keys, serialized);
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
    long emptySizeOnHeap()
    {
        return EMPTY_SIZE;
    }

    @Override
    public Future<Void> apply(Key key, SafeCommandStore safeStore, Timestamp executeAt, DataStore store)
    {
        PartitionUpdate update = getDeserialized((PartitionKey) key);
        if (update == null)
            return SUCCESS;
        AccordCommandsForKey cfk = (AccordCommandsForKey) safeStore.commandsForKey(key);
        long timestamp = cfk.timestampMicrosFor(executeAt, true);
        int nowInSeconds = cfk.nowInSecondsFor(executeAt, true);
        update = new PartitionUpdate.Builder(update, 0).updateAllTimestampAndLocalDeletionTime(timestamp, nowInSeconds).build();
        Mutation mutation = new Mutation(update);
        return (Future<Void>) Stage.MUTATION.submit((Runnable) mutation::apply);
    }

    public static final IVersionedSerializer<AccordWrite> serializer = new Serializer<>(AccordWrite::new);
}
