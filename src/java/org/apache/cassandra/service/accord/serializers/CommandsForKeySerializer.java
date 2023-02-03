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

package org.apache.cassandra.service.accord.serializers;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.google.common.annotations.VisibleForTesting;

import accord.impl.CommandsForKey.CommandLoader;
import accord.local.Command;
import accord.local.SaveStatus;
import accord.primitives.PartialDeps;
import accord.primitives.Timestamp;
import accord.primitives.TxnId;
import accord.utils.Invariants;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.LocalVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.AccordSerializerVersion;

public class CommandsForKeySerializer
{
    @VisibleForTesting
    public static final IVersionedSerializer<List<TxnId>> depsIdSerializer = new IVersionedSerializer<List<TxnId>>()
    {
        @Override
        public void serialize(List<TxnId> ids, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(ids.size());
            for (int i=0,mi=ids.size(); i<mi; i++)
                CommandSerializers.txnId.serialize(ids.get(i), out, version);
        }

        @Override
        public List<TxnId> deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readInt();
            List<TxnId> ids = new ArrayList<>(size);
            for (int i=0; i<size; i++)
                ids.add(CommandSerializers.txnId.deserialize(in, version));
            return ids;
        }

        @Override
        public long serializedSize(List<TxnId> ids, int version)
        {
            long size = TypeSizes.INT_SIZE;
            for (int i=0,mi=ids.size(); i<mi; i++)
                size += CommandSerializers.txnId.serializedSize(ids.get(i), version);
            return size;
        }
    };

    private static final LocalVersionedSerializer<List<TxnId>> depsIdsLocalSerializer = new LocalVersionedSerializer<>(AccordSerializerVersion.CURRENT, AccordSerializerVersion.serializer, depsIdSerializer);

    public static final CommandLoader<ByteBuffer> loader = new AccordCFKLoader();
    private static class AccordCFKLoader implements CommandLoader<ByteBuffer>
    {
        private static final int HAS_DEPS = 0x01;
        private static final int HAS_EXECUTE_AT = 0x02;

        private static final long FIXED_SIZE;
        private static final int FLAG_OFFSET;
        private static final int STATUS_OFFSET;
        private static final int TXNID_OFFSET;
        private static final int EXECUTEAT_OFFSET;
        private static final int DEPS_OFFSET;

        static
        {
            long size = 0;

            FLAG_OFFSET = (int) size;
            size += TypeSizes.BYTE_SIZE;

            STATUS_OFFSET = (int) size;
            size += TypeSizes.BYTE_SIZE;

            TXNID_OFFSET = (int) size;
            size += CommandSerializers.txnId.serializedSize();

            FIXED_SIZE = size;

            EXECUTEAT_OFFSET = (int) size;
            size += CommandSerializers.timestamp.serializedSize();

            DEPS_OFFSET = (int) size;
        }

        private int serializedSize(Command command)
        {
            return (int) (FIXED_SIZE
                          + (command.executeAt() != null ? CommandSerializers.timestamp.serializedSize() : 0)
                          + (command.partialDeps() != null ? depsIdsLocalSerializer.serializedSize(command.partialDeps().txnIds()) : 0));
        }

        private static final ValueAccessor<ByteBuffer> accessor = ByteBufferAccessor.instance;

        private static byte toByte(int v)
        {
            Invariants.checkArgument(v < Byte.MAX_VALUE, "Value %d is larger than %d", v, Byte.MAX_VALUE);
            return (byte) v;
        }

        private AccordCFKLoader() {}

        @Override
        public ByteBuffer saveForCFK(Command command)
        {
            int flags = 0;

            PartialDeps deps = command.partialDeps();
            Timestamp executeAt = command.executeAt();
            if (deps != null)
                flags |= HAS_DEPS;
            if (executeAt != null)
                flags |= HAS_EXECUTE_AT;

            int size = serializedSize(command);
            ByteBuffer buffer = accessor.allocate(size);
            accessor.putByte(buffer, FLAG_OFFSET, toByte(flags));
            accessor.putByte(buffer, STATUS_OFFSET, toByte(command.saveStatus().ordinal()));
            CommandSerializers.txnId.serialize(command.txnId(), buffer, accessor, TXNID_OFFSET);
            if (executeAt != null)
                CommandSerializers.timestamp.serialize(executeAt, buffer, accessor, EXECUTEAT_OFFSET);
            if (deps != null)
            {
                ByteBuffer duplicate = buffer.duplicate();
                duplicate.position(executeAt != null ? DEPS_OFFSET : EXECUTEAT_OFFSET);
                try (DataOutputBuffer out = new DataOutputBuffer(duplicate))
                {
                    depsIdsLocalSerializer.serialize(deps.txnIds(), out);
                }
                catch (IOException e)
                {
                    throw new RuntimeException(e);
                }
            }
            return buffer;
        }

        @Override
        public TxnId txnId(ByteBuffer data)
        {
            return CommandSerializers.txnId.deserialize(data, accessor, TXNID_OFFSET);
        }

        @Override
        public Timestamp executeAt(ByteBuffer data)
        {
            byte flags = accessor.getByte(data, FLAG_OFFSET);
            if ((flags & HAS_EXECUTE_AT) == 0)
                return null;
            return CommandSerializers.timestamp.deserialize(data, accessor, EXECUTEAT_OFFSET);
        }

        @Override
        public SaveStatus saveStatus(ByteBuffer data)
        {
            return SaveStatus.values()[accessor.getByte(data, STATUS_OFFSET)];
        }

        @Override
        public List<TxnId> depsIds(ByteBuffer data)
        {
            byte flags = accessor.getByte(data, FLAG_OFFSET);
            if ((flags & HAS_DEPS) == 0)
                return null;
            ByteBuffer buffer = data.duplicate();
            int offset = (flags & HAS_EXECUTE_AT) == 0 ? EXECUTEAT_OFFSET : DEPS_OFFSET;
            buffer.position(data.position() + offset);
            try (DataInputBuffer in = new DataInputBuffer(buffer, false))
            {
                return depsIdsLocalSerializer.deserialize(in);
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
