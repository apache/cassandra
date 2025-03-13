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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

import accord.primitives.Deps;
import accord.primitives.KeyDeps;
import accord.primitives.PartialDeps;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.RangeDeps;
import accord.primitives.RoutingKeys;
import accord.primitives.TxnId;
import org.apache.cassandra.io.UnversionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.utils.NullableSerializer;

import static accord.primitives.KeyDeps.SerializerSupport.keysToTxnIds;
import static accord.primitives.KeyDeps.SerializerSupport.keysToTxnIdsCount;
import static accord.primitives.RangeDeps.SerializerSupport.rangesToTxnIds;
import static accord.primitives.RangeDeps.SerializerSupport.rangesToTxnIdsCount;
import static org.apache.cassandra.db.TypeSizes.sizeofUnsignedVInt;

public class DepsSerializers
{
    public static final UnversionedSerializer<Range> tokenRange;
    public static final DepsSerializer<Deps> deps;
    public static final UnversionedSerializer<Deps> nullableDeps;
    public static final DepsSerializer<PartialDeps> partialDeps;
    public static final UnversionedSerializer<PartialDeps> nullablePartialDeps;

    static
    {
        // We use a separate class for initialization to make it easier for BurnTest to plug its own serializers.
        Impl serializers = new Impl((UnversionedSerializer<Range>) (UnversionedSerializer<?>) TokenRange.serializer);
        tokenRange = serializers.tokenRange;
        deps = serializers.deps;
        nullableDeps = serializers.nullableDeps;
        partialDeps = serializers.partialDeps;
        nullablePartialDeps = serializers.nullablePartialDeps;
    }

    public static abstract class DepsSerializer<D extends Deps> implements UnversionedSerializer<D>
    {
        protected UnversionedSerializer<Range> tokenRange;
        public DepsSerializer(UnversionedSerializer<Range> tokenRange)
        {
            this.tokenRange = tokenRange;
        }

        abstract D deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, DataInputPlus in) throws IOException;

        @Override
        public void serialize(D deps, DataOutputPlus out) throws IOException
        {
            {
                KeyDeps keyDeps = deps.keyDeps;
                KeySerializers.routingKeys.serialize(keyDeps.keys(), out);
                int txnIdCount = keyDeps.txnIdCount();
                out.writeUnsignedVInt32(txnIdCount);
                for (int i = 0; i < txnIdCount; i++)
                    CommandSerializers.txnId.serialize(keyDeps.txnId(i), out);

                int keysToTxnIdsCount = keysToTxnIdsCount(keyDeps);
                out.writeUnsignedVInt32(keysToTxnIdsCount);
                for (int i = 0; i < keysToTxnIdsCount; i++)
                    out.writeUnsignedVInt32(keysToTxnIds(keyDeps, i));
            }
            {
                RangeDeps rangeDeps = deps.rangeDeps;
                int rangeCount = rangeDeps.rangeCount();
                out.writeUnsignedVInt32(rangeCount);
                for (int i = 0; i < rangeCount; i++)
                    tokenRange.serialize(rangeDeps.range(i), out);

                int txnIdCount = rangeDeps.txnIdCount();
                out.writeUnsignedVInt32(txnIdCount);
                for (int i = 0; i < txnIdCount; i++)
                    CommandSerializers.txnId.serialize(rangeDeps.txnId(i), out);

                int rangesToTxnIdsCount = rangesToTxnIdsCount(rangeDeps);
                out.writeUnsignedVInt32(rangesToTxnIdsCount);
                for (int i = 0; i < rangesToTxnIdsCount; i++)
                    out.writeUnsignedVInt32(rangesToTxnIds(rangeDeps, i));
            }
        }

        @Override
        public D deserialize(DataInputPlus in) throws IOException
        {
            KeyDeps keyDeps;
            {
                RoutingKeys keys = KeySerializers.routingKeys.deserialize(in);
                int txnIdCount = in.readUnsignedVInt32();
                TxnId[] txnIds = new TxnId[txnIdCount];
                for (int i = 0; i < txnIdCount; i++)
                    txnIds[i] = CommandSerializers.txnId.deserialize(in);

                int keysToTxnIdsCount = in.readUnsignedVInt32();
                int[] keysToTxnIds = new int[keysToTxnIdsCount];
                for (int i = 0; i < keysToTxnIdsCount; i++)
                    keysToTxnIds[i] = in.readUnsignedVInt32();

                keyDeps = KeyDeps.SerializerSupport.create(keys, txnIds, keysToTxnIds);
            }

            RangeDeps rangeDeps;
            {
                int rangeCount = Ints.checkedCast(in.readUnsignedVInt32());
                Range[] ranges = new Range[rangeCount];
                for (int i = 0; i < rangeCount; i++)
                    ranges[i] = tokenRange.deserialize(in);

                int txnIdCount = in.readUnsignedVInt32();
                TxnId[] txnIds = new TxnId[txnIdCount];
                for (int i = 0; i < txnIdCount; i++)
                    txnIds[i] = CommandSerializers.txnId.deserialize(in);

                int rangesToTxnIdsCount = in.readUnsignedVInt32();
                int[] rangesToTxnIds = new int[rangesToTxnIdsCount];
                for (int i = 0; i < rangesToTxnIdsCount; i++)
                    rangesToTxnIds[i] = in.readUnsignedVInt32();

                rangeDeps = RangeDeps.SerializerSupport.create(ranges, txnIds, rangesToTxnIds);
            }
            return deserialize(keyDeps, rangeDeps, in);
        }

        @Override
        public long serializedSize(D deps)
        {
            long size;
            {
                KeyDeps keyDeps = deps.keyDeps;
                size = KeySerializers.routingKeys.serializedSize(deps.keyDeps.keys());
                int txnIdCount = keyDeps.txnIdCount();
                size += sizeofUnsignedVInt(txnIdCount);
                for (int i = 0; i < txnIdCount; i++)
                    size += CommandSerializers.txnId.serializedSize(keyDeps.txnId(i));

                int keysToTxnIdsCount = keysToTxnIdsCount(keyDeps);
                size += sizeofUnsignedVInt(keysToTxnIdsCount);
                for (int i = 0; i < keysToTxnIdsCount; i++)
                    size += sizeofUnsignedVInt(keysToTxnIds(keyDeps, i));
            }

            {
                RangeDeps rangeDeps = deps.rangeDeps;
                int rangeCount = rangeDeps.rangeCount();
                size += sizeofUnsignedVInt(rangeCount);
                for (int i = 0; i < rangeCount; ++i)
                    size += tokenRange.serializedSize(rangeDeps.range(i));

                int txnIdCount = rangeDeps.txnIdCount();
                size += sizeofUnsignedVInt(txnIdCount);
                for (int i = 0; i < txnIdCount; i++)
                    size += CommandSerializers.txnId.serializedSize(rangeDeps.txnId(i));

                int rangesToTxnIdsCount = rangesToTxnIdsCount(rangeDeps);
                size += sizeofUnsignedVInt(rangesToTxnIdsCount);
                for (int i = 0; i < rangesToTxnIdsCount; i++)
                    size += sizeofUnsignedVInt(rangesToTxnIds(rangeDeps, i));
            }
            return size;
        }
    }

    @VisibleForTesting
    public static class Impl
    {
        final UnversionedSerializer<Range> tokenRange;
        final DepsSerializer<Deps> deps;
        final UnversionedSerializer<Deps> nullableDeps;
        final DepsSerializer<PartialDeps> partialDeps;
        final UnversionedSerializer<PartialDeps> nullablePartialDeps;

        public Impl(UnversionedSerializer<Range> tokenRange)
        {
            this.tokenRange = tokenRange;
            this.deps = new DepsSerializer<>(tokenRange)
            {
                @Override
                Deps deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, DataInputPlus in)
                {
                    return new Deps(keyDeps, rangeDeps);
                }
            };
            this.nullableDeps = NullableSerializer.wrap(deps);
            this.partialDeps = new DepsSerializer<>(tokenRange)
            {
                @Override
                PartialDeps deserialize(KeyDeps keyDeps, RangeDeps rangeDeps, DataInputPlus in) throws IOException
                {
                    Participants<?> covering = KeySerializers.participants.deserialize(in);
                    return new PartialDeps(covering, keyDeps, rangeDeps);
                }

                @Override
                public void serialize(PartialDeps partialDeps, DataOutputPlus out) throws IOException
                {
                    super.serialize(partialDeps, out);
                    KeySerializers.participants.serialize(partialDeps.covering, out);
                }

                @Override
                public long serializedSize(PartialDeps partialDeps)
                {
                    return super.serializedSize(partialDeps)
                           + KeySerializers.participants.serializedSize(partialDeps.covering);
                }
            };

            this.nullablePartialDeps = NullableSerializer.wrap(partialDeps);
        }
    }
}