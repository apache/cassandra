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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import accord.api.Query;
import accord.api.Read;
import accord.api.Update;
import accord.api.Write;
import accord.coordinate.Infer;
import accord.local.Node;
import accord.local.StoreParticipants;
import accord.primitives.Ballot;
import accord.primitives.Known;
import accord.primitives.Known.Definition;
import accord.primitives.Known.KnownDeps;
import accord.primitives.Known.KnownExecuteAt;
import accord.primitives.Known.KnownRoute;
import accord.primitives.Known.Outcome;
import accord.primitives.PartialTxn;
import accord.primitives.Participants;
import accord.primitives.Route;
import accord.primitives.SaveStatus;
import accord.primitives.Seekables;
import accord.primitives.Status;
import accord.primitives.Status.Durability;
import accord.primitives.Timestamp;
import accord.primitives.Txn;
import accord.primitives.TxnId;
import accord.primitives.Writes;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ValueAccessor;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.service.accord.serializers.IVersionedWithKeysSerializer.AbstractWithKeysSerializer;
import org.apache.cassandra.service.accord.serializers.SmallEnumSerializer.NullableSmallEnumSerializer;
import org.apache.cassandra.service.accord.txn.AccordUpdate;
import org.apache.cassandra.service.accord.txn.TxnQuery;
import org.apache.cassandra.service.accord.txn.TxnRead;
import org.apache.cassandra.service.accord.txn.TxnWrite;
import org.apache.cassandra.utils.CastingSerializer;
import org.apache.cassandra.utils.NullableSerializer;

public class CommandSerializers
{
    private CommandSerializers()
    {
    }

    public static final TimestampSerializer<TxnId> txnId = new TimestampSerializer<>(TxnId::fromBits);
    public static final IVersionedSerializer<TxnId> nullableTxnId = NullableSerializer.wrap(txnId);
    public static final TimestampSerializer<Timestamp> timestamp = new TimestampSerializer<>(Timestamp::fromBits);
    public static final IVersionedSerializer<Timestamp> nullableTimestamp = NullableSerializer.wrap(timestamp);
    public static final TimestampSerializer<Ballot> ballot = new TimestampSerializer<>(Ballot::fromBits);
    public static final IVersionedSerializer<Ballot> nullableBallot = NullableSerializer.wrap(ballot);
    public static final EnumSerializer<Txn.Kind> kind = new EnumSerializer<>(Txn.Kind.class);
    public static final StoreParticipantsSerializer participants = new StoreParticipantsSerializer();

    // TODO (expected): optimise using subset serializers (but be careful for range txns, e.g. some collections have differently sliced sub ranges)
    public static class StoreParticipantsSerializer implements IVersionedSerializer<StoreParticipants>
    {
        static final int HAS_ROUTE = 0x1;
        static final int HAS_TOUCHED_EQUALS_ROUTE = 0x2;
        static final int TOUCHES_EQUALS_HAS_TOUCHED = 0x4;
        static final int OWNS_EQUALS_TOUCHES = 0x8;

        @Override
        public void serialize(StoreParticipants t, DataOutputPlus out, int version) throws IOException
        {
            boolean hasRoute = t.route() != null;
            boolean hasTouchedEqualsRoute = t.route() == t.hasTouched();
            boolean touchesEqualsHasTouched = t.touches() == t.hasTouched();
            boolean ownsEqualsTouches = t.owns() == t.touches();
            out.writeByte((hasRoute ? HAS_ROUTE : 0)
                          | (hasTouchedEqualsRoute ? HAS_TOUCHED_EQUALS_ROUTE : 0)
                          | (touchesEqualsHasTouched ? TOUCHES_EQUALS_HAS_TOUCHED : 0)
                          | (ownsEqualsTouches ? OWNS_EQUALS_TOUCHES : 0)
            );
            if (hasRoute) KeySerializers.route.serialize(t.route(), out, version);
            if (!hasTouchedEqualsRoute) KeySerializers.participants.serialize(t.hasTouched(), out, version);
            if (!touchesEqualsHasTouched) KeySerializers.participants.serialize(t.touches(), out, version);
            if (!ownsEqualsTouches) KeySerializers.participants.serialize(t.owns(), out, version);
        }

        public void skip(DataInputPlus in, int version) throws IOException
        {
            int flags = in.readByte();
            if (0 != (flags & HAS_ROUTE))
                KeySerializers.route.deserialize(in, version);
        }

        @Override
        public StoreParticipants deserialize(DataInputPlus in, int version) throws IOException
        {
            int flags = in.readByte();
            Route<?> route = 0 == (flags & HAS_ROUTE) ? null : KeySerializers.route.deserialize(in, version);
            Participants<?> hasTouched = 0 != (flags & HAS_TOUCHED_EQUALS_ROUTE) ? route : KeySerializers.participants.deserialize(in, version);
            Participants<?> touches = 0 != (flags & TOUCHES_EQUALS_HAS_TOUCHED) ? hasTouched : KeySerializers.participants.deserialize(in, version);
            Participants<?> owns = 0 != (flags & OWNS_EQUALS_TOUCHES) ? touches : KeySerializers.participants.deserialize(in, version);
            return StoreParticipants.SerializationSupport.create(route, owns, touches, hasTouched);
        }

        public Route<?> deserializeRouteOnly(DataInputPlus in, int version) throws IOException
        {
            int flags = in.readByte();
            if (0 == (flags & HAS_ROUTE))
                return null;

            return KeySerializers.route.deserialize(in, version);
        }

        @Override
        public long serializedSize(StoreParticipants t, int version)
        {
            boolean hasRoute = t.route() != null;
            boolean hasTouchedEqualsRoute = t.route() == t.hasTouched();
            boolean touchesEqualsHasTouched = t.touches() == t.hasTouched();
            boolean ownsEqualsTouches = t.owns() == t.touches();
            long size = 1;
            if (hasRoute) size += KeySerializers.route.serializedSize(t.route(), version);
            if (!hasTouchedEqualsRoute) size += KeySerializers.participants.serializedSize(t.hasTouched(), version);
            if (!touchesEqualsHasTouched) size += KeySerializers.participants.serializedSize(t.touches(), version);
            if (!ownsEqualsTouches) size += KeySerializers.participants.serializedSize(t.owns(), version);
            return size;
        }
    }

    public static class TimestampSerializer<T extends Timestamp> implements IVersionedSerializer<T>
    {
        interface Factory<T extends Timestamp>
        {
            T create(long msb, long lsb, Node.Id node);
        }

        private final TimestampSerializer.Factory<T> factory;

        private TimestampSerializer(TimestampSerializer.Factory<T> factory)
        {
            this.factory = factory;
        }

        @Override
        public void serialize(T ts, DataOutputPlus out, int version) throws IOException
        {
            out.writeLong(ts.msb);
            out.writeLong(ts.lsb);
            TopologySerializers.nodeId.serialize(ts.node, out, version);
        }

        public void serialize(T ts, DataOutputPlus out) throws IOException
        {
            out.writeLong(ts.msb);
            out.writeLong(ts.lsb);
            TopologySerializers.NodeIdSerializer.serialize(ts.node, out);
        }

        public <V> int serialize(T ts, V dst, ValueAccessor<V> accessor, int offset)
        {
            int position = offset;
            position += accessor.putLong(dst, position, ts.msb);
            position += accessor.putLong(dst, position, ts.lsb);
            position += TopologySerializers.nodeId.serialize(ts.node, dst, accessor, position);
            int size = position - offset;
            Preconditions.checkState(size == serializedSize());
            return size;
        }

        public void serialize(T ts, ByteBuffer out)
        {
            out.putLong(ts.msb);
            out.putLong(ts.lsb);
            TopologySerializers.nodeId.serialize(ts.node, out);
        }

        public void skip(DataInputPlus in, int version) throws IOException
        {
            in.skipBytesFully(serializedSize());
        }

        @Override
        public T deserialize(DataInputPlus in, int version) throws IOException
        {
            return factory.create(in.readLong(),
                                  in.readLong(),
                                  TopologySerializers.nodeId.deserialize(in, version));
        }

        public T deserialize(DataInputPlus in) throws IOException
        {
            return factory.create(in.readLong(),
                                  in.readLong(),
                                  TopologySerializers.NodeIdSerializer.deserialize(in));
        }

        public <V> T deserialize(V src, ValueAccessor<V> accessor, int offset)
        {
            long msb = accessor.getLong(src, offset);
            offset += TypeSizes.LONG_SIZE;
            long lsb = accessor.getLong(src, offset);
            offset += TypeSizes.LONG_SIZE;
            Node.Id node = TopologySerializers.nodeId.deserialize(src, accessor, offset);
            return factory.create(msb, lsb, node);
        }

        public T deserialize(ByteBuffer buffer, int position)
        {
            long msb = buffer.getLong(position);
            position += TypeSizes.LONG_SIZE;
            long lsb = buffer.getLong(position);
            position += TypeSizes.LONG_SIZE;
            Node.Id node = TopologySerializers.nodeId.deserialize(buffer, position);
            return factory.create(msb, lsb, node);
        }

        @Override
        public long serializedSize(T ts, int version)
        {
            return serializedSize();
        }

        public int serializedSize()
        {
            return TypeSizes.LONG_SIZE +  // ts.msb
                   TypeSizes.LONG_SIZE +  // ts.lsb
                   TopologySerializers.nodeId.serializedSize();   // ts.node
        }
    }

    public static class PartialTxnSerializer extends AbstractWithKeysSerializer
    implements IVersionedSerializer<PartialTxn>
    {
        private final IVersionedSerializer<Read> readSerializer;
        private final IVersionedSerializer<Query> querySerializer;
        private final IVersionedSerializer<Update> updateSerializer;

        public PartialTxnSerializer(IVersionedSerializer<Read> readSerializer, IVersionedSerializer<Query> querySerializer, IVersionedSerializer<Update> updateSerializer)
        {
            this.readSerializer = readSerializer;
            this.querySerializer = querySerializer;
            this.updateSerializer = updateSerializer;
        }

        @Override
        public void serialize(PartialTxn txn, DataOutputPlus out, int version) throws IOException
        {
            KeySerializers.seekables.serialize(txn.keys(), out, version);
            serializeWithoutKeys(txn, out, version);
        }

        @Override
        public PartialTxn deserialize(DataInputPlus in, int version) throws IOException
        {
            Seekables<?, ?> keys = KeySerializers.seekables.deserialize(in, version);
            return deserializeWithoutKeys(keys, in, version);
        }

        @Override
        public long serializedSize(PartialTxn txn, int version)
        {
            long size = KeySerializers.seekables.serializedSize(txn.keys(), version);
            size += serializedSizeWithoutKeys(txn, version);
            return size;
        }

        private void serializeWithoutKeys(PartialTxn txn, DataOutputPlus out, int version) throws IOException
        {
            CommandSerializers.kind.serialize(txn.kind(), out, version);
            readSerializer.serialize(txn.read(), out, version);
            querySerializer.serialize(txn.query(), out, version);
            out.writeBoolean(txn.update() != null);
            if (txn.update() != null)
                updateSerializer.serialize(txn.update(), out, version);
        }

        private PartialTxn deserializeWithoutKeys(Seekables<?, ?> keys, DataInputPlus in, int version) throws IOException
        {
            Txn.Kind kind = CommandSerializers.kind.deserialize(in, version);
            Read read = readSerializer.deserialize(in, version);
            Query query = querySerializer.deserialize(in, version);
            Update update = in.readBoolean() ? updateSerializer.deserialize(in, version) : null;
            return new PartialTxn.InMemory(kind, keys, read, query, update);
        }

        private long serializedSizeWithoutKeys(PartialTxn txn, int version)
        {
            long size = CommandSerializers.kind.serializedSize(txn.kind(), version);
            size += readSerializer.serializedSize(txn.read(), version);
            size += querySerializer.serializedSize(txn.query(), version);
            size += TypeSizes.sizeof(txn.update() != null);
            if (txn.update() != null)
                size += updateSerializer.serializedSize(txn.update(), version);
            return size;
        }
    }

    public static final IVersionedSerializer<Read> read;
    public static final IVersionedSerializer<Query> query;
    public static final IVersionedSerializer<Update> update;
    public static final IVersionedSerializer<Write> write;

    public static final IVersionedSerializer<PartialTxn> partialTxn;
    public static final IVersionedSerializer<PartialTxn> nullablePartialTxn;

    static
    {
        // We use a separate class for initialization to make it easier for BurnTest to plug its own serializers.
        QuerySerializers querySerializers = new QuerySerializers();
        read = querySerializers.read;
        query = querySerializers.query;
        update = querySerializers.update;
        write = querySerializers.write;

        partialTxn = querySerializers.partialTxn;
        nullablePartialTxn = querySerializers.nullablePartialTxn;
    }

    @VisibleForTesting
    public static class QuerySerializers
    {
        public final IVersionedSerializer<Read> read;
        public final IVersionedSerializer<Query> query;
        public final IVersionedSerializer<Update> update;
        public final IVersionedSerializer<Write> write;

        public final IVersionedSerializer<PartialTxn> partialTxn;
        public final IVersionedSerializer<PartialTxn> nullablePartialTxn;

        private QuerySerializers()
        {
            this(new CastingSerializer<>(TxnRead.class, TxnRead.serializer),
                 new CastingSerializer<>(TxnQuery.class, TxnQuery.serializer),
                 new CastingSerializer<>(AccordUpdate.class, AccordUpdate.serializer),
                 new CastingSerializer<>(TxnWrite.class, TxnWrite.serializer));
        }

        public QuerySerializers(IVersionedSerializer<Read> read,
                                IVersionedSerializer<Query> query,
                                IVersionedSerializer<Update> update,
                                IVersionedSerializer<Write> write)
        {
            this.read = read;
            this.query = query;
            this.update = update;
            this.write = write;

            this.partialTxn = new PartialTxnSerializer(read, query, update);
            this.nullablePartialTxn = NullableSerializer.wrap(partialTxn);
        }
    }

    public static final EnumSerializer<SaveStatus> saveStatus = new EnumSerializer<>(SaveStatus.class);
    public static final EnumSerializer<Status> status = new EnumSerializer<>(Status.class);
    public static final EnumSerializer<Durability> durability = new EnumSerializer<>(Durability.class);

    public static final IVersionedSerializer<Writes> writes = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Writes writes, DataOutputPlus out, int version) throws IOException
        {
            txnId.serialize(writes.txnId, out, version);
            timestamp.serialize(writes.executeAt, out, version);
            KeySerializers.seekables.serialize(writes.keys, out, version);
            boolean hasWrites = writes.write != null;
            out.writeBoolean(hasWrites);

            if (hasWrites)
                CommandSerializers.write.serialize(writes.write, out, version);
        }

        @Override
        public Writes deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Writes(txnId.deserialize(in, version), timestamp.deserialize(in, version),
                              KeySerializers.seekables.deserialize(in, version),
                              in.readBoolean() ? CommandSerializers.write.deserialize(in, version) : null);
        }

        @Override
        public long serializedSize(Writes writes, int version)
        {
            long size = txnId.serializedSize(writes.txnId, version);
            size += timestamp.serializedSize(writes.executeAt, version);
            size += KeySerializers.seekables.serializedSize(writes.keys, version);
            boolean hasWrites = writes.write != null;
            size += TypeSizes.sizeof(hasWrites);
            if (hasWrites)
                size += CommandSerializers.write.serializedSize(writes.write, version);
            return size;
        }
    };

    public static final IVersionedSerializer<Writes> nullableWrites = NullableSerializer.wrap(writes);

    public static final SmallEnumSerializer<KnownRoute> knownRoute = new SmallEnumSerializer<>(KnownRoute.class);
    public static final SmallEnumSerializer<Definition> definition = new SmallEnumSerializer<>(Definition.class);
    public static final SmallEnumSerializer<KnownExecuteAt> knownExecuteAt = new SmallEnumSerializer<>(KnownExecuteAt.class);
    public static final SmallEnumSerializer<KnownDeps> knownDeps = new SmallEnumSerializer<>(KnownDeps.class);
    public static final NullableSmallEnumSerializer<KnownDeps> nullableKnownDeps = new NullableSmallEnumSerializer<>(knownDeps);
    public static final SmallEnumSerializer<Outcome> outcome = new SmallEnumSerializer<>(Outcome.class);
    public static final SmallEnumSerializer<Infer.InvalidIf> invalidIf = new SmallEnumSerializer<>(Infer.InvalidIf.class);

    public static final IVersionedSerializer<Known> known = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(Known known, DataOutputPlus out, int version) throws IOException
        {
            knownRoute.serialize(known.route, out, version);
            definition.serialize(known.definition, out, version);
            knownExecuteAt.serialize(known.executeAt, out, version);
            knownDeps.serialize(known.deps, out, version);
            outcome.serialize(known.outcome, out, version);
        }

        @Override
        public Known deserialize(DataInputPlus in, int version) throws IOException
        {
            return new Known(knownRoute.deserialize(in, version),
                             definition.deserialize(in, version),
                             knownExecuteAt.deserialize(in, version),
                             knownDeps.deserialize(in, version),
                             outcome.deserialize(in, version));
        }

        @Override
        public long serializedSize(Known known, int version)
        {
            return knownRoute.serializedSize(known.route, version)
                   + definition.serializedSize(known.definition, version)
                   + knownExecuteAt.serializedSize(known.executeAt, version)
                   + knownDeps.serializedSize(known.deps, version)
                   + outcome.serializedSize(known.outcome, version);
        }
    };

    public static final IVersionedSerializer<Known> nullableKnown = NullableSerializer.wrap(known);
}