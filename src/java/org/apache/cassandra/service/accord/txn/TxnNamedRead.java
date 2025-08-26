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

package org.apache.cassandra.service.accord.txn;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.RoutingKey;
import accord.primitives.Range;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.utils.Invariants;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterators;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.Token.KeyBound;
import org.apache.cassandra.io.ParameterisedVersionedSerializer;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordExecutor;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.TableMetadatas;
import org.apache.cassandra.service.accord.serializers.TableMetadatasAndKeys;
import org.apache.cassandra.service.accord.api.TokenKey;
import org.apache.cassandra.service.accord.serializers.Version;
import org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Comparables;
import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.io.util.DataOutputBuffer.scratchBuffer;
import static org.apache.cassandra.utils.ByteBufferUtil.readWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.serializedSizeWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.skipWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.writeWithVIntLength;

public class TxnNamedRead extends AbstractParameterisedVersionedSerialized<ReadCommand, TableMetadatas>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TxnNamedRead.class);

    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnNamedRead(0, null, null));

    private final int name;
    private final Seekable key;

    public TxnNamedRead(int name, PartitionKey key, SinglePartitionReadCommand value, TableMetadatas tables)
    {
        super(serializeInternal(value, tables, Version.LATEST));
        this.name = name;
        this.key = key;
    }

    public TxnNamedRead(int name, AbstractBounds<PartitionPosition> range, PartitionRangeReadCommand value, TableMetadatas tables)
    {
        super(serializeInternal(value, tables, Version.LATEST));
        TableId tableId = value.metadata().id;
        this.name = name;
        this.key = boundsAsAccordRange(range, tableId);
    }

    TxnNamedRead(int name, Seekable key, ByteBuffer bytes)
    {
        super(bytes);
        this.name = name;
        this.key = key;
    }

    public static TokenRange boundsAsAccordRange(AbstractBounds<PartitionPosition> range, TableId tableId)
    {
        // Should already have been unwrapped
        checkState(!AbstractBounds.strictlyWrapsAround(range.left, range.right));

        // Read commands can contain a mix of different kinds of bounds to facilitate paging
        // and we need to communicate that to Accord as its own ranges. This uses
        // TokenKey, SentinelKey, and MinTokenKey and sticks exclusively with left exclusive/right inclusive
        // ranges rather add more types of ranges to the mix
        // MinTokenKey allows emulating inclusive left and exclusive right with Range
        boolean inclusiveLeft = range.inclusiveLeft();
        PartitionPosition startPP = range.left;
        boolean startIsMinKeyBound = startPP.getClass() == KeyBound.class ? ((KeyBound)startPP).isMinimumBound : false;
        Token startToken = startPP.getToken();
        Token stopToken = range.right.getToken();
        TokenKey startTokenKey;
        if (startToken.isMinimum() && inclusiveLeft)
            startTokenKey = TokenKey.min(tableId, startToken.getPartitioner());
        else if (inclusiveLeft || startIsMinKeyBound || startToken.equals(stopToken))
            startTokenKey = TokenKey.before(tableId, startToken);
        else
            startTokenKey = new TokenKey(tableId, startToken);

        boolean inclusiveRight = range.inclusiveRight();
        PartitionPosition endPP = range.right;
        boolean endIsMinKeyBound = endPP.getClass() == KeyBound.class ? ((KeyBound)endPP).isMinimumBound : false;
        TokenKey stopTokenKey;
        if (stopToken.isMinimum())
            stopTokenKey = TokenKey.max(tableId, startToken.getPartitioner());
        else if (inclusiveRight && !endIsMinKeyBound)
            stopTokenKey = new TokenKey(tableId, stopToken);
        else
            stopTokenKey = TokenKey.before(tableId, stopToken);
        return TokenRange.create(startTokenKey, stopTokenKey);
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        // we don't measure the key, as this is shared
        size += (unsafeBytes() != null ? ByteBufferUtil.estimatedSizeOnHeap(unsafeBytes()) : 0);
        return size;
    }

    @Override
    protected ByteBuffer serialize(ReadCommand value, TableMetadatas param, Version version)
    {
        return serializeInternal(value, param, version);
    }

    private static ByteBuffer serializeInternal(ReadCommand value, TableMetadatas param, Version version)
    {
        try (DataOutputBuffer buffer = scratchBuffer.get())
        {
            ReadCommand.serializer.serializeForAccord(value, param, buffer, version.messageVersion());
            return buffer.asNewBuffer();
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    protected ByteBuffer reserialize(ByteBuffer buffer, TableMetadatas param, Version srcVersion, Version trgVersion)
    {
        return buffer;
    }

    @Override
    protected ReadCommand deserialize(TableMetadatas param, ByteBuffer bytes, Version version)
    {
        try (DataInputBuffer buffer = new DataInputBuffer(bytes, true))
        {
            return ReadCommand.serializer.deserializeForAccord(key, param, buffer, version.messageVersion());
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;
        TxnNamedRead namedRead = (TxnNamedRead) o;
        return name == namedRead.name && key.equals(namedRead.key);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(super.hashCode(), name, key);
    }

    @Override
    public String toString()
    {
        return "TxnNamedRead{name='" + name + '\'' + ", keys=" + key + '}';
    }

    public int txnDataName()
    {
        return name;
    }

    public Seekable key()
    {
        return key;
    }

    public static long nowInSeconds(Timestamp executeAt)
    {
        return TimeUnit.MICROSECONDS.toSeconds(executeAt.hlc());
    }

    public AsyncChain<Data> read(AccordExecutor executor, TableMetadatas tables, ConsistencyLevel consistencyLevel, Seekable key, Timestamp executeAt)
    {
        ReadCommand command = deserialize(tables);
        if (command == null)
            return AsyncResults.success(new TxnData());

        // It's fine for our nowInSeconds to lag slightly our insertion timestamp, as to the user
        // this simply looks like the transaction witnessed TTL'd data and the data then expired
        // immediately after the transaction executed, and this simplifies things a great deal
        long nowInSeconds = nowInSeconds(executeAt);

        boolean withoutReconciliation = readsWithoutReconciliation(consistencyLevel);
        switch (key.domain())
        {
            case Key:
                return performLocalKeyRead(executor, ((SinglePartitionReadCommand) command).withTransactionalSettings(withoutReconciliation, nowInSeconds));
            case Range:
                return performLocalRangeRead(executor, ((PartitionRangeReadCommand) command), key.asRange(), consistencyLevel, nowInSeconds);
            default:
                throw new IllegalStateException("Unhandled domain " + key.domain());
        }
    }

    public TxnNamedRead slice(Range range)
    {
        Invariants.require(key.domain().isRange());
        if (key.equals(range))
            return this;

        Invariants.require(((Range)key).contains(range));
        return new TxnNamedRead(txnDataName(), range, unsafeBytes());
    }

    public TxnNamedRead merge(TxnNamedRead with)
    {
        Invariants.require(key.domain().isRange());
        if (key.equals(with.key))
            return this;

        Range thisRange = key.asRange();
        Range thatRange = with.key.asRange();
        Invariants.require(thisRange.compareTouching(thatRange) == 0);
        RoutingKey start = Comparables.min(thisRange.start(), thatRange.start());
        RoutingKey end = Comparables.max(thisRange.end(), thatRange.end());
        Range range = thisRange.newRange(start, end);
        return new TxnNamedRead(txnDataName(), range, unsafeBytes());
    }

    public static boolean readsWithoutReconciliation(ConsistencyLevel consistencyLevel)
    {
        boolean withoutReconciliation = consistencyLevel == null || consistencyLevel == ConsistencyLevel.ONE;
        return withoutReconciliation;
    }


    public ReadCommand command(TableMetadatas tables)
    {
        return deserialize(tables);
    }

    private AsyncChain<Data> performLocalKeyRead(AccordExecutor executor, SinglePartitionReadCommand command)
    {
        Callable<Data> callable = new Callable<>()
        {
            @Override
            public Data call()
            {
                try (ReadExecutionController controller = command.executionController();
                     PartitionIterator iterator = UnfilteredPartitionIterators.filter(command.executeLocally(controller), command.nowInSec()))
                {
                    TxnData result = new TxnData();
                    if (iterator.hasNext())
                    {
                        TxnDataKeyValue value = new TxnDataKeyValue(iterator.next());
                        if (value.hasRows() || command.selectsFullPartition())
                            result.put(name, value);
                    }
                    return result;
                }
            }

            @Override
            public String toString()
            {
                return command.toCQLString();
            }
        };

        return submit(executor, callable, callable);
    }

    public static PartitionRangeReadCommand commandForSubrange(PartitionRangeReadCommand command, Range r, ConsistencyLevel consistencyLevel, long nowInSeconds)
    {
        AbstractBounds<PartitionPosition> bounds = command.dataRange().keyRange();
        PartitionPosition startPP = bounds.left;
        PartitionPosition endPP = bounds.right;
        TokenKey startRoutingKey = ((TokenKey)r.start());
        TokenKey endRoutingKey = ((TokenKey)r.end());
        Token subRangeStartToken = startRoutingKey.isMin() ? startPP.getToken() : startRoutingKey.token();
        Token subRangeEndToken = endRoutingKey.isMax() ? endPP.getToken() : endRoutingKey.token();

        /*
         * The way ranges/bounds work for range queries is that the beginning and ending bounds from the command
         * could be tokens (and min/max key bounds) or actual keys depending on the bounds of the top level query we
         * are running and where we are in paging. We need to preserve whatever is in the command in case it is a
         * key and not a token, or it's a token but might be a min/max key bound.
         *
         * Then Accord will further subdivide the range in the command so need to inject additional bounds in the middle
         * that match the range ownership of Accord.
         *
         * The command still contains the original bound and then the Accord range passed in determines what subset of
         * that bound we want. We have to make sure to use the bounds from the command if it is the start or end instead
         * of a key bound created from the Accord range since it could be a real key or min/max bound.
         *
         * When we are dealing with a bound created by Accord's further subdivision we use a maxKeyBound (exclusive)
         * for both beginning and end because Bounds is left and right inclusive while Range is only left inclusive.
         * We only use TokenRange with Accord which matches the left/right inclusivity of Cassandra's Range.
         *
         * That means the Range we get from Accord overlaps the previous Range on the left which when converted to a Bound
         * would potentially read the same Token twice. So the left needs to be a maxKeyBound to exclude the data that isn't
         * owned here and to avoid potentially reading the same data twice. The right bound also needs to be a maxKeyBound since Range
         * is right inclusive so every partition we find needs to be < the right bound.
         */
        boolean isFirstSubrange = startPP.getToken().equals(subRangeStartToken);
        PartitionPosition subRangeStartPP = isFirstSubrange ? startPP : subRangeStartToken.maxKeyBound();
        PartitionPosition subRangeEndPP = endPP.getToken().equals(subRangeEndToken) ? endPP : subRangeEndToken.maxKeyBound();
        // Need to preserve the fact it is a bounds for paging to work, a range is not left inclusive and will not start from where we left off
        AbstractBounds<PartitionPosition> subRange = isFirstSubrange ? bounds.withNewRight(subRangeEndPP) : new org.apache.cassandra.dht.Range(subRangeStartPP, subRangeEndPP);
        boolean isRangeContinuation = startPP.getToken().equals(subRangeStartToken);
        return command.withTransactionalSettings(nowInSeconds, subRange, isRangeContinuation, readsWithoutReconciliation(consistencyLevel));
    }

    private AsyncChain<Data> performLocalRangeRead(AccordExecutor executor, PartitionRangeReadCommand command, Range r, ConsistencyLevel consistencyLevel, long nowInSeconds)
    {
        PartitionRangeReadCommand read = commandForSubrange(command, r, consistencyLevel, nowInSeconds);
        Callable<Data> callable = new Callable<>()
        {
            @Override
            public Data call()
            {
                try (ReadExecutionController controller = read.executionController();
                     UnfilteredPartitionIterator partition = read.executeLocally(controller);
                     PartitionIterator iterator = UnfilteredPartitionIterators.filter(partition, read.nowInSec()))
                {
                    TxnData result = new TxnData();
                    TxnDataRangeValue value = new TxnDataRangeValue();
                    while (iterator.hasNext())
                    {
                        try (RowIterator rows = iterator.next())
                        {
                            FilteredPartition filtered = FilteredPartition.create(rows);
                            if (filtered.hasRows() || read.selectsFullPartition())
                            {
                                value.add(filtered);
                            }
                        }
                    }
                    result.put(TxnData.txnDataName(TxnDataNameKind.USER), value);
                    return result;
                }
            }

            @Override
            public String toString()
            {
                return command.toCQLString();
            }
        };
        return submit(executor, callable, callable);
    }

    private AsyncChain<Data> submit(AccordExecutor executor, Callable<Data> readCallable, Object describe)
    {
        return executor.buildDebuggable(readCallable, describe);
    }

    static final ParameterisedVersionedSerializer<TxnNamedRead, TableMetadatasAndKeys, Version> serializer = new ParameterisedVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnNamedRead read, TableMetadatasAndKeys tablesAndKeys, DataOutputPlus out, Version version) throws IOException
        {
            out.writeInt(read.name);
            tablesAndKeys.serializeSeekable(read.key, out);
            if (!read.isNull())
            {
                out.write(0);
                writeWithVIntLength(read.bytes(tablesAndKeys.tables, version), out);
            }
            else
            {
                out.write(1);
            }
        }

        @Override
        public TxnNamedRead deserialize(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
        {
            int name = in.readInt();
            Seekable key = tablesAndKeys.deserializeSeekable(in);
            ByteBuffer bytes = in.readByte() == 1 ? null : readWithVIntLength(in);
            if (version != Version.LATEST)
                bytes = serializeUnchecked(deserializeUnchecked(tablesAndKeys, bytes, version), tablesAndKeys, Version.LATEST);
            return new TxnNamedRead(name, key, bytes);
        }

        @Override
        public void skip(TableMetadatasAndKeys tablesAndKeys, DataInputPlus in, Version version) throws IOException
        {
            in.readInt();
            tablesAndKeys.skipSeekable(in);
            if (in.readByte() != 1) skipWithVIntLength(in);
        }

        @Override
        public long serializedSize(TxnNamedRead read, TableMetadatasAndKeys tablesAndKeys, Version version)
        {
            long size = 0;
            size += TypeSizes.sizeof(read.name);
            size += tablesAndKeys.serializedSeekableSize(read.key);
            size += TypeSizes.BYTE_SIZE; // is null
            if (!read.isNull())
                size += serializedSizeWithVIntLength(read.bytes(tablesAndKeys.tables, version));
            return size;
        }
    };
}
