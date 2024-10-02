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
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.primitives.Range;
import accord.primitives.Seekable;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import accord.utils.async.AsyncResults;
import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.Stage;
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
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.accord.AccordObjectSizes;
import org.apache.cassandra.service.accord.TokenRange;
import org.apache.cassandra.service.accord.api.AccordRoutingKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.MinTokenKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.SentinelKey;
import org.apache.cassandra.service.accord.api.AccordRoutingKey.TokenKey;
import org.apache.cassandra.service.accord.api.PartitionKey;
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.utils.ByteBufferUtil.readWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.serializedSizeWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.writeWithVIntLength;

public class TxnNamedRead extends AbstractSerialized<ReadCommand>
{
    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(TxnNamedRead.class);

    private static final long EMPTY_SIZE = ObjectSizes.measure(new TxnNamedRead(0, null, (ByteBuffer) null));

    private final int name;
    private final Seekable key;

    public TxnNamedRead(int name, @Nullable SinglePartitionReadCommand value)
    {
        super(value);
        this.name = name;
        this.key = new PartitionKey(value.metadata().id, value.partitionKey());
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
        AccordRoutingKey startAccordRoutingKey;
        if (startToken.isMinimum() && inclusiveLeft)
            startAccordRoutingKey = SentinelKey.min(tableId);
        else if (inclusiveLeft || startIsMinKeyBound)
            startAccordRoutingKey = new MinTokenKey(tableId, startToken);
        else
            startAccordRoutingKey = new TokenKey(tableId, startToken);

        boolean inclusiveRight = range.inclusiveRight();
        PartitionPosition endPP = range.right;
        boolean endIsMinKeyBound = endPP.getClass() == KeyBound.class ? ((KeyBound)endPP).isMinimumBound : false;
        Token stopToken = range.right.getToken();
        AccordRoutingKey stopAccordRoutingKey;
        if (stopToken.isMinimum())
            stopAccordRoutingKey = SentinelKey.max(tableId);
        else if (inclusiveRight && !endIsMinKeyBound)
            stopAccordRoutingKey = new TokenKey(tableId, stopToken);
        else
            stopAccordRoutingKey = new MinTokenKey(tableId, stopToken);
        return TokenRange.create(startAccordRoutingKey, stopAccordRoutingKey);
    }

    public TxnNamedRead(int name, AbstractBounds<PartitionPosition> range, PartitionRangeReadCommand value)
    {
        super(value);
        TableId tableId = value.metadata().id;
        this.name = name;
        this.key = boundsAsAccordRange(range, tableId);
    }

    public TxnNamedRead(int name, Seekable key, ByteBuffer bytes)
    {
        super(bytes);
        this.name = name;
        this.key = key;
    }

    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        size += AccordObjectSizes.seekable(key);
        size += (bytes() != null ? ByteBufferUtil.estimatedSizeOnHeap(bytes()) : 0);
        return size;
    }

    @Override
    protected IVersionedSerializer<ReadCommand> serializer()
    {
        return ReadCommand.serializer;
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
        return "TxnNamedRead{name='" + name + '\'' + ", keys=" + key + ", update=" + get() + '}';
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

    public AsyncChain<Data> read(ConsistencyLevel consistencyLevel, Seekable key, Timestamp executeAt)
    {
        ReadCommand command = get();
        if (command == null)
            return AsyncResults.success(TxnData.NOOP_DATA);

        // TODO (required, safety): before release, double check reasoning that this is safe
//        AccordCommandsForKey cfk = ((SafeAccordCommandStore)safeStore).commandsForKey(key);
//        int nowInSeconds = cfk.nowInSecondsFor(executeAt, isForWriteTxn);
        // It's fine for our nowInSeconds to lag slightly our insertion timestamp, as to the user
        // this simply looks like the transaction witnessed TTL'd data and the data then expired
        // immediately after the transaction executed, and this simplifies things a great deal
        long nowInSeconds = nowInSeconds(executeAt);

        boolean withoutReconciliation = readsWithoutReconciliation(consistencyLevel);
        switch (key.domain())
        {
            case Key:
                return performLocalKeyRead(((SinglePartitionReadCommand) command).withTransactionalSettings(withoutReconciliation, nowInSeconds));
            case Range:
                return performLocalRangeRead(((PartitionRangeReadCommand) command), key.asRange(), consistencyLevel, nowInSeconds);
            default:
                throw new IllegalStateException("Unhandled domain " + key.domain());
        }
    }

    public static boolean readsWithoutReconciliation(ConsistencyLevel consistencyLevel)
    {
        boolean withoutReconciliation = consistencyLevel == null || consistencyLevel == ConsistencyLevel.ONE;
        return withoutReconciliation;
    }


    public ReadCommand command()
    {
        return get();
    }

    private AsyncChain<Data> performLocalKeyRead(SinglePartitionReadCommand read)
    {
        Callable<Data> readCallable = () ->
        {
            try (ReadExecutionController controller = read.executionController();
                 PartitionIterator iterator = UnfilteredPartitionIterators.filter(read.executeLocally(controller), read.nowInSec()))
            {
                TxnData result = new TxnData();
                if (iterator.hasNext())
                {
                    TxnDataKeyValue value = new TxnDataKeyValue(iterator.next());
                    if (value.hasRows() || read.selectsFullPartition())
                        result.put(name, value);
                }
                return result;
            }
        };

        return AsyncChains.ofCallable(Stage.READ.executor(), readCallable, (callable, receiver) ->
            new DebuggableTask.RunnableDebuggableTask()
            {
                private final long approxCreationTimeNanos = MonotonicClock.Global.approxTime.now();
                private volatile long approxStartTimeNanos;

                @Override
                public void run()
                {
                    approxStartTimeNanos = MonotonicClock.Global.approxTime.now();

                    try
                    {
                        Data call = callable.call();
                        receiver.accept(call, null);
                    }
                    catch (Throwable t)
                    {
                        logger.debug("AsyncChain Callable threw an Exception", t);
                        receiver.accept(null, t);
                    }
                }

                @Override
                public long creationTimeNanos()
                {
                    return approxCreationTimeNanos;
                }

                @Override
                public long startTimeNanos()
                {
                    return approxStartTimeNanos;
                }

                @Override
                public String description()
                {
                    return read.toCQLString();
                }
            }
        );
    }

    public static PartitionRangeReadCommand commandForSubrange(PartitionRangeReadCommand command, Range r, ConsistencyLevel consistencyLevel, long nowInSeconds)
    {
        AbstractBounds<PartitionPosition> bounds = command.dataRange().keyRange();
        PartitionPosition startPP = bounds.left;
        PartitionPosition endPP = bounds.right;
        TokenKey startTokenKey = new TokenKey(command.metadata().id, startPP.getToken());
        AccordRoutingKey startRoutingKey = ((AccordRoutingKey)r.start());
        AccordRoutingKey endRoutingKey = ((AccordRoutingKey)r.end());
        Token subRangeStartToken = startRoutingKey.getClass() == SentinelKey.class ? startPP.getToken() : ((AccordRoutingKey)r.start()).asTokenKey().token();
        Token subRangeEndToken = endRoutingKey.getClass() == SentinelKey.class ? endPP.getToken() : ((AccordRoutingKey)r.end()).asTokenKey().token();

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
        return command.withTransactionalSettings(nowInSeconds, subRange, startTokenKey.equals(r.start()), readsWithoutReconciliation(consistencyLevel));
    }

    private AsyncChain<Data> performLocalRangeRead(PartitionRangeReadCommand command, Range r, ConsistencyLevel consistencyLevel, long nowInSeconds)
    {
        PartitionRangeReadCommand read = commandForSubrange(command, r, consistencyLevel, nowInSeconds);
        Callable<Data> readCallable = () ->
        {
            try (ReadExecutionController controller = read.executionController();
                 UnfilteredPartitionIterator partition = read.executeLocally(controller);
                 PartitionIterator iterator = UnfilteredPartitionIterators.filter(partition, read.nowInSec()))
            {
                TxnData result = new TxnData();
                TxnDataRangeValue value = new TxnDataRangeValue();
                while(iterator.hasNext())
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
        };

        return AsyncChains.ofCallable(Stage.READ.executor(), readCallable, (callable, receiver) ->
                                                                           new DebuggableTask.RunnableDebuggableTask()
                                                                           {
                                                                               private final long approxCreationTimeNanos = MonotonicClock.Global.approxTime.now();
                                                                               private volatile long approxStartTimeNanos;

                                                                               @Override
                                                                               public void run()
                                                                               {
                                                                                   approxStartTimeNanos = MonotonicClock.Global.approxTime.now();

                                                                                   try
                                                                                   {
                                                                                       Data call = callable.call();
                                                                                       receiver.accept(call, null);
                                                                                   }
                                                                                   catch (Throwable t)
                                                                                   {
                                                                                       logger.debug("AsyncChain Callable threw an Exception", t);
                                                                                       receiver.accept(null, t);
                                                                                   }
                                                                               }

                                                                               @Override
                                                                               public long creationTimeNanos()
                                                                               {
                                                                                   return approxCreationTimeNanos;
                                                                               }

                                                                               @Override
                                                                               public long startTimeNanos()
                                                                               {
                                                                                   return approxStartTimeNanos;
                                                                               }

                                                                               @Override
                                                                               public String description()
                                                                               {
                                                                                   return command.toCQLString();
                                                                               }
                                                                           }
        );
    }

    static final IVersionedSerializer<TxnNamedRead> serializer = new IVersionedSerializer<>()
    {
        @Override
        public void serialize(TxnNamedRead read, DataOutputPlus out, int version) throws IOException
        {
            out.writeInt(read.name);
            KeySerializers.seekable.serialize(read.key, out, version);

            if (read.bytes() != null)
            {
                out.write(0);
                writeWithVIntLength(read.bytes(), out);
            }
            else
            {
                out.write(1);
            }
        }

        @Override
        public TxnNamedRead deserialize(DataInputPlus in, int version) throws IOException
        {
            int name = in.readInt();
            Seekable key = KeySerializers.seekable.deserialize(in, version);
            ByteBuffer bytes = in.readByte() == 1 ? null : readWithVIntLength(in);
            return new TxnNamedRead(name, key, bytes);
        }

        @Override
        public long serializedSize(TxnNamedRead read, int version)
        {
            long size = 0;
            size += TypeSizes.sizeof(read.name);
            size += KeySerializers.seekable.serializedSize(read.key, version);
            size += TypeSizes.BYTE_SIZE; // is null
            if (read.bytes() != null)
                size += serializedSizeWithVIntLength(read.bytes());
            return size;
        }
    };
}
