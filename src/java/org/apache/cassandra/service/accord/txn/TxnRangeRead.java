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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import accord.api.Data;
import accord.api.DataStore;
import accord.api.Read;
import accord.local.SafeCommandStore;
import accord.primitives.AbstractRanges.UnionMode;
import accord.primitives.Participants;
import accord.primitives.Range;
import accord.primitives.Ranges;
import accord.primitives.Routable.Domain;
import accord.primitives.Routables.Slice;
import accord.primitives.Seekable;
import accord.primitives.Seekables;
import accord.primitives.Timestamp;
import accord.utils.async.AsyncChain;
import accord.utils.async.AsyncChains;
import org.apache.cassandra.concurrent.DebuggableTask;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadExecutionController;
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
import org.apache.cassandra.service.accord.serializers.KeySerializers;
import org.apache.cassandra.service.accord.txn.TxnData.TxnDataNameKind;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MonotonicClock;
import org.apache.cassandra.utils.ObjectSizes;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.cassandra.service.accord.AccordSerializers.consistencyLevelSerializer;
import static org.apache.cassandra.service.accord.IAccordService.SUPPORTED_READ_CONSISTENCY_LEVELS;
import static org.apache.cassandra.utils.ByteBufferUtil.readWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.serializedSizeWithVIntLength;
import static org.apache.cassandra.utils.ByteBufferUtil.writeWithVIntLength;
import static org.apache.cassandra.utils.NullableSerializer.deserializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializeNullable;
import static org.apache.cassandra.utils.NullableSerializer.serializedNullableSize;

public class TxnRangeRead extends AbstractSerialized<ReadCommand> implements TxnRead
{
    private static final Logger logger = LoggerFactory.getLogger(TxnRangeRead.class);

    public static final TxnRangeRead EMPTY = new TxnRangeRead(null, null, (Ranges)null);
    private static final long EMPTY_SIZE = ObjectSizes.measure(EMPTY);

    @Nonnull
    private final ConsistencyLevel cassandraConsistencyLevel;
    @Nonnull
    private final Ranges covering;

    public TxnRangeRead(@Nonnull PartitionRangeReadCommand command, @Nonnull List<AbstractBounds<PartitionPosition>> ranges, @Nonnull ConsistencyLevel cassandraConsistencyLevel)
    {
        super(command);
        checkArgument(cassandraConsistencyLevel == null || SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cassandraConsistencyLevel), "Unsupported consistency level for read");
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
        TableId tableId = command.metadata().id;

        TokenRange[] accordRanges = new TokenRange[ranges.size()];
        for (int i = 0; i < ranges.size(); i++)
        {
            AbstractBounds<PartitionPosition> range = ranges.get(i);
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
            accordRanges[i] = TokenRange.create(startAccordRoutingKey, stopAccordRoutingKey);
        }
        covering = Ranges.of(accordRanges);
    }

    private TxnRangeRead(@Nonnull ByteBuffer commandBytes, @Nonnull ConsistencyLevel cassandraConsistencyLevel, @Nonnull Ranges covering)
    {
        super(commandBytes);
        checkArgument(cassandraConsistencyLevel == null || SUPPORTED_READ_CONSISTENCY_LEVELS.contains(cassandraConsistencyLevel), "Unsupported consistency level for read");
        this.cassandraConsistencyLevel = cassandraConsistencyLevel;
        this.covering = covering;
    }

    @Override
    public Seekables<?, ?> keys()
    {
        return covering;
    }

    @Override
    public AsyncChain<Data> read(Seekable range, SafeCommandStore commandStore, Timestamp executeAt, DataStore store)
    {
        // Set to null since we don't need it and interop can pass in null
        commandStore = null;

        // It's fine for our nowInSeconds to lag slightly our insertion timestamp, as to the user
        // this simply looks like the transaction witnessed TTL'd data and the data then expired
        // immediately after the transaction executed, and this simplifies things a great deal
        long nowInSeconds = TimeUnit.MICROSECONDS.toSeconds(executeAt.hlc());

        List<AsyncChain<Data>> results = new ArrayList<>();
        PartitionRangeReadCommand command = (PartitionRangeReadCommand)get();
        if (cassandraConsistencyLevel == null || cassandraConsistencyLevel == ConsistencyLevel.ONE)
            command = command.withoutReconciliation();

        Ranges intersecting = covering.slice(Ranges.of(range.asRange()), Slice.Minimal);
        for (Range subRange : intersecting)
            results.add(performLocalRead(command, subRange, nowInSeconds));
        if (results.isEmpty())
            // Result type must match everywhere
            return AsyncChains.success(new TxnData());

        return AsyncChains.reduce(results, Data::merge);
    }

    public PartitionRangeReadCommand commandForSubrange(Range r, long nowInSeconds)
    {
        return commandForSubrange((PartitionRangeReadCommand) get(), r, nowInSeconds);
    }

    public PartitionRangeReadCommand commandForSubrange(PartitionRangeReadCommand command, Range r, long nowInSeconds)
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
        return command.forSubRangeWithNowInSeconds(nowInSeconds, subRange, startTokenKey.equals(r.start()));
    }

    private AsyncChain<Data> performLocalRead(PartitionRangeReadCommand command, Range r, long nowInSeconds)
    {
        Callable<Data> readCallable = () ->
        {
            PartitionRangeReadCommand read = commandForSubrange(command, r, nowInSeconds);

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

    @Override
    public Read slice(Ranges ranges)
    {
        return new TxnRangeRead(bytes(), cassandraConsistencyLevel, covering.slice(ranges, Slice.Minimal));
    }

    @Override
    public Read intersecting(Participants<?> participants)
    {
        checkArgument(participants.domain() == Domain.Range, "Can only intersect with ranges");
        return new TxnRangeRead(bytes(), cassandraConsistencyLevel, covering.intersecting(participants, Slice.Minimal));
    }

    @Override
    public Read merge(Read other)
    {
        Ranges newCovering = ((TxnRangeRead)other).covering.union(UnionMode.MERGE_OVERLAPPING, covering);
        return new TxnRangeRead(bytes(), cassandraConsistencyLevel, newCovering);
    }

    @Override
    public Kind kind()
    {
        return Kind.range;
    }

    @Override
    public long estimatedSizeOnHeap()
    {
        long size = EMPTY_SIZE;
        size += ByteBufferUtil.estimatedSizeOnHeap(bytes());
        size += AccordObjectSizes.ranges(covering);
        return size;
    }

    public PartitionRangeReadCommand command()
    {
        return (PartitionRangeReadCommand) get();
    }

    public static final TxnReadSerializer<TxnRangeRead> serializer = new TxnReadSerializer<TxnRangeRead>()
    {
        @Override
        public void serialize(TxnRangeRead read, DataOutputPlus out, int version) throws IOException
        {
            serializeNullable(read.cassandraConsistencyLevel, out, version, consistencyLevelSerializer);
            KeySerializers.ranges.serialize(read.covering, out, version);
            writeWithVIntLength(read.bytes(), out);
        }

        @Override
        public TxnRangeRead deserialize(DataInputPlus in, int version) throws IOException
        {
            ConsistencyLevel consistencyLevel = deserializeNullable(in, version, consistencyLevelSerializer);
            Ranges covering = KeySerializers.ranges.deserialize(in, version);
            ByteBuffer commandBytes = readWithVIntLength(in);
            return new TxnRangeRead(commandBytes, consistencyLevel, covering);
        }

        @Override
        public long serializedSize(TxnRangeRead read, int version)
        {
            long size = 0;
            size += KeySerializers.ranges.serializedSize(read.covering, version);
            size += serializedNullableSize(read.cassandraConsistencyLevel, version, consistencyLevelSerializer);
            size += serializedSizeWithVIntLength(read.bytes());
            return size;
        }
    };

    @Override
    protected IVersionedSerializer<ReadCommand> serializer()
    {
        return PartitionRangeReadCommand.serializer;
    }

    @Override
    public ConsistencyLevel cassandraConsistencyLevel()
    {
        return cassandraConsistencyLevel;
    }
}
