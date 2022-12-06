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

package org.apache.cassandra.service.paxos;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.marshal.ByteBufferAccessor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TupleType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;

import static java.lang.Math.min;
import static org.apache.cassandra.service.paxos.Commit.isAfter;
import static org.apache.cassandra.service.paxos.Commit.latest;

public class PaxosRepairHistory
{
    public static final PaxosRepairHistory EMPTY = new PaxosRepairHistory(new Token[0], new Ballot[] { Ballot.none() });
    private static final Token.TokenFactory TOKEN_FACTORY = DatabaseDescriptor.getPartitioner().getTokenFactory();
    private static final Token MIN_TOKEN = DatabaseDescriptor.getPartitioner().getMinimumToken();
    private static final TupleType TYPE = new TupleType(ImmutableList.of(BytesType.instance, BytesType.instance));

    /**
     * The following two fields represent the mapping of ranges to ballot lower bounds, for example:
     *
     *   ballotLowBound           = [ none(), b2, none(), b4, none() ]
     *   tokenInclusiveUpperBound = [ t1, t2, t3, t4 ]
     *
     * Correspond to the following token bounds:
     *
     *   (MIN_VALUE, t1] => none()
     *   (t1, t2]        => b2
     *   (t2, t3]        => none()
     *   (t3, t4]        => b4
     *   (t4, MAX_VALUE) => none()
     */

    private final Token[] tokenInclusiveUpperBound;
    private final Ballot[] ballotLowBound; // always one longer to capture values up to "MAX_VALUE" (which in some cases doesn't exist, as is infinite)

    PaxosRepairHistory(Token[] tokenInclusiveUpperBound, Ballot[] ballotLowBound)
    {
        assert ballotLowBound.length == tokenInclusiveUpperBound.length + 1;
        this.tokenInclusiveUpperBound = tokenInclusiveUpperBound;
        this.ballotLowBound = ballotLowBound;
    }

    public Ballot maxLowBound()
    {
        Ballot maxBallot = Ballot.none();
        for (Ballot lowBound : ballotLowBound)
        {
            maxBallot = Commit.latest(maxBallot, lowBound);
        }
        return maxBallot;
    }

    public String toString()
    {
        return "PaxosRepairHistory{" +
                IntStream.range(0, ballotLowBound.length)
                        .filter(i -> !Ballot.none().equals(ballotLowBound[i]))
                        .mapToObj(i -> range(i) + "=" + ballotLowBound[i])
                        .collect(Collectors.joining(", ")) + '}';
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PaxosRepairHistory that = (PaxosRepairHistory) o;
        return Arrays.equals(ballotLowBound, that.ballotLowBound) && Arrays.equals(tokenInclusiveUpperBound, that.tokenInclusiveUpperBound);
    }

    public int hashCode()
    {
        return Arrays.hashCode(ballotLowBound);
    }

    public Ballot ballotForToken(Token token)
    {
        int idx = Arrays.binarySearch(tokenInclusiveUpperBound, token);
        if (idx < 0) idx = -1 - idx;
        return ballotLowBound[idx];
    }

    private Ballot ballotForIndex(int idx)
    {
        if (idx < 0 || idx > size())
            throw new IndexOutOfBoundsException();

        return ballotLowBound[idx];
    }

    private int indexForToken(Token token)
    {
        int idx = Arrays.binarySearch(tokenInclusiveUpperBound, token);
        if (idx < 0) idx = -1 - idx;
        return idx;
    }

    private boolean contains(int idx, Token token)
    {
        if (idx < 0 || idx > size())
            throw new IndexOutOfBoundsException();

        return  (idx == 0      || tokenInclusiveUpperBound[idx - 1].compareTo(token) <  0)
             && (idx == size() || tokenInclusiveUpperBound[idx    ].compareTo(token) >= 0);
    }

    public int size()
    {
        return tokenInclusiveUpperBound.length;
    }

    private RangeIterator rangeIterator()
    {
        return new RangeIterator();
    }

    private Range<Token> range(int i)
    {
        return new Range<>(tokenExclusiveLowerBound(i), tokenInclusiveUpperBound(i));
    }

    public Searcher searcher()
    {
        return new Searcher();
    }

    private Token tokenExclusiveLowerBound(int i)
    {
        return i == 0 ? MIN_TOKEN : tokenInclusiveUpperBound[i - 1];
    }

    private Token tokenInclusiveUpperBound(int i)
    {
        return i == tokenInclusiveUpperBound.length ? MIN_TOKEN : tokenInclusiveUpperBound[i];
    }

    public List<ByteBuffer> toTupleBufferList()
    {
        List<ByteBuffer> tuples = new ArrayList<>(size() + 1);
        for (int i = 0 ; i < 1 + size() ; ++i)
            tuples.add(TupleType.buildValue(new ByteBuffer[] { TOKEN_FACTORY.toByteArray(tokenInclusiveUpperBound(i)), ballotLowBound[i].toBytes() }));
        return tuples;
    }

    public static PaxosRepairHistory fromTupleBufferList(List<ByteBuffer> tuples)
    {
        Token[] tokenInclusiveUpperBounds = new Token[tuples.size() - 1];
        Ballot[] ballotLowBounds = new Ballot[tuples.size()];
        for (int i = 0 ; i < tuples.size() ; ++i)
        {
            ByteBuffer[] split = TYPE.split(ByteBufferAccessor.instance, tuples.get(i));
            if (i < tokenInclusiveUpperBounds.length)
                tokenInclusiveUpperBounds[i] = TOKEN_FACTORY.fromByteArray(split[0]);
            ballotLowBounds[i] = Ballot.deserialize(split[1]);
        }

        return new PaxosRepairHistory(tokenInclusiveUpperBounds, ballotLowBounds);
    }

    // append the item to the given list, modifying the underlying list
    // if the item makes previoud entries redundant

    public static PaxosRepairHistory merge(PaxosRepairHistory historyLeft, PaxosRepairHistory historyRight)
    {
        if (historyLeft == null)
            return historyRight;

        if (historyRight == null)
            return historyLeft;

        Builder builder = new Builder(historyLeft.size() + historyRight.size());

        RangeIterator left = historyLeft.rangeIterator();
        RangeIterator right = historyRight.rangeIterator();
        while (left.hasUpperBound() && right.hasUpperBound())
        {
            int cmp = left.tokenInclusiveUpperBound().compareTo(right.tokenInclusiveUpperBound());

            Ballot ballot = latest(left.ballotLowBound(), right.ballotLowBound());
            if (cmp == 0)
            {
                builder.append(left.tokenInclusiveUpperBound(), ballot);
                left.next();
                right.next();
            }
            else
            {
                RangeIterator firstIter = cmp < 0 ? left : right;
                builder.append(firstIter.tokenInclusiveUpperBound(), ballot);
                firstIter.next();
            }
        }

        while (left.hasUpperBound())
        {
            builder.append(left.tokenInclusiveUpperBound(), latest(left.ballotLowBound(), right.ballotLowBound()));
            left.next();
        }

        while (right.hasUpperBound())
        {
            builder.append(right.tokenInclusiveUpperBound(), latest(left.ballotLowBound(), right.ballotLowBound()));
            right.next();
        }

        builder.appendLast(latest(left.ballotLowBound(), right.ballotLowBound()));
        return builder.build();
    }

    @VisibleForTesting
    public static PaxosRepairHistory add(PaxosRepairHistory existing, Collection<Range<Token>> ranges, Ballot ballot)
    {
        ranges = Range.normalize(ranges);
        Builder builder = new Builder(ranges.size() * 2);
        for (Range<Token> range : ranges)
        {
            // don't add a point for an opening min token, since it
            // effectively leaves the bottom of the range unbounded
            builder.appendMaybeMin(range.left, Ballot.none());
            builder.appendMaybeMax(range.right, ballot);
        }

        return merge(existing, builder.build());
    }

    /**
     * returns a copy of this PaxosRepairHistory limited to the ranges supplied, with all other ranges reporting Ballot.none()
     */
    @VisibleForTesting
    static PaxosRepairHistory trim(PaxosRepairHistory existing, Collection<Range<Token>> ranges)
    {
        Builder builder = new Builder(existing.size());

        ranges = Range.normalize(ranges);
        for (Range<Token> select : ranges)
        {
            RangeIterator intersects = existing.intersects(select);
            while (intersects.hasNext())
            {
                if (Ballot.none().equals(intersects.ballotLowBound()))
                {
                    intersects.next();
                    continue;
                }

                Token exclusiveLowerBound = maxExclusiveLowerBound(select.left, intersects.tokenExclusiveLowerBound());
                Token inclusiveUpperBound = minInclusiveUpperBound(select.right, intersects.tokenInclusiveUpperBound());
                assert exclusiveLowerBound.compareTo(inclusiveUpperBound) < 0 || inclusiveUpperBound.isMinimum();

                builder.appendMaybeMin(exclusiveLowerBound, Ballot.none());
                builder.appendMaybeMax(inclusiveUpperBound, intersects.ballotLowBound());
                intersects.next();
            }
        }

        return builder.build();
    }

    RangeIterator intersects(Range<Token> unwrapped)
    {
        int from = Arrays.binarySearch(tokenInclusiveUpperBound, unwrapped.left);
        if (from < 0) from = -1 - from; else ++from;
        int to = unwrapped.right.isMinimum() ? ballotLowBound.length - 1 : Arrays.binarySearch(tokenInclusiveUpperBound, unwrapped.right);
        if (to < 0) to = -1 - to;
        return new RangeIterator(from, min(1 + to, ballotLowBound.length));
    }

    private static Token maxExclusiveLowerBound(Token a, Token b)
    {
        return a.compareTo(b) < 0 ? b : a;
    }

    private static Token minInclusiveUpperBound(Token a, Token b)
    {
        if (!a.isMinimum() && !b.isMinimum()) return a.compareTo(b) <= 0 ? a : b;
        else if (!a.isMinimum()) return a;
        else if (!b.isMinimum()) return b;
        else return a;
    }

    public static final IVersionedSerializer<PaxosRepairHistory> serializer = new IVersionedSerializer<PaxosRepairHistory>()
    {
        public void serialize(PaxosRepairHistory history, DataOutputPlus out, int version) throws IOException
        {
            out.writeUnsignedVInt32(history.size());
            for (int i = 0; i < history.size() ; ++i)
            {
                Token.serializer.serialize(history.tokenInclusiveUpperBound[i], out, version);
                history.ballotLowBound[i].serialize(out);
            }
            history.ballotLowBound[history.size()].serialize(out);
        }

        public PaxosRepairHistory deserialize(DataInputPlus in, int version) throws IOException
        {
            int size = in.readUnsignedVInt32();
            Token[] tokenInclusiveUpperBounds = new Token[size];
            Ballot[] ballotLowBounds = new Ballot[size + 1];
            for (int i = 0; i < size; i++)
            {
                tokenInclusiveUpperBounds[i] = Token.serializer.deserialize(in, DatabaseDescriptor.getPartitioner(), version);
                ballotLowBounds[i] = Ballot.deserialize(in);
            }
            ballotLowBounds[size] = Ballot.deserialize(in);
            return new PaxosRepairHistory(tokenInclusiveUpperBounds, ballotLowBounds);
        }

        public long serializedSize(PaxosRepairHistory history, int version)
        {
            long size = TypeSizes.sizeofUnsignedVInt(history.size());
            for (int i = 0; i < history.size() ; ++i)
            {
                size += Token.serializer.serializedSize(history.tokenInclusiveUpperBound[i], version);
                size += Ballot.sizeInBytes();
            }
            size += Ballot.sizeInBytes();
            return size;
        }
    };

    public class Searcher
    {
        int idx = -1;

        public Ballot ballotForToken(Token token)
        {
            if (idx < 0 || !contains(idx, token))
                idx = indexForToken(token);
            return ballotForIndex(idx);
        }
    }

    class RangeIterator
    {
        final int end;
        int i;

        RangeIterator()
        {
            this.end = ballotLowBound.length;
        }

        RangeIterator(int from, int to)
        {
            this.i = from;
            this.end = to;
        }

        boolean hasNext()
        {
            return i < end;
        }

        boolean hasUpperBound()
        {
            return i < tokenInclusiveUpperBound.length;
        }

        void next()
        {
            ++i;
        }

        Token tokenExclusiveLowerBound()
        {
            return PaxosRepairHistory.this.tokenExclusiveLowerBound(i);
        }

        Token tokenInclusiveUpperBound()
        {
            return PaxosRepairHistory.this.tokenInclusiveUpperBound(i);
        }

        Ballot ballotLowBound()
        {
            return ballotLowBound[i];
        }
    }

    static class Builder
    {
        final List<Token> tokenInclusiveUpperBounds;
        final List<Ballot> ballotLowBounds;

        Builder(int capacity)
        {
            this.tokenInclusiveUpperBounds = new ArrayList<>(capacity);
            this.ballotLowBounds = new ArrayList<>(capacity + 1);
        }

        void appendMaybeMin(Token inclusiveLowBound, Ballot ballotLowBound)
        {
            if (inclusiveLowBound.isMinimum())
                assert ballotLowBound.equals(Ballot.none()) && ballotLowBounds.isEmpty();
            else
                append(inclusiveLowBound, ballotLowBound);
        }

        void appendMaybeMax(Token inclusiveLowBound, Ballot ballotLowBound)
        {
            if (inclusiveLowBound.isMinimum())
                appendLast(ballotLowBound);
            else
                append(inclusiveLowBound, ballotLowBound);
        }

        void append(Token inclusiveLowBound, Ballot ballotLowBound)
        {
            int tailIdx = tokenInclusiveUpperBounds.size() - 1;

            assert tokenInclusiveUpperBounds.size() == ballotLowBounds.size();
            assert tailIdx < 0 || inclusiveLowBound.compareTo(tokenInclusiveUpperBounds.get(tailIdx)) >= 0;

            boolean sameAsTailToken = tailIdx >= 0 && inclusiveLowBound.equals(tokenInclusiveUpperBounds.get(tailIdx));
            boolean sameAsTailBallot = tailIdx >= 0 && ballotLowBound.equals(ballotLowBounds.get(tailIdx));
            if (sameAsTailToken || sameAsTailBallot)
            {
                if (sameAsTailBallot)
                    tokenInclusiveUpperBounds.set(tailIdx, inclusiveLowBound);
                else if (isAfter(ballotLowBound, ballotLowBounds.get(tailIdx)))
                    ballotLowBounds.set(tailIdx, ballotLowBound);
            }
            else
            {
                tokenInclusiveUpperBounds.add(inclusiveLowBound);
                ballotLowBounds.add(ballotLowBound);
            }
        }

        void appendLast(Ballot ballotLowBound)
        {
            assert ballotLowBounds.size() == tokenInclusiveUpperBounds.size();
            int tailIdx = tokenInclusiveUpperBounds.size() - 1;
            if (!ballotLowBounds.isEmpty() && ballotLowBound.equals(ballotLowBounds.get(tailIdx)))
                tokenInclusiveUpperBounds.remove(tailIdx);
            else
                ballotLowBounds.add(ballotLowBound);
        }

        PaxosRepairHistory build()
        {
            if (tokenInclusiveUpperBounds.size() == ballotLowBounds.size())
                ballotLowBounds.add(Ballot.none());
            return new PaxosRepairHistory(tokenInclusiveUpperBounds.toArray(new Token[0]), ballotLowBounds.toArray(new Ballot[0]));
        }
    }
}
