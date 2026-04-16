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
package org.apache.cassandra.locator.satellites;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.apache.cassandra.dht.NormalizedRanges;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.dht.TokenRangeMap;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

import static org.apache.cassandra.locator.satellites.SatelliteFailover.State;

/**
 * Per-keyspace satellite failover state
 *
 * Tracks which ranges are in which failover state during a primary DC transfer.
 * Uses a {@link TokenRangeMap} to map every point on the token ring to a {@link State}.
 *
 * All instances are immutable. State mutations return new instances.
 */
public class KeyspaceFailoverState implements SatelliteFailover.Info
{
    @Nonnull
    public final String fromDC;

    @Nonnull
    public final Epoch processStarted;

    @Nonnull
    private final TokenRangeMap<State> rangeStates;

    public KeyspaceFailoverState(@Nonnull String fromDC,
                                 @Nonnull Epoch processStarted,
                                 @Nonnull TokenRangeMap<State> rangeStates)
    {
        this.fromDC = Objects.requireNonNull(fromDC, "fromDC");
        this.processStarted = Objects.requireNonNull(processStarted, "processStarted");
        this.rangeStates = Objects.requireNonNull(rangeStates, "rangeStates");
    }

    public static KeyspaceFailoverState create(@Nonnull String fromDC,
                                               @Nonnull Epoch processStarted,
                                               @Nonnull NormalizedRanges<Token> failoverRanges)
    {
        TokenRangeMap<State> states = TokenRangeMap.<State>create(State.NORMAL)
                                                   .set(failoverRanges, State.TRANSITION_ACK);
        return new KeyspaceFailoverState(fromDC, processStarted, states);
    }

    public KeyspaceFailoverState withRangesTransitioning(NormalizedRanges<Token> ranges)
    {
        return withRangesAdvancedTo(ranges, State.TRANSITION);
    }

    public KeyspaceFailoverState withRangesNormal(NormalizedRanges<Token> ranges)
    {
        return withRangesAdvancedTo(ranges, State.NORMAL);
    }

    /**
     * Advance the requested ranges to {@code target}, monotonically.
     *
     * Only the sub-ranges whose current state is strictly behind {@code target} (per
     * {@link State#failoverProgress()}) are updated; sub-ranges already at or past {@code target} are left
     * unchanged. This keeps the transformation idempotent and prevents a stale commit — e.g. a lagging replica
     * node driving the same range from an older metadata snapshot — from regressing a range that another node has
     * already moved forward (a range must never move NORMAL -> TRANSITION). Returns {@code this} unchanged when
     * there is nothing to advance.
     */
    private KeyspaceFailoverState withRangesAdvancedTo(NormalizedRanges<Token> ranges, State target)
    {
        List<Range<Token>> toAdvance = new ArrayList<>();
        rangeStates.forEach((left, right, state) -> {
            if (state.failoverProgress() < target.failoverProgress())
            {
                Range<Token> interval = new Range<>(left, right);
                for (Range<Token> requested : ranges)
                    toAdvance.addAll(interval.intersectionWith(requested));
            }
        });

        if (toAdvance.isEmpty())
            return this;

        TokenRangeMap<State> updated = rangeStates.set(NormalizedRanges.normalizedRanges(toAdvance), target);
        return new KeyspaceFailoverState(fromDC, processStarted, updated);
    }

    public boolean isComplete()
    {
        return rangeStates.allEqual(State.NORMAL);
    }

    public boolean hasRangesInState(State state)
    {
        return !rangeStates.allMatch(s -> s != state);
    }

    public void forEachRange(BiConsumer<Range<Token>, State> consumer)
    {
        rangeStates.forEach((left, right, state) -> {
            if (state != State.NORMAL)
                consumer.accept(new Range<>(left, right), state);
        });
    }

    @Override
    public String getFromDC()
    {
        return fromDC;
    }

    @Override
    public State stateForToken(Token token)
    {
        return rangeStates.get(token);
    }

    @Override
    public State leastAdvancedState(Range<Token> range)
    {
        State[] least = new State[1];
        rangeStates.forEach((left, right, state) -> {
            if (!new Range<>(left, right).intersects(range))
                return;
            if (least[0] == null || state.failoverProgress() < least[0].failoverProgress())
                least[0] = state;
        });
        // rangeStates covers the whole ring, so any range intersects at least one interval
        Preconditions.checkState(least[0] != null);
        return least[0];
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyspaceFailoverState that = (KeyspaceFailoverState) o;
        return fromDC.equals(that.fromDC)
               && processStarted.equals(that.processStarted)
               && rangeStates.equals(that.rangeStates);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fromDC, processStarted, rangeStates);
    }

    @Override
    public String toString()
    {
        return String.format("KeyspaceFailoverState{fromDC=%s, processStarted=%s, rangeStates=%s}",
                             fromDC, processStarted, rangeStates);
    }

    private static final MetadataSerializer<TokenRangeMap<State>> rangeStatesSerializer = TokenRangeMap.metadataSerializer(State.metadataSerializer);

    public static final MetadataSerializer<KeyspaceFailoverState> serializer = new MetadataSerializer<>()
    {
        @Override
        public void serialize(KeyspaceFailoverState state, DataOutputPlus out, Version version) throws IOException
        {
            out.writeUTF(state.fromDC);
            Epoch.serializer.serialize(state.processStarted, out, version);
            rangeStatesSerializer.serialize(state.rangeStates, out, version);
        }

        @Override
        public KeyspaceFailoverState deserialize(DataInputPlus in, Version version) throws IOException
        {
            String fromDC = in.readUTF();
            Epoch processStarted = Epoch.serializer.deserialize(in, version);
            TokenRangeMap<State> rangeStates = rangeStatesSerializer.deserialize(in, version);
            return new KeyspaceFailoverState(fromDC, processStarted, rangeStates);
        }

        @Override
        public long serializedSize(KeyspaceFailoverState state, Version version)
        {
            return org.apache.cassandra.db.TypeSizes.sizeof(state.fromDC)
                   + Epoch.serializer.serializedSize(state.processStarted, version)
                   + rangeStatesSerializer.serializedSize(state.rangeStates, version);
        }
    };
}
