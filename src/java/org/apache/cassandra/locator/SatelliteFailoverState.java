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
package org.apache.cassandra.locator;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Preconditions;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

/**
 * Failover state management for satellite datacenter replication
 *
 * Tracks per-range failover state to support different replica layouts / consistency requirements
 * during different failover states to maintain correctness
 *
 * NOTE: Initial implementation stores state in memory. Future work will integrate
 * with TCM for cluster-wide coordination and persistence.
 */
public class SatelliteFailoverState
{
    /**
     * Failover state for a range or keyspace.
     *
     * Each state corresponds to a specific phase in the failover process as defined
     * in CEP-58, with different consistency level requirements.
     */
    public enum State
    {
        /**
         * Normal operation: Primary DC + satellite active.
         * Read/Write CL: QUORUM_OF_QUORUMS (primary + satellite OR secondary)
         */
        NORMAL,

        /**
         * First stage of failover. Waits for a QoQ to acknowledge new state before moving to
         * TRANSITION state. During TRANSITION_ACK, coordinators will not start paxos operations, and the
         * primary transition state machine will not transition to the transition state until it confirms
         * that a QoQ nodes for a range are also on TRANSITION_ACK. This temporary gap in paxos availability
         * prevents the different full dcs from performing conflicting paxos operations concurrently.
         */
        TRANSITION_ACK,

        /**
         *
         * Currently syncing to new primary, once sync is complete, failover state will transition to NORMAL
         * for this range
         */
        TRANSITION,
    }

    /**
     * Failover state with DC context.
     *
     * Tracks which DC we're failing over from. The target DC is always the current
     * primary DC configured in the replication strategy, enabling failover between
     * any configured DCs via schema change (ALTER KEYSPACE ... WITH replication = {'primary': 'DC2'}).
     */
    public static class FailoverInfo
    {
        private final State state;
        private final String fromDC;  // DC we're failing over from (old primary), null for NORMAL

        /**
         * Create failover info.
         *
         * @param state The failover state
         * @param fromDC The old primary DC we're failing from (null for NORMAL state)
         */
        private FailoverInfo(State state, String fromDC)
        {
            this.state = state;
            this.fromDC = fromDC;

            switch (state)
            {
                case NORMAL:
                    Preconditions.checkArgument(fromDC == null);
                    break;
                case TRANSITION_ACK:
                case TRANSITION:
                    Preconditions.checkArgument(fromDC != null);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown state: " + state);
            }

        }

        /**
         * Get the failover state.
         */
        public State getState()
        {
            return state;
        }

        /**
         * Get the DC we're failing over FROM (old primary).
         * Returns null for NORMAL state.
         */
        public String getFromDC()
        {
            return fromDC;
        }

        /**
         * Create NORMAL state (no failover in progress).
         */
        public static FailoverInfo normal()
        {
            return new FailoverInfo(State.NORMAL, null);
        }

        /**
         * Create TRANSITION_ACK state.
         *
         * @param fromDC The old primary DC we're failing over from
         */
        public static FailoverInfo transitionAck(String fromDC)
        {
            return new FailoverInfo(State.TRANSITION_ACK, fromDC);
        }

        /**
         * Create TRANSITION state
         *
         * @param fromDC The old primary DC we're transferring from
         */
        public static FailoverInfo transition(String fromDC)
        {
            return new FailoverInfo(State.TRANSITION, fromDC);
        }

        @Override
        public String toString()
        {
            if (fromDC == null)
                return state.toString();
            return String.format("%s(from=%s)", state, fromDC);
        }

        public boolean isTransitioning()
        {
            switch (state)
            {
                case TRANSITION:
                case TRANSITION_ACK:
                    return true;
                default:
                    return false;
            }
        }
    }

    /**
     * Container for per-range failover state.
     *
     * Stores failover state indexed by token range, enabling different ranges
     * to be in different failover states (though initial implementation uses
     * uniform state across all ranges).
     *
     * NOTE: This is in-memory for initial implementation. Future work will
     * store this in TCM for persistence and cluster-wide coordination.
     */
    public static class FailoverStateMap
    {
        private final Map<Range<Token>, FailoverInfo> perRangeState;
        private final FailoverInfo defaultState;  // Used for ranges not explicitly set

        private FailoverStateMap(Map<Range<Token>, FailoverInfo> perRangeState,
                                FailoverInfo defaultState)
        {
            this.perRangeState = Collections.unmodifiableMap(new HashMap<>(perRangeState));
            this.defaultState = defaultState;
        }

        private FailoverStateMap(FailoverInfo globalState)
        {
            this.perRangeState = Collections.emptyMap();
            this.defaultState = globalState;
        }

        public FailoverInfo getFailoverInfo(Range<Token> range)
        {
            // FIXME: This is just meant for testing at the moment and assumes that we never query ranges that don't
            //  align with  the state map. Fix as part of the failover process implementation
            return getFailoverInfo(range.right);
        }

        public FailoverInfo getFailoverInfo(Token token)
        {
            for (Map.Entry<Range<Token>, FailoverInfo> entry : perRangeState.entrySet())
            {
                if (entry.getKey().contains(token))
                    return entry.getValue();
            }
            return defaultState;
        }

        public static class Builder
        {
            private final Map<Range<Token>, FailoverInfo> state = new HashMap<>();
            private FailoverInfo defaultState = FailoverInfo.normal();

            /**
             * Set the default state for ranges not explicitly set.
             */
            public Builder setDefault(FailoverInfo info)
            {
                this.defaultState = info;
                return this;
            }

            /**
             * Set failover state for a specific range.
             */
            public Builder setState(Range<Token> range, FailoverInfo info)
            {
                state.put(range, info);
                return this;
            }

            /**
             * Set failover state for multiple ranges.
             */
            public Builder setState(Collection<Range<Token>> ranges, FailoverInfo info)
            {
                ranges.forEach(r -> state.put(r, info));
                return this;
            }

            public FailoverStateMap build()
            {
                return new FailoverStateMap(state, defaultState);
            }
        }

        public static Builder builder()
        {
            return new Builder();
        }

        /**
         * Convenience factory: Create state map with single state for all ranges.
         */
        public static FailoverStateMap allRanges(FailoverInfo info)
        {
            return new FailoverStateMap(info);
        }
    }
}
