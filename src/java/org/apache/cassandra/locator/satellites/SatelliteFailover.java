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

import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.serialization.MetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * Failover state types for satellite datacenter replication.
 *
 * Defines the per-range failover state enum and FailoverInfo value type used by
 * SatelliteReplicationStrategy to determine replica layouts and consistency requirements
 * during failover transitions.
 *
 * The actual per-range state is persisted in TCM via {@code SatelliteFailoverProcessState}
 * and {@code KeyspaceFailoverState}. This class provides the query-path value types.
 */
public class SatelliteFailover
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
         * First step of failover. The entire ring is put into this state as part of the primary dc schema changes.
         * Before a range exits the TRANSITION_ACK state, a QoQ of nodes in the old query group must report an
         * epoch >= the epoch that changed the primary DC. If the old primary has not been disabled, paxos repair
         * must also be completed on a quorum of replicas in the old DC as well.
         * During TRANSITION_ACK, coordinators will not start paxos operations. This temporary gap in paxos availability
         * prevents the different full dcs from performing conflicting paxos operations concurrently. The paxos repair
         * step prevents operations committed locally in the old primary from being asynchronously replicated after the
         * new primary has already begun processing paxos operations.
         */
        TRANSITION_ACK,

        /**
         * Second step of failover. Before a range exits the TRANSITION state, data on the satellites must have been
         * reconciled to the new primary datacenter. During the TRANSITION phase, reads will block on QoQ from both the
         * current and previous primary DC query groups
         */
        TRANSITION;

        public boolean isTransitioning()
        {
            switch (this)
            {
                case TRANSITION:
                case TRANSITION_ACK:
                    return true;
                default:
                    return false;
            }
        }

        /**
         * Monotonic ordering of a range's failover lifecycle: {@code TRANSITION_ACK -> TRANSITION -> NORMAL}.
         *
         * Note this deliberately differs from {@link #ordinal()} (which is fixed by the declaration order the
         * serializer depends on). A range may only ever move forward through these ranks; state advancement is
         * applied monotonically so that a stale commit from a lagging driver can never regress a range that a
         * concurrent driver has already advanced.
         */
        public int failoverProgress()
        {
            switch (this)
            {
                case TRANSITION_ACK:
                    return 0;
                case TRANSITION:
                    return 1;
                case NORMAL:
                    return 2;
                default:
                    throw new IllegalStateException("Unhandled failover state: " + this);
            }
        }

        static final MetadataSerializer<State> metadataSerializer = new MetadataSerializer<>()
        {
            @Override
            public void serialize(State state, DataOutputPlus out, Version version) throws IOException
            {
                out.writeUnsignedVInt32(state.ordinal());
            }

            @Override
            public State deserialize(DataInputPlus in, Version version) throws IOException
            {
                return State.values()[in.readUnsignedVInt32()];
            }

            @Override
            public long serializedSize(State state, Version version)
            {
                return TypeSizes.sizeofUnsignedVInt(state.ordinal());
            }
        };
    }

    /**
     * The current failover state for a keyspace. Exposes methods to users for finding
     * the DC we're failing over from as well the failover state of specific tokens
     */
    public interface Info
    {

        Info NORMAL = new Info()
        {
            @Override
            public State stateForToken(Token token)
            {
                return State.NORMAL;
            }

            @Override
            public State leastAdvancedState(Range<Token> range)
            {
                return State.NORMAL;
            }

            @Override
            public String getFromDC()
            {
                return null;
            }
        };

        State stateForToken(Token token);

        /**
         * The least advanced state, per {@link State#failoverProgress()}, of any sub-range of {@code range}.
         *
         * A range may be only partially advanced: a concurrent driver on another replica node can move some of
         * its sub-ranges forward while we're working, so no single token is representative of the whole range.
         * Callers staging failover work need to reason about the whole range, not a single point in it.
         */
        State leastAdvancedState(Range<Token> range);

        default State stateForPartitionPosition(PartitionPosition position)
        {
            return stateForToken(position.getToken());
        }

        /**
         * Get the DC we're failing over from (old primary).
         * Returns null for NORMAL state.
         */
        String getFromDC();
    }

}
