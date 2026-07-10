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

import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.reads.ReadCoordinator;
import org.apache.cassandra.service.reads.SpeculativeRetryPolicy;
import org.apache.cassandra.tcm.ClusterMetadata;

/**
 * Ties together replica selection and response tracking for a single operation.
 *
 * This immutable container ensures that the replica plan (who to contact) and
 * the response tracker (how to determine success) are created atomically by the
 * replication strategy, consulting the same state. This is particularly important
 * for strategies that have per-range state (e.g., failover state in SRS) where
 * the replica selection and quorum requirements must be consistent.
 *
 * The separation between ReplicaPlan and ResponseTracker allows:
 * <ul>
 *   <li>ReplicaPlan to focus on replica topology and selection</li>
 *   <li>ResponseTracker to encapsulate completion logic</li>
 *   <li>Replication strategies to customize both consistently</li>
 * </ul>
 *
 * No polymorphism is needed at this level - the variation is captured in the
 * ResponseTracker implementations. This is just a typed tuple ensuring the
 * two pieces travel together.
 *
 * @param <P> the type of ReplicaPlan (ForRead, ForWrite, ForPaxosWrite, etc.)
 */
public abstract class CoordinationPlan<E extends Endpoints<E>, P extends ReplicaPlan<E, P>>
{
    // TODO (now): consolidate callback Condition instances into this. Replace all condition await calls with calls  to this.
    private final ResponseTracker responses;

    /**
     * Create a coordination plan.
     *
     * @param responses the response tracker for determining completion/success
     */
    public CoordinationPlan(ResponseTracker responses)
    {
        Preconditions.checkNotNull(responses);
        this.responses = responses;
    }

    public ConsistencyLevel consistencyLevel()
    {
        return replicas().consistencyLevel();
    }

    public AbstractReplicationStrategy replicationStrategy()
    {
        return replicas().replicationStrategy();
    }

    public abstract P replicas();

    /**
     * The response tracker for determining completion/success.
     *
     * The tracker encapsulates the logic for:
     * - Recording responses and failures
     * - Determining when the operation is complete
     * - Checking if the operation succeeded
     *
     * @return the response tracker
     */
    public ResponseTracker responses()
    {
        return responses;
    }

    @Override
    public String toString()
    {
        return String.format("CoordinationPlan[replicaPlan=%s, tracker=%s]", replicas(), responses.getClass().getSimpleName());
    }

    /**
     * Extended plan including source cluster metadata and ideal coordination plan.
     */
    public static class ForWrite extends CoordinationPlan<EndpointsForToken, ReplicaPlan.ForWrite>
    {
        private final ReplicaPlan.ForWrite replicas;

        public ForWrite(ReplicaPlan.ForWrite replicas, ResponseTracker responses)
        {
            super(responses);
            this.replicas = replicas;
        }

        @Override
        public ReplicaPlan.ForWrite replicas()
        {
            return replicas;
        }
    }

    public static class ForWriteWithIdeal extends CoordinationPlan.ForWrite
    {
        public final CoordinationPlan.ForWrite ideal;

        public ForWriteWithIdeal(ReplicaPlan.ForWrite replicas, ResponseTracker responses, CoordinationPlan.ForWrite ideal)
        {
            super(replicas, responses);
            this.ideal = ideal;
        }

        /**
         * Create coordination plan for batchlog write.
         *
         * The batchlog is a system-level durability mechanism independent of keyspace replication:
         * - Stored in system.batches regardless of which keyspace(s) the mutations target
         * - Replica selection is DC-local based on rack diversity and liveness
         * - Uses simple ack counting (ONE or TWO based on available replicas)
         *
         * @param metadata the cluster metadata
         * @param isAny whether to allow any node (for legacy batch compatibility)
         * @return coordination plan for batchlog write
         * @throws UnavailableException if insufficient replicas are available
         */
        public static ForWriteWithIdeal forBatchlogWrite(ClusterMetadata metadata, boolean isAny)
                throws UnavailableException
        {
            ReplicaPlan.ForWrite plan = ReplicaPlans.forBatchlogWrite(metadata, isAny);
            int blockFor = plan.consistencyLevel().blockFor(plan.replicationStrategy());
            ResponseTracker tracker = new SimpleResponseTracker(blockFor, plan.contacts().size());
            return new ForWriteWithIdeal(plan, tracker, null);
        }
    }

    public abstract static class ForRead<E extends Endpoints<E>, P extends ReplicaPlan<E, P>> extends CoordinationPlan<E, P> implements Supplier<P>
    {
        final ReplicaPlan.Shared<E, P> replicas;

        public ForRead(ReplicaPlan.Shared<E, P> replicas, ResponseTracker responses)
        {
            super(responses);
            this.replicas = replicas;
        }

        public abstract ForRead<E, P> copyWithResetTracker();

        @Override
        public P get()
        {
            return replicas.get();
        }

        @Override
        public P replicas()
        {
            return replicas.get();
        }

        public void addToContacts(Replica replica)
        {
            replicas.addToContacts(replica);
        }
    }

    public static class ForTokenRead extends ForRead<EndpointsForToken, ReplicaPlan.ForTokenRead>
    {
        public ForTokenRead(ReplicaPlan.Shared<EndpointsForToken, ReplicaPlan.ForTokenRead> replicas, ResponseTracker responses)
        {
            super(replicas, responses);
        }

        @Override
        public ForTokenRead copyWithResetTracker()
        {
            return new ForTokenRead(replicas, responses().resetCopy());
        }
    }

    public static class ForRangeRead extends ForRead<EndpointsForRange, ReplicaPlan.ForRangeRead>
    {
        public ForRangeRead(ReplicaPlan.Shared<EndpointsForRange, ReplicaPlan.ForRangeRead> replicas, ResponseTracker responses)
        {
            super(replicas, responses);
        }

        @Override
        public ForRangeRead copyWithResetTracker()
        {
            return new ForRangeRead(replicas, responses().resetCopy());
        }
    }

    // ---- Static convenience methods that look up the replication strategy internally ----

    private static AbstractReplicationStrategy getStrategy(ClusterMetadata metadata, Keyspace keyspace)
    {
        if (SchemaConstants.isLocalSystemKeyspace(keyspace.getName()))
            return keyspace.getReplicationStrategy();

        return metadata.schema.getKeyspaceMetadata(keyspace.getName()).replicationStrategy;
    }

    public static ForWriteWithIdeal forWrite(ClusterMetadata metadata,
                                             Keyspace keyspace,
                                             ConsistencyLevel consistencyLevel,
                                             Function<ClusterMetadata, ReplicaLayout.ForTokenWrite> liveAndDown,
                                             ReplicaPlans.Selector selector)
    {
        return getStrategy(metadata, keyspace).planForWrite(metadata, keyspace, consistencyLevel, liveAndDown, selector);
    }

    public static ForWriteWithIdeal forWrite(ClusterMetadata metadata,
                                             Keyspace keyspace,
                                             ConsistencyLevel consistencyLevel,
                                             Token token,
                                             ReplicaPlans.Selector selector)
    {
        return getStrategy(metadata, keyspace).planForWrite(metadata, keyspace, consistencyLevel, token, selector);
    }

    public static ForWrite forForwardingCounterWrite(ClusterMetadata metadata,
                                                     Keyspace keyspace,
                                                     Token token,
                                                     Function<ClusterMetadata, Replica> replicaSupplier)
    {
        return getStrategy(metadata, keyspace).planForForwardingCounterWrite(metadata, keyspace, token, replicaSupplier);
    }

    public static ForWriteWithIdeal forReplayMutation(ClusterMetadata metadata,
                                                      Keyspace keyspace,
                                                      Token token)
    {
        return getStrategy(metadata, keyspace).planForReplayMutation(metadata, keyspace, token);
    }

    public static ForTokenRead forTokenRead(ClusterMetadata metadata,
                                            Keyspace keyspace,
                                            TableId tableId,
                                            Token token,
                                            @Nullable Index.QueryPlan indexQueryPlan,
                                            ConsistencyLevel consistencyLevel,
                                            SpeculativeRetryPolicy retry,
                                            ReadCoordinator coordinator)
    {
        return getStrategy(metadata, keyspace).planForTokenRead(metadata, keyspace, tableId, token, indexQueryPlan, consistencyLevel, retry, coordinator);
    }

    public static ForRangeRead forRangeRead(ClusterMetadata metadata,
                                            Keyspace keyspace,
                                            TableId tableId,
                                            @Nullable Index.QueryPlan indexQueryPlan,
                                            ConsistencyLevel consistencyLevel,
                                            AbstractBounds<PartitionPosition> range,
                                            int vnodeCount)
    {
        return getStrategy(metadata, keyspace).planForRangeRead(metadata, keyspace, tableId, indexQueryPlan, consistencyLevel, range, vnodeCount);
    }

    public static ForRangeRead maybeMergeRangeReads(ClusterMetadata metadata,
                                                     Keyspace keyspace,
                                                     TableId tableId,
                                                     ConsistencyLevel consistencyLevel,
                                                     ForRangeRead left,
                                                     ForRangeRead right)
    {
        return getStrategy(metadata, keyspace).maybeMergeRangeReads(metadata, keyspace, tableId, consistencyLevel, left.replicas(), right.replicas());
    }

    public static ForRangeRead forFullRangeRead(Keyspace keyspace,
                                                ConsistencyLevel consistencyLevel,
                                                AbstractBounds<PartitionPosition> range,
                                                Set<InetAddressAndPort> endpointsToContact,
                                                int vnodeCount)
    {
        return keyspace.getReplicationStrategy().planForFullRangeRead(keyspace, consistencyLevel, range, endpointsToContact, vnodeCount);
    }

    public static ForTokenRead forSingleReplicaTokenRead(Keyspace keyspace, Token token, Replica replica)
    {
        return keyspace.getReplicationStrategy().planForSingleReplicaTokenRead(keyspace, token, replica);
    }

    public static ForRangeRead forSingleReplicaRangeRead(Keyspace keyspace,
                                                         AbstractBounds<PartitionPosition> range,
                                                         Replica replica,
                                                         int vnodeCount)
    {
        return keyspace.getReplicationStrategy().planForSingleReplicaRangeRead(keyspace, range, replica, vnodeCount);
    }
}
