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

package org.apache.cassandra.service;

import java.util.Collection;
import java.util.UUID;
import javax.annotation.Nullable;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.locator.ReplicaPlan;
import org.apache.cassandra.metrics.ClientRequestsMetrics;
import org.apache.cassandra.service.paxos.Commit;

/**
 * Facilitates mutations for counters, simple inserts, unlogged batches and LWTs.
 * Used on the coordinator.
 * <br/>
 * The implementations may choose how and where to send the mutations.
 * <br/>
 * An instance of this interface implementation must be obtained via {@link MutatorProvider#instance}.
 */
public interface Mutator
{

    /**
     * Used for handling the given {@code mutations} as a logged batch.
     */
    void mutateAtomically(Collection<Mutation> mutations,
                          ConsistencyLevel consistencyLevel,
                          boolean requireQuorumForRemove,
                          long queryStartNanoTime,
                          ClientRequestsMetrics metrics,
                          ClientState clientState)
    throws UnavailableException, OverloadedException, WriteTimeoutException;

    /**
     * Used for handling counter mutations on the coordinator level:
     * - if coordinator is a replica, it will apply the counter mutation locally and forward the applied mutation to other counter replica
     * - if coordinator is not a replica, it will forward the counter mutation to a counter leader which is a replica
     */
    AbstractWriteResponseHandler<IMutation> mutateCounter(CounterMutation cm, String localDataCenter, long queryStartNanoTime);

    /**
     * Used for handling counter mutations on the counter leader level
     */
    AbstractWriteResponseHandler<IMutation> mutateCounterOnLeader(CounterMutation mutation,
                                                                  String localDataCenter,
                                                                  StorageProxy.WritePerformer performer,
                                                                  Runnable callback,
                                                                  long queryStartNanoTime);

    /**
     * Used for standard inserts and unlogged batchs.
     */
    AbstractWriteResponseHandler<IMutation> mutateStandard(Mutation mutation,
                                                           ConsistencyLevel consistencyLevel,
                                                           String localDataCenter,
                                                           StorageProxy.WritePerformer writePerformer,
                                                           Runnable callback,
                                                           WriteType writeType,
                                                           long queryStartNanoTime);

    /**
     * Used for LWT mutation at the last (COMMIT) phase of Paxos.
     */
    @Nullable
    AbstractWriteResponseHandler<Commit> mutatePaxos(Commit proposal, ConsistencyLevel consistencyLevel, boolean allowHints, long queryStartNanoTime);

    /**
     * Used to persist the given batch of mutations. Usually invoked as part of
     * {@link #mutateAtomically(Collection, ConsistencyLevel, boolean, long, ClientRequestsMetrics, ClientState)}.
     */
    void persistBatchlog(Collection<Mutation> mutations, long queryStartNanoTime, ReplicaPlan.ForTokenWrite replicaPlan, UUID batchUUID);

    /**
     * Used to clear the given batch id. Usually invoked as part of
     * {@link #mutateAtomically(Collection, ConsistencyLevel, boolean, long, ClientRequestsMetrics, ClientState)}.
     */
    void clearBatchlog(String keyspace, ReplicaPlan.ForTokenWrite replicaPlan, UUID batchUUID);

    /**
     * Callback invoked when the given {@code mutation} is localy applied.
     */
    default void onAppliedMutation(IMutation mutation)
    {
        // no-op
    }

    /**
     * Callback invoked when the given {@code counter} is localy applied.
     */
    default void onAppliedCounter(IMutation counter, AbstractWriteResponseHandler<IMutation> handler)
    {
        // no-op
    }

    /**
     * Callback invoked when the given {@code proposal} is localy committed.
     */
    default void onAppliedProposal(Commit proposal)
    {
        // no-op
    }
}