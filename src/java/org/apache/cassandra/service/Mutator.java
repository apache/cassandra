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

import javax.annotation.Nullable;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
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
     * Used for handling counter mutations on the coordinator level.
     */
    AbstractWriteResponseHandler<IMutation> mutateCounter(CounterMutation cm, String localDataCenter, long queryStartNanoTime);

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
     * Callback invoked when the given {@code mutation} is localy applied.
     */
    default void onAppliedMutation(IMutation mutation)
    {
        // no-op
    }

    /**
     * Callback invoked when the given {@code counter} is localy applied.
     */
    default void onAppliedCounter(IMutation counter)
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