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

import org.junit.Test;

import junit.framework.TestCase;
import org.apache.cassandra.config.CassandraRelevantProperties;
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

public class MutatorProviderTest extends TestCase
{
    public static class TestMutator implements Mutator
    {
        @Override
        public AbstractWriteResponseHandler<IMutation> mutateCounter(CounterMutation cm, String localDataCenter, long queryStartNanoTime)
        {
            return null;
        }

        @Override
        public AbstractWriteResponseHandler<IMutation> mutateCounterOnLeader(CounterMutation mutation,
                                                                             String localDataCenter,
                                                                             StorageProxy.WritePerformer performer,
                                                                             Runnable callback,
                                                                             long queryStartNanoTime)
        {
            return null;
        }

        @Override
        public AbstractWriteResponseHandler<IMutation> mutateStandard(Mutation mutation, ConsistencyLevel consistencyLevel, String localDataCenter, StorageProxy.WritePerformer writePerformer, Runnable callback, WriteType writeType, long queryStartNanoTime)
        {
            return null;
        }

        @Nullable
        @Override
        public AbstractWriteResponseHandler<Commit> mutatePaxos(Commit proposal, ConsistencyLevel consistencyLevel, boolean allowHints, long queryStartNanoTime)
        {
            return null;
        }

        @Override
        public void mutateAtomically(Collection<Mutation> mutations, ConsistencyLevel consistencyLevel, boolean requireQuorumForRemove, long queryStartNanoTime, ClientRequestsMetrics metrics, ClientState clientState) throws UnavailableException, OverloadedException, WriteTimeoutException
        {
            // no-op
        }

        @Override
        public void persistBatchlog(Collection<Mutation> mutations, long queryStartNanoTime, ReplicaPlan.ForTokenWrite replicaPlan, UUID batchUUID)
        {
            // no-op
        }

        @Override
        public void clearBatchlog(String keyspace, ReplicaPlan.ForTokenWrite replicaPlan, UUID batchUUID)
        {
            // no-op
        }
    }

    @Test
    public void testInstantinatingCustomMutator()
    {
        CassandraRelevantProperties.CUSTOM_MUTATOR_CLASS.setString("org.apache.cassandra.service.MutatorProviderTest$TestMutator");
        Mutator mutator = MutatorProvider.getCustomOrDefault();
        assertSame(mutator.getClass(), TestMutator.class);
        System.clearProperty(CassandraRelevantProperties.CUSTOM_MUTATOR_CLASS.getKey());
    }
}