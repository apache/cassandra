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
package org.apache.cassandra.replication;

import org.agrona.collections.IntHashSet;
import org.jctools.maps.NonBlockingHashMapLong;

import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Keep track of outgoing mutations so that we don't double-send any mutations
 * when read reconciliation writes race with regular writes and/or each other.
 * </p>
 * Regular writes have a callback and need to be retried;
 * Reconciliation writes have no callback, and only need to be retried if the writing node owns the log.
 * </p>
 * Note: this could live inside {@link CoordinatorLog}, since we cannot ever send a mutation
 * that we don't have an instance of {@link CoordinatorLog} for, but for symmetry with
 * {@link IncomingMutations} we put this inside {@link MutationTrackingService}.
 */
class OutgoingMutations
{
    private final NonBlockingHashMapLong<LogOutgoingMutations> log2MutationsMap = new NonBlockingHashMapLong<>();

    void sentWriteRequest(Mutation mutation, IntHashSet toHostIds)
    {
        getOrCreate(mutation.id()).sentWriteRequest(mutation, toHostIds);
    }

    void receivedWriteResponse(ShortMutationId mutationId, int fromHostId)
    {
        getOrCreate(mutationId).receivedWriteResponse(mutationId, fromHostId);
    }

    void writeFailed(ShortMutationId mutationId, RequestFailureReason reason, InetAddressAndPort onHost)
    {
        getOrCreate(mutationId).writeFailed(mutationId, reason, onHost);
    }

    private LogOutgoingMutations getOrCreate(ShortMutationId mutationId)
    {
        LogOutgoingMutations mutations = log2MutationsMap.get(mutationId.logId());
        if (mutations == null)
            mutations = log2MutationsMap.computeIfAbsent(mutationId.logId(), ignore -> new LogOutgoingMutations());
        return mutations;
    }

    private static class LogOutgoingMutations
    {
        // {host, offset} -> per-participant mutation state map
        private final NonBlockingHashMapLong<OutgoingState> states = new NonBlockingHashMapLong<>();

        void sentWriteRequest(Mutation mutation, IntHashSet toHostIds)
        {
            IntHashSet.IntIterator iterator = toHostIds.iterator();
            while (iterator.hasNext())
            {
                OutgoingState state = getOrCreateState(iterator.nextValue(), mutation.id().offset());
                state.setInFlight();
            }
        }

        void receivedWriteResponse(ShortMutationId mutationId, int fromHostId)
        {
            OutgoingState state = remove(fromHostId, mutationId.offset());
            if (state != null)
                state.receivedWriteResponse();
        }

        void writeFailed(ShortMutationId mutationId, RequestFailureReason reason, InetAddressAndPort onHost)
        {
            // TODO check if need to implement this
        }

        // TODO FIXME: proper implementation
        OutgoingState getOrCreateState(int hostId, int offset)
        {
            long key = key(hostId, offset);
            OutgoingState state = states.get(key);
            if (state != null)
                return state;
            return states.putIfAbsent(key, new OutgoingState());
        }

        OutgoingState remove(int hostId, int offset)
        {
            long key = key(hostId, offset);
            return states.remove(key);
        }

        private long key(int hostId, int offset)
        {
            return ((long) hostId << 32) | (offset & 0xffffffffL);
        }

        private enum OutgoingStatus
        {
            IN_FLIGHT, ENQUEUED, DELIVERED
        }

        private static class OutgoingState
        {
            // TODO: callbacks for completion or failure
            private volatile OutgoingStatus status;

            void setInFlight()
            {
                status = OutgoingStatus.IN_FLIGHT;
            }

            void setEnqueued()
            {
                status = OutgoingStatus.ENQUEUED;
            }

            void receivedWriteResponse()
            {
                status = OutgoingStatus.DELIVERED;
                // TODO: invoke all callbacks
            }
        }

        private static class InFlight extends OutgoingState
        {
        }

        private static class Queued extends OutgoingState
        {
        }

        public interface Callback
        {
            void onSuccess();

            void onFailure();
        }
    }
}
