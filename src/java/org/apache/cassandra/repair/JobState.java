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

package org.apache.cassandra.repair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.ValidationStatusRequest;
import org.apache.cassandra.repair.messages.ValidationStatusResponse;
import org.apache.cassandra.utils.CompletableFutureUtil;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

public final class JobState
{
    public enum State {
        INIT, START,
        SNAPSHOT_REQUEST, SNAPSHOT_COMPLETE,
        VALIDATION_REQUEST, VALIDATON_COMPLETE,
        SYNC_REQUEST, SYNC_COMPLETE,
        SUCCESS, FAILURE
    }

    public final UUID id = UUIDGen.getTimeUUID();
    private final long creationTimeMillis = System.currentTimeMillis();
    private final long[] stateTimes = new long[State.values().length];
    private int currentState;
    private String failureCause;
    private volatile long lastUpdatedAtNs;
    public final RepairJobDesc desc;
    private final Set<InetAddressAndPort> participants;

    public JobState(RepairJobDesc desc, Set<InetAddressAndPort> participants)
    {
        this.desc = desc;
        this.participants = participants;
        updateState(State.INIT);
    }

    public CompletableFuture<Map<InetAddressAndPort, RemoteState>> getParticipantState()
    {
        CompletableFuture<Map<InetAddressAndPort, RemoteState>> future;
        switch (getState())
        {
            case VALIDATION_REQUEST:
                future = getValidationState();
                break;
            default:
                future = getLocalState();
        }
        return future;
    }

    private CompletableFuture<Map<InetAddressAndPort, RemoteState>> getValidationState()
    {
        MessagingService ms = MessagingService.instance();
        Message<ValidationStatusRequest> msg = Message.builder(Verb.VALIDATION_STAT_REQ, new ValidationStatusRequest(desc)).build();
        List<CompletableFuture<Pair<InetAddressAndPort, RemoteState>>> futures = new ArrayList<>(participants.size());
        for (InetAddressAndPort participant : participants)
        {
            CompletableFuture<Pair<InetAddressAndPort, RemoteState>> f = ms.<ValidationStatusResponse>sendFuture(msg, participant).thenApply(rsp -> {
                ValidationStatusResponse status = rsp.payload;
                if (!status.isFound())
                    return Pair.create(participant, new RemoteState(State.VALIDATION_REQUEST.name().toLowerCase(), 0f, status.failureCause, 0, 0));
                RemoteState state;
                switch (status.state)
                {
                    case SUCCESS:
                        state = new RemoteState("participant validation is successful, but coordinator has not seen the merkle tree yet", 1f, null, status.lastUpdatedAtMillis, status.durationNanos);
                        break;
                    case FAILURE:
                        state = new RemoteState("failure", 1f, status.failureCause, status.lastUpdatedAtMillis, status.durationNanos);
                        break;
                    default:
                        state = new RemoteState("validating", status.progress, null, status.lastUpdatedAtMillis, status.durationNanos);
                }
                return Pair.create(participant, state);
            });
            futures.add(f);
        }
        return CompletableFutureUtil.allOf(futures)
                                    .thenApply(list -> {
                                 Map<InetAddressAndPort, RemoteState> map = Maps.newHashMapWithExpectedSize(list.size());
                                 for (Pair<InetAddressAndPort, RemoteState> e : list)
                                     map.put(e.left, e.right);
                                 return map;
                             });
    }

    private float getProgress()
    {
        if (isComplete())
            return 1f;
        return (float) currentState / (float) State.values().length;
    }

    private CompletableFuture<Map<InetAddressAndPort, RemoteState>> getLocalState()
    {
        Map<InetAddressAndPort, RemoteState> map = Maps.newHashMapWithExpectedSize(participants.size());
        State state = getState();
        for (InetAddressAndPort p : participants)
            map.put(p, new RemoteState(state.name().toLowerCase(), getProgress(), failureCause, getLastUpdatedAtMillis(), getDurationNanos()));
        return CompletableFuture.completedFuture(map);
    }

    public Set<InetAddressAndPort> getParticipants()
    {
        return participants;
    }

    public void start()
    {
        updateState(State.START);
    }

    public void snapshotRequest()
    {
        updateState(State.SNAPSHOT_REQUEST);
    }

    public void snapshotComplete()
    {
        updateState(State.SNAPSHOT_COMPLETE);
    }

    public void validationRequested()
    {
        updateState(State.VALIDATION_REQUEST);
    }

    public void validationComplete()
    {
        updateState(State.VALIDATON_COMPLETE);
    }

    public void syncRequest()
    {
        updateState(State.SYNC_REQUEST);
    }

    public void complete()
    {
        updateState(State.SUCCESS);
    }

    public void fail(String failureCause)
    {
        this.failureCause = failureCause;
        updateState(State.FAILURE);
    }

    public void fail(Throwable failureCause)
    {
        fail(Throwables.getStackTraceAsString(failureCause));
    }

    public State getState()
    {
        return State.values()[currentState];
    }

    public boolean isComplete()
    {
        switch (getState())
        {
            case SYNC_COMPLETE:
            case SUCCESS:
            case FAILURE:
                return true;
            default:
                return false;
        }
    }

    public long getDurationNanos()
    {
        if (isComplete())
            return lastUpdatedAtNs - stateTimes[0];
        // progress may not be updated frequently, so this shows live duration
        return System.nanoTime() - stateTimes[0];
    }

    public long getLastUpdatedAtMillis()
    {
        // this is not acurate, but close enough to target human readability
        long durationNanos = lastUpdatedAtNs - stateTimes[0];
        return creationTimeMillis + TimeUnit.NANOSECONDS.toMillis(durationNanos);
    }

    private void updateState(State state)
    {
        long now = System.nanoTime();
        stateTimes[currentState = state.ordinal()] = now;
        lastUpdatedAtNs = now;
    }
}
