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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.messages.ValidationStatusRequest;
import org.apache.cassandra.repair.messages.ValidationStatusResponse;
import org.apache.cassandra.utils.CompletableFutureUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;

/**
 * Keeps track of the state for a single repair.
 *
 * This class is responsible for all sub-tasks (session, job, validation, sync, etc.) so must have a accurate
 * view of the state.
 *
 * The definition of success and progress are relative to sub-tasks, so a repair's progress is based off sessions which
 * are based off jobs which are based off validations/sync.  Since some tasks are remote (validation, sync, etc.) the
 * state of these tasks are expected to be stale and require refresh from time to time.
 *
 * This class is expected to be seperate from {@link RepairRunnable}, {@link RepairSession}, and {@link RepairJob} to
 * make sure any and all resources (objects, threads, etc.) can be GCed once the repair is complete. The state
 * should survive longer so it can be exposed to operators (via virtual tables and JMX).
 *
 * Repair has some special cases to worry about when it comes to state mangment; this mostly comes from the fact that
 * each repair ({@link RepairRunnable}) and tasks are actors which can fail on their own.  For this reason its possible
 * to have a repair succeed or fail without any child tasks; same is true for {@link RepairSession} as well.
 */
public class RepairState implements Iterable<RepairState.SessionState>
{
    public enum Phase {
        INIT, SETUP, STARTED,
        PREPARE_SUBMIT, PREPARE_COMPLETE,
        SESSIONS_SUBMIT, SESSIONS_COMPLETE,
        SKIPPED, SUCCESS, FAILURE
    }

    public final UUID id;
    public final int cmd; // managed by org.apache.cassandra.service.ActiveRepairService.repairStatusByCmd.  TODO can this be removed in favor of this class?
    public final String keyspace;
    public final RepairOption options;

    // state tracking
    private final long creationTimeMillis = System.currentTimeMillis();
    private final long[] phaseTimesNanos = new long[Phase.values().length];
    private int currentState;
    private String failureCause;
    private volatile long lastUpdatedAtNs;
    private final Map<UUID, SessionState> sessions = new HashMap<>();

    // defined once repair starts
    public List<CommonRange> commonRanges; // each CommonRange will spawn a new RepairSession
    public String[] cfnames;

    public RepairState(UUID id, int cmd, String keyspace, RepairOption options)
    {
        this.id = id;
        this.cmd = cmd;
        this.keyspace = keyspace;
        this.options = options;

        updatePhase(Phase.INIT);
    }

    public SessionState createSession(CommonRange range)
    {
        SessionState sessionState = new SessionState(this, range);
        sessions.put(sessionState.id, sessionState);
        return sessionState;
    }

    public Iterator<SessionState> iterator()
    {
        return sessions.values().iterator();
    }

    public Phase getPhase()
    {
        return Phase.values()[currentState];
    }

    public boolean isComplete()
    {
        switch (getPhase())
        {
            case SKIPPED:
            case SUCCESS:
            case FAILURE:
                return true;
            default:
                return false;
        }
    }

    public long getPhaseTimeMillis(Phase phase)
    {
        long deltaNanos = phaseTimesNanos[phase.ordinal()] - phaseTimesNanos[0];
        return creationTimeMillis + TimeUnit.NANOSECONDS.toMillis(deltaNanos);
    }

    public long getLastUpdatedAtMillis()
    {
        long deltaNanos = lastUpdatedAtNs - phaseTimesNanos[0];
        return creationTimeMillis + TimeUnit.NANOSECONDS.toMillis(deltaNanos);
    }

    public String getFailureCause()
    {
        return failureCause;
    }

    public void phaseSetup()
    {
        updatePhase(Phase.SETUP);
    }

    public void phaseStart(String[] cfnames, List<CommonRange> commonRanges)
    {
        this.cfnames = cfnames;
        this.commonRanges = commonRanges;
        updatePhase(Phase.STARTED);
    }

    public void phasePrepareStart()
    {
        updatePhase(Phase.PREPARE_SUBMIT);
    }

    public void phasePrepareComplete()
    {
        updatePhase(Phase.PREPARE_COMPLETE);
    }

    public void phasSessionsSubmitted()
    {
        updatePhase(Phase.SESSIONS_SUBMIT);
    }

    public void phaseSessionsCompleted()
    {
        updatePhase(Phase.SESSIONS_COMPLETE);
    }

    public void success()
    {
        updatePhase(Phase.SUCCESS);
    }

    public void skip(String reason)
    {
        this.failureCause = reason;
        updatePhase(Phase.SKIPPED);
    }

    public void fail(Throwable reason)
    {
        fail(Throwables.getStackTraceAsString(reason));
    }

    public void fail(String reason)
    {
        this.failureCause = reason;
        updatePhase(Phase.FAILURE);
    }

    private void updatePhase(Phase phase)
    {
        long now = System.nanoTime();
        phaseTimesNanos[currentState = phase.ordinal()] = now;
        lastUpdatedAtNs = now;
    }

    public static final class SessionState implements Iterable<JobState>
    {
        public enum State {
            INIT, START,
            JOBS_SUBMIT, JOBS_COMPLETE,
            SKIPPED, FAILURE
        }

        public final UUID id = UUIDGen.getTimeUUID();
        public final Map<UUID, JobState> jobs = new HashMap<>();
        private final long creationTimeMillis = System.currentTimeMillis();
        private final long[] stateTimes = new long[State.values().length];
        private int currentState;
        private String failureCause;
        private volatile long lastUpdatedAtNs;
        private final RepairState repair;
        public final CommonRange range;

        private SessionState(RepairState repair, CommonRange range)
        {
            this.repair = repair;
            this.range = range;
        }

        public JobState createJob(String tableName)
        {
            RepairJobDesc desc = new RepairJobDesc(repair.id, id, repair.keyspace, tableName, range.ranges);
            // endpoints removes self, so add it back
            Set<InetAddressAndPort> participants = ImmutableSet.<InetAddressAndPort>builder()
                                                   .addAll(range.endpoints) // this includes transient endpoints
                                                   .add(FBUtilities.getBroadcastAddressAndPort())
                                                   .build();
            JobState state = new JobState(desc, participants);
            jobs.put(state.id, state);
            return state;
        }

        public void start()
        {
            updateState(State.START);
        }

        public void jobsSubmitted()
        {
            updateState(State.JOBS_SUBMIT);
        }

        public void complete()
        {
            updateState(State.JOBS_COMPLETE);
        }

        public void skip(String reason)
        {
            failureCause = reason;
            updateState(State.SKIPPED);
        }

        public void fail(String cause)
        {
            failureCause = cause;
            updateState(State.FAILURE);
        }

        public void fail(Throwable cause)
        {
            fail(Throwables.getStackTraceAsString(cause));
        }

        public State getState()
        {
            return State.values()[currentState];
        }

        private void updateState(State state)
        {
            long now = System.nanoTime();
            stateTimes[currentState = state.ordinal()] = now;
            lastUpdatedAtNs = now;
        }

        public Iterator<JobState> iterator()
        {
            return jobs.values().iterator();
        }
    }

    public static final class JobState
    {
        public enum State {
            INIT, START,
            SNAPSHOT_REQUEST, SNAPSHOT_COMPLETE,
            VALIDATION_REQUEST, VALIDATON_COMPLETE,
            SYNC_REQUEST, SYNC_COMPLETE,
            FAILURE
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
                    RemoteState state;
                    switch (status.state)
                    {
                        case UNKNOWN:
                            state = new RemoteState(JobState.State.VALIDATION_REQUEST.name().toLowerCase(), 0f, null, status.lastUpdatedAtMillis, status.durationNanos);
                            break;
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
            updateState(State.SYNC_COMPLETE);
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
}
