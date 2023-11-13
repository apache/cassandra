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

package org.apache.cassandra.repair.consistent;

import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.messages.FailSession;
import org.apache.cassandra.repair.messages.FinalizeCommit;
import org.apache.cassandra.repair.messages.FinalizePromise;
import org.apache.cassandra.repair.messages.FinalizePropose;
import org.apache.cassandra.repair.messages.PrepareConsistentRequest;
import org.apache.cassandra.repair.messages.PrepareConsistentResponse;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.repair.messages.StatusRequest;
import org.apache.cassandra.repair.messages.StatusResponse;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.tools.nodetool.RepairAdmin;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Base class for consistent Local and Coordinator sessions
 *
 * <p/>
 * There are 4 stages to a consistent incremental repair.
 *
 * <h1>Repair prepare</h1>
 *  First, the normal {@link ActiveRepairService#prepareForRepair(TimeUUID, InetAddressAndPort, Set, RepairOption, boolean, List)} stuff
 *  happens, which sends out {@link PrepareMessage} and creates a {@link ActiveRepairService.ParentRepairSession}
 *  on the coordinator and each of the neighbors.
 *
 * <h1>Consistent prepare</h1>
 *  The consistent prepare step promotes the parent repair session to a consistent session, and isolates the sstables
 *  being repaired from  other sstables. First, the coordinator sends a {@link PrepareConsistentRequest} message to each repair
 *  participant (including itself). When received, the node creates a {@link LocalSession} instance, sets it's state to
 *  {@code PREPARING}, persists it, and begins a preparing the tables for incremental repair, which segregates the data
 *  being repaired from the rest of the table data. When the preparation completes, the session state is set to
 *  {@code PREPARED}, and a {@link PrepareConsistentResponse} is sent to the coordinator indicating success or failure.
 *  If the pending anti-compaction fails, the local session state is set to {@code FAILED}.
 *  <p/>
 *  (see {@link LocalSessions#handlePrepareMessage(org.apache.cassandra.net.Message)}
 *  <p/>
 *  Once the coordinator recieves positive {@code PrepareConsistentResponse} messages from all the participants, the
 *  coordinator begins the normal repair process.
 *  <p/>
 *  (see {@link CoordinatorSession#handlePrepareResponse(InetAddressAndPort, boolean)}
 *
 * <h1>Repair</h1>
 *  The coordinator runs the normal data repair process against the sstables segregated in the previous step. When a
 *  node recieves a {@link ValidationRequest}, it sets it's local session state to {@code REPAIRING}.
 *  <p/>
 *
 *  If all of the RepairSessions complete successfully, the coordinator begins the {@code Finalization} process. Otherwise,
 *  it begins the {@code Failure} process.
 *
 * <h1>Finalization</h1>
 *  The finalization step finishes the session and promotes the sstables to repaired. The coordinator begins by sending
 *  {@link FinalizePropose} messages to each of the participants. Each participant will set it's state to {@code FINALIZE_PROMISED}
 *  and respond with a {@link FinalizePromise} message. Once the coordinator has received promise messages from all participants,
 *  it will send a {@link FinalizeCommit} message to all of them, ending the coordinator session. When a node receives the
 *  {@code FinalizeCommit} message, it will set it's sessions state to {@code FINALIZED}, completing the {@code LocalSession}.
 *  <p/>
 *
 *  For the sake of simplicity, finalization does not immediately mark pending repair sstables repaired because of potential
 *  conflicts with in progress compactions. The sstables will be marked repaired as part of the normal compaction process.
 *  <p/>
 *
 *  On the coordinator side, see {@link CoordinatorSession#finalizePropose()}, {@link CoordinatorSession#handleFinalizePromise(InetAddressAndPort, boolean)},
 *  & {@link CoordinatorSession#finalizeCommit()}
 *  <p/>
 *
 *  On the local session side, see {@link LocalSessions#handleFinalizeProposeMessage(InetAddressAndPort, FinalizePropose)}
 *  & {@link LocalSessions#handleFinalizeCommitMessage(InetAddressAndPort, FinalizeCommit)}
 *
 * <h1>Failure</h1>
 *  If there are any failures or problems during the process above, the session will be failed. When a session is failed,
 *  the coordinator will send {@link FailSession} messages to each of the participants. In some cases (basically those not
 *  including Validation and Sync) errors are reported back to the coordinator by the local session, at which point, it
 *  will send {@code FailSession} messages out.
 *  <p/>
 *  Just as with finalization, sstables aren't immediately moved back to unrepaired, but will be demoted as part of the
 *  normal compaction process.
 *
 *  <p/>
 *  See {@link LocalSessions#failSession(UUID, boolean)} and {@link CoordinatorSession#fail()}
 *
 * <h1>Failure Recovery & Session Cleanup</h1>
 *  There are a few scenarios where sessions can get stuck. If a node fails mid session, or it misses a {@code FailSession}
 *  or {@code FinalizeCommit} message, it will never finish. To address this, there is a cleanup task that runs every
 *  10 minutes that attempts to complete idle sessions.
 *
 *  <p/>
 *  If a session is not completed (not {@code FINALIZED} or {@code FAILED}) and there's been no activity on the session for
 *  over an hour, the cleanup task will attempt to finish the session by learning the session state of the other participants.
 *  To do this, it sends a {@link StatusRequest} message to the other session participants. The participants respond with a
 *  {@link StatusResponse} message, notifying the sender of their state. If the sender receives a {@code FAILED} response
 *  from any of the participants, it fails the session locally. If it receives a {@code FINALIZED} response from any of the
 *  participants, it will set it's state to {@code FINALIZED} as well. Since the coordinator won't finalize sessions until
 *  it's received {@code FinalizePromise} messages from <i>all</i> participants, this is safe.
 *
 *
 *  <p/>
 *  If a session is not completed, and hasn't had any activity for over a day, the session is auto-failed.
 *
 *  <p/>
 *  Once a session has been completed for over 2 days, it's deleted.
 *
 *  <p/>
 *  Operators can also manually fail sessions with {@code nodetool repair_admin --cancel}
 *
 *  <p/>
 *  See {@link LocalSessions#cleanup()} and {@link RepairAdmin}
 *
 */
public abstract class ConsistentSession
{
    /**
     * The possible states of a {@code ConsistentSession}. The typical progression is {@link State#PREPARING}, {@link State#PREPARED},
     * {@link State#REPAIRING}, {@link State#FINALIZE_PROMISED}, and {@link State#FINALIZED}. With the exception of {@code FINALIZED},
     * any state can be transitions to {@link State#FAILED}.
     */
    public enum State
    {
        PREPARING(0),
        PREPARED(1),
        REPAIRING(2),
        FINALIZE_PROMISED(3),
        FINALIZED(4),
        FAILED(5);

        State(int expectedOrdinal)
        {
            assert ordinal() == expectedOrdinal;
        }

        private static final Map<State, Set<State>> transitions = new EnumMap<State, Set<State>>(State.class) {{
            put(PREPARING, ImmutableSet.of(PREPARED, FAILED));
            put(PREPARED, ImmutableSet.of(REPAIRING, FAILED));
            put(REPAIRING, ImmutableSet.of(FINALIZE_PROMISED, FAILED));
            put(FINALIZE_PROMISED, ImmutableSet.of(FINALIZED, FAILED));
            put(FINALIZED, ImmutableSet.of());
            put(FAILED, ImmutableSet.of());
        }};

        public boolean canTransitionTo(State state)
        {
            // redundant transitions are allowed because the failure recovery  mechanism can
            // send redundant status changes out, and they shouldn't throw exceptions
            return state == this || transitions.get(this).contains(state);
        }

        public static State valueOf(int ordinal)
        {
            return values()[ordinal];
        }
    }

    private volatile State state;
    public final SharedContext ctx;
    public final TimeUUID sessionID;
    public final InetAddressAndPort coordinator;
    public final ImmutableSet<TableId> tableIds;
    public final long repairedAt;
    public final ImmutableSet<Range<Token>> ranges;
    public final ImmutableSet<InetAddressAndPort> participants;

    ConsistentSession(AbstractBuilder builder)
    {
        builder.validate();
        this.ctx = builder.ctx;
        this.state = builder.state;
        this.sessionID = builder.sessionID;
        this.coordinator = builder.coordinator;
        this.tableIds = ImmutableSet.copyOf(builder.ids);
        this.repairedAt = builder.repairedAt;
        this.ranges = ImmutableSet.copyOf(builder.ranges);
        this.participants = ImmutableSet.copyOf(builder.participants);
    }

    public boolean isCompleted()
    {
        State s = getState();
        return s == State.FINALIZED || s == State.FAILED;
    }

    public State getState()
    {
        return state;
    }

    public void setState(State state)
    {
        this.state = state;
    }

    public boolean intersects(Iterable<Range<Token>> otherRanges)
    {
        return Iterables.any(ranges, r -> r.intersects(otherRanges));
    }

    public boolean equals(Object o)
    {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ConsistentSession that = (ConsistentSession) o;

        if (repairedAt != that.repairedAt) return false;
        if (state != that.state) return false;
        if (!sessionID.equals(that.sessionID)) return false;
        if (!coordinator.equals(that.coordinator)) return false;
        if (!tableIds.equals(that.tableIds)) return false;
        if (!ranges.equals(that.ranges)) return false;
        return participants.equals(that.participants);
    }

    public int hashCode()
    {
        int result = state.hashCode();
        result = 31 * result + sessionID.hashCode();
        result = 31 * result + coordinator.hashCode();
        result = 31 * result + tableIds.hashCode();
        result = 31 * result + (int) (repairedAt ^ (repairedAt >>> 32));
        result = 31 * result + ranges.hashCode();
        result = 31 * result + participants.hashCode();
        return result;
    }

    public String toString()
    {
        return "ConsistentSession{" +
               "state=" + state +
               ", sessionID=" + sessionID +
               ", coordinator=" + coordinator +
               ", tableIds=" + tableIds +
               ", repairedAt=" + repairedAt +
               ", ranges=" + ranges +
               ", participants=" + participants +
               '}';
    }

    abstract static class AbstractBuilder
    {
        private final SharedContext ctx;
        private State state;
        private TimeUUID sessionID;
        private InetAddressAndPort coordinator;
        private Set<TableId> ids;
        private long repairedAt;
        private Collection<Range<Token>> ranges;
        private Set<InetAddressAndPort> participants;

        protected AbstractBuilder(SharedContext ctx)
        {
            this.ctx = ctx;
        }

        void withState(State state)
        {
            this.state = state;
        }

        void withSessionID(TimeUUID sessionID)
        {
            this.sessionID = sessionID;
        }

        void withCoordinator(InetAddressAndPort coordinator)
        {
            this.coordinator = coordinator;
        }

        void withUUIDTableIds(Iterable<UUID> ids)
        {
            this.ids = ImmutableSet.copyOf(Iterables.transform(ids, TableId::fromUUID));
        }

        void withTableIds(Set<TableId> ids)
        {
            this.ids = ids;
        }

        void withRepairedAt(long repairedAt)
        {
            this.repairedAt = repairedAt;
        }

        void withRanges(Collection<Range<Token>> ranges)
        {
            this.ranges = ranges;
        }

        void withParticipants(Set<InetAddressAndPort> peers)
        {
            this.participants = peers;
        }

        void validate()
        {
            Preconditions.checkArgument(state != null);
            Preconditions.checkArgument(sessionID != null);
            Preconditions.checkArgument(coordinator != null);
            Preconditions.checkArgument(ids != null);
            Preconditions.checkArgument(!ids.isEmpty());
            Preconditions.checkArgument(repairedAt > 0
                                        || repairedAt == ActiveRepairService.UNREPAIRED_SSTABLE);
            Preconditions.checkArgument(ranges != null);
            Preconditions.checkArgument(!ranges.isEmpty());
            Preconditions.checkArgument(participants != null);
            Preconditions.checkArgument(!participants.isEmpty());
            Preconditions.checkArgument(participants.contains(coordinator));
        }
    }


}
