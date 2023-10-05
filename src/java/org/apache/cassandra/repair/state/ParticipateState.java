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
package org.apache.cassandra.repair.state;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.messages.PrepareMessage;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class ParticipateState extends AbstractCompletable<TimeUUID> implements WeightedHierarchy.Root
{
    public enum RegisterStatus
    { ACCEPTED, EXISTS, STATUS_REJECTED, ALREADY_COMPLETED }
    public final InetAddressAndPort initiator;
    public final List<TableId> tableIds;
    public final Collection<Range<Token>> ranges;
    public final boolean incremental;
    public final long repairedAt;
    public final boolean global;
    public final PreviewKind previewKind;
    private volatile boolean accepted = false;

    public final Phase phase = new Phase();

    public final ConcurrentMap<RepairJobDesc, Job> jobs = new ConcurrentHashMap<>();

    private static final long EMPTY_SIZE = ObjectSizes.measure(new ParticipateState(SharedContext.Global.instance.clock(), null, new PrepareMessage(nextTimeUUID(), Collections.emptyList(), Collections.emptyList(), false, 0L, false, PreviewKind.NONE)));
    private final AtomicLong estimatedRetainedSize = new AtomicLong(0);

    @Override
    public long independentRetainedSize()
    {
        long size = EMPTY_SIZE;

        // initiator comes from the deserialized PrepareMessage, not TokenMetadata, so the reference retained by this class
        // is expected to be the sole reference to that instance.
        size += initiator == null ? 0 : initiator.unsharedHeapSize();
        for (TableId ignored : tableIds)
            size += TableId.EMPTY_SIZE;
        for (Range<Token> range : ranges)
            size += ObjectSizes.sizeOf(range);

        Result result = getResult();
        size += result == null ? 0 : result.unsharedHeapSize();

        return size;
    }

    @Override
    public WeightedHierarchy.Root root()
    {
        return this;
    }

    @Override
    public AtomicLong totalNestedRetainedSize()
    {
        return estimatedRetainedSize;
    }

    @Override
    public void onRetainedSizeUpdate()
    {
        ActiveRepairService.instance().onUpdate(this);
    }

    @Override
    public boolean isAccepted()
    {
        return accepted;
    }

    public ParticipateState(Clock clock, InetAddressAndPort initiator, PrepareMessage msg)
    {
        super(clock, msg.parentRepairSession);
        this.initiator = initiator;
        this.tableIds = msg.tableIds;
        this.ranges = msg.ranges;
        this.incremental = msg.isIncremental;
        this.repairedAt = msg.repairedAt;
        this.global = msg.isGlobal;
        this.previewKind = msg.previewKind;
    }

    @Nullable
    public Job job(RepairJobDesc desc)
    {
        return jobs.get(desc);
    }

    public Job getOrCreateJob(RepairJobDesc desc)
    {
        boolean isNew = !jobs.containsKey(desc);
        Job job = jobs.computeIfAbsent(desc, d -> new Job(clock, this, d));
        if (isNew)
            onNestedStateRegistration(job);
        return job;
    }

    @Nullable
    public ValidationState validation(RepairJobDesc desc)
    {
        Job job = job(desc);
        if (job == null)
            return null;
        return job.validation();
    }

    public RegisterStatus register(ValidationState state)
    {
        return getOrCreateJob(state.desc).register(state);
    }

    @Nullable
    public SyncState sync(RepairJobDesc desc, SyncState.Id id)
    {
        Job job = job(desc);
        if (job == null)
            return null;
        return job.sync(id);
    }

    public RegisterStatus register(SyncState state)
    {
        return getOrCreateJob(state.id.desc).register(state);
    }

    public Collection<ValidationState> validations()
    {
        return jobs.values().stream()
                   .map(j -> j.validation())
                   .filter(f -> f != null)
                   .collect(Collectors.toList());
    }

    public Collection<UUID> validationIds()
    {
        return jobs.values().stream()
                   .map(j -> j.validation())
                   .filter(f -> f != null)
                   .map(v -> v.id)
                   .collect(Collectors.toList());
    }

    @Override
    public String toString()
    {
        Result result = getResult();
        return "ParticipateState{" +
               "initiator=" + initiator +
               ", status=" + (result == null ? "pending" : result.toString()) +
               ", jobs=" + jobs.values() +
               '}';
    }

    public class Phase extends BasePhase
    {
        public void accept()
        {
            accepted = true;
        }
    }

    public static class Job extends AbstractState<Job.State, RepairJobDesc> implements WeightedHierarchy.InternalNode
    {
        public enum State { ACCEPT, SNAPSHOT, VALIDATION, SYNC }

        private final ParticipateState participateState;
        private final AtomicReference<ValidationState> validation = new AtomicReference<>(null);
        private final ConcurrentMap<SyncState.Id, SyncState> syncs = new ConcurrentHashMap<>();
        private static final long EMPTY_SIZE;
        static
        {
            PrepareMessage msg = new PrepareMessage(nextTimeUUID(), Collections.emptyList(), Collections.emptyList(), false, 0L, false, PreviewKind.NONE);
            ParticipateState participateState = new ParticipateState(Clock.Global.clock(), FBUtilities.getBroadcastAddressAndPort(), msg);
            RepairJobDesc desc = new RepairJobDesc(null, null, null, null, null);
            EMPTY_SIZE = ObjectSizes.measure(new Job(Clock.Global.clock(), participateState, desc));
        }

        public Job(Clock clock, ParticipateState participateState, RepairJobDesc desc)
        {
            super(clock, desc, State.class);
            this.participateState = participateState;
        }

        @Override
        public WeightedHierarchy.Root root()
        {
            return participateState;
        }

        @Override
        public long independentRetainedSize()
        {
            long size = EMPTY_SIZE;

            // Excludes participateState since that's already measured as the parent, and both validation and syncs propagate
            // their updates back to the root so do not need to be counted here.

            return size;
        }

        @Override
        protected synchronized UpdateType maybeUpdateState(State state)
        {
            return super.maybeUpdateState(state);
        }

        public void snapshot()
        {
            updateState(State.SNAPSHOT);
        }

        public RegisterStatus register(ValidationState state)
        {
            return register(s -> {
                ValidationState validationState = validation.compareAndSet(null, s) ? null : validation();
                if (validationState != null)
                    onNestedStateRegistration(validationState);
                return validationState;
            }, State.VALIDATION, state);
        }

        @Nullable
        public ValidationState validation()
        {
            return validation.get();
        }

        public RegisterStatus register(SyncState state)
        {
            return register(s -> {
                SyncState syncState = syncs.putIfAbsent(s.id, s);
                if (syncState == null)
                    onNestedStateRegistration(s);
                return syncState;
            }, State.SYNC, state);
        }

        private <I, S extends AbstractState<?, I>> RegisterStatus register(Function<S, S> putter, State state, S value)
        {
            UpdateType updateType = maybeUpdateState(state);
            switch (updateType)
            {
                case ALREADY_COMPLETED:
                    return RegisterStatus.ALREADY_COMPLETED;
                case LARGER_STATE_SEEN:
                    return RegisterStatus.STATUS_REJECTED;
                case ACCEPTED:
                case NO_CHANGE:
                    // allow
                    break;
                default:
                    throw new IllegalStateException("Unknown status: " + updateType);
            }
            S current = putter.apply(value);
            return current == null ? RegisterStatus.ACCEPTED : RegisterStatus.EXISTS;
        }

        @Nullable
        public SyncState sync(SyncState.Id id)
        {
            return syncs.get(id);
        }

        @Override
        public String toString()
        {
            return super.toString();
        }
    }
}
