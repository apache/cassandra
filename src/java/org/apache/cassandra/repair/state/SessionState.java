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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.CommonRange;
import org.apache.cassandra.repair.RepairParallelism;
import org.apache.cassandra.repair.SharedContext;
import org.apache.cassandra.repair.consistent.CoordinatorSessions;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.ObjectSizes;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class SessionState extends AbstractState<SessionState.State, TimeUUID> implements WeightedHierarchy.InternalNode
{
    public enum State
    {
        START, JOBS_START
    }

    public final TimeUUID parentRepairSession;
    public final String keyspace;
    public final String[] cfnames;
    public final CommonRange commonRange;
    private final CoordinatorState coordinator;
    private final ConcurrentMap<UUID, JobState> jobs = new ConcurrentHashMap<>();

    public final Phase phase = new Phase();

    private static final long EMPTY_SIZE = ObjectSizes.measure(new SessionState(new CoordinatorState(SharedContext.Global.instance.clock(), 0, "", new RepairOption(RepairParallelism.SEQUENTIAL, false, false, false, 8, Collections.emptySet(), false, false, false, PreviewKind.NONE, false, false, false, false)), SharedContext.Global.instance.clock(), "", new String[] {}, new CommonRange(ImmutableSet.of(FBUtilities.getBroadcastAddressAndPort()), Collections.emptySet(), Range.rangeSet(new Range<>(new Murmur3Partitioner.LongToken(Long.MIN_VALUE), new Murmur3Partitioner.LongToken(Long.MIN_VALUE))))));

    public SessionState(CoordinatorState coordinator, Clock clock, String keyspace, String[] cfnames, CommonRange commonRange)
    {
        super(clock, nextTimeUUID(), State.class);
        this.coordinator = coordinator;
        this.parentRepairSession = coordinator.id;
        this.keyspace = keyspace;
        this.cfnames = cfnames;
        this.commonRange = commonRange;
    }

    public Collection<JobState> getJobs()
    {
        return jobs.values();
    }

    public JobState getJob(UUID id)
    {
        return jobs.get(id);
    }

    public Set<UUID> getJobIds()
    {
        return jobs.keySet();
    }

    public Set<InetAddressAndPort> getParticipants()
    {
        return commonRange.endpoints;
    }

    @Override
    public String status()
    {
        State state = getStatus();
        Result result = getResult();
        if (result != null)
            return result.kind.name();
        else if (state == null)
            return "init";
        else if (state == State.JOBS_START)
            return state.name() + " " + jobs.entrySet().stream().map(e -> e.getKey() + " -> " + e.getValue().status()).collect(Collectors.toList());
        else
            return state.name();
    }

    public void register(JobState state)
    {
        jobs.put(state.id, state);
        onNestedStateRegistration(state);
    }

    @Override
    public long independentRetainedSize()
    {
        long size = EMPTY_SIZE;
        size += TimeUUID.TIMEUUID_SIZE; // parentRepairSession
        size += ObjectSizes.sizeOf(keyspace);
        for (String cfname : cfnames)
            size += ObjectSizes.sizeOf(cfname);
        size += commonRange.unsharedHeapSize();

        // Excludes coordinator because it is the parent for this cached object, so it is already counted. Excludes jobs
        // because those are also accounted for separately and reported as part of the parent's total cached size. See
        // CoordinatorState.nestedStateRetainedSize, which includes both SessionStates and JobStates under a CoordinatorState.

        return size;
    }

    @Override
    public WeightedHierarchy.Root root()
    {
        return coordinator;
    }

    public final class Phase extends BaseSkipPhase
    {
        public void start()
        {
            updateState(State.START);
        }

        public void jobsSubmitted()
        {
            updateState(State.JOBS_START);
        }
    }
}
