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
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.CommonRange;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class SessionState extends AbstractState<SessionState.State, TimeUUID>
{
    public enum State
    {
        START, JOBS_START
    }

    public final TimeUUID parentRepairSession;
    public final String keyspace;
    public final String[] cfnames;
    public final CommonRange commonRange;
    private final ConcurrentMap<UUID, JobState> jobs = new ConcurrentHashMap<>();

    public final Phase phase = new Phase();

    public SessionState(Clock clock, TimeUUID parentRepairSession, String keyspace, String[] cfnames, CommonRange commonRange)
    {
        super(clock, nextTimeUUID(), State.class);
        this.parentRepairSession = parentRepairSession;
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
