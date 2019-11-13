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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public final class SessionState implements Iterable<JobState>
{
    public enum State {
        INIT, START,
        JOBS_SUBMIT, JOBS_COMPLETE,
        SKIPPED, SUCCESS, FAILURE
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

    SessionState(RepairState repair, CommonRange range)
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
        updateState(State.SUCCESS);
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
