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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.CommonRange;
import org.apache.cassandra.repair.RepairCoordinator;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;

public class CoordinatorState extends AbstractState<CoordinatorState.State, TimeUUID>
{
    public enum State
    {
        SETUP, START,
        PREPARE_START, PREPARE_COMPLETE,
        REPAIR_START, REPAIR_COMPLETE
    }

    public final int cmd;
    public final String keyspace;
    public final RepairOption options;
    private final ConcurrentMap<TimeUUID, SessionState> sessions = new ConcurrentHashMap<>();

    private List<ColumnFamilyStore> columnFamilies = null;
    private RepairCoordinator.NeighborsAndRanges neighborsAndRanges = null;

    // API to split function calls for phase changes from getting the state
    public final Phase phase = new Phase();

    public CoordinatorState(Clock clock, int cmd, String keyspace, RepairOption options)
    {
        super(clock, nextTimeUUID(), State.class);
        this.cmd = cmd;
        this.keyspace = Objects.requireNonNull(keyspace);
        this.options = Objects.requireNonNull(options);
    }

    public Collection<SessionState> getSessions()
    {
        return sessions.values();
    }

    public Set<TimeUUID> getSessionIds()
    {
        return sessions.keySet();
    }

    public void register(SessionState state)
    {
        sessions.put(state.id, state);
    }

    public List<ColumnFamilyStore> getColumnFamilies()
    {
        return columnFamilies;
    }

    public String[] getColumnFamilyNames()
    {
        if (columnFamilies == null)
            return null;
        return columnFamilies.stream().map(ColumnFamilyStore::getTableName).toArray(String[]::new);
    }

    public RepairCoordinator.NeighborsAndRanges getNeighborsAndRanges()
    {
        return neighborsAndRanges;
    }

    public Set<InetAddressAndPort> getParticipants()
    {
        if (neighborsAndRanges == null)
            return null;
        return neighborsAndRanges.participants;
    }

    public List<CommonRange> getCommonRanges()
    {
        if (neighborsAndRanges == null)
            return null;
        return neighborsAndRanges.commonRanges;
    }

    public List<CommonRange> getFilteredCommonRanges()
    {
        if (neighborsAndRanges == null)
            return null;
        return neighborsAndRanges.filterCommonRanges(keyspace, getColumnFamilyNames());
    }

    @Override
    public String status()
    {
        State currentState = getStatus();
        Result result = getResult();
        if (result != null)
            return result.kind.name();
        else if (currentState == null)
            return "init";
        else if (currentState == State.REPAIR_START)
            return currentState.name() + " " + sessions.entrySet().stream().map(e -> e.getKey() + " -> " + e.getValue().status()).collect(Collectors.toList());
        else
            return currentState.name();
    }

    @Override
    public String toString()
    {
        return "CoordinatorState{" +
               "id=" + id +
               ", stateTimesNanos=" + Arrays.toString(stateTimesNanos) +
               ", status=" + status() +
               ", lastUpdatedAtNs=" + lastUpdatedAtNs +
               '}';
    }

    public final class Phase extends BaseSkipPhase
    {
        public void setup()
        {
            updateState(State.SETUP);
        }

        public void start(List<ColumnFamilyStore> columnFamilies, RepairCoordinator.NeighborsAndRanges neighborsAndRanges)
        {
            CoordinatorState.this.columnFamilies = Objects.requireNonNull(columnFamilies);
            CoordinatorState.this.neighborsAndRanges = Objects.requireNonNull(neighborsAndRanges);
            updateState(State.START);
        }

        public void prepareStart()
        {
            updateState(State.PREPARE_START);
        }

        public void prepareComplete()
        {
            updateState(State.PREPARE_COMPLETE);
        }

        public void repairSubmitted()
        {
            updateState(State.REPAIR_START);
        }

        public void repairCompleted()
        {
            updateState(State.REPAIR_COMPLETE);
        }
    }
}
