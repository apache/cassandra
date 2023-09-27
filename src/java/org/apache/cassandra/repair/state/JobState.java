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

import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.utils.Clock;

public class JobState extends AbstractState<JobState.State, UUID>
{
    public enum State
    {
        START,
        SNAPSHOT_START, SNAPSHOT_COMPLETE,
        VALIDATION_START, VALIDATION_COMPLETE,
        STREAM_START
    }

    public final RepairJobDesc desc;
    private final ImmutableSet<InetAddressAndPort> endpoints;

    public final Phase phase = new Phase();

    public JobState(Clock clock, RepairJobDesc desc, ImmutableSet<InetAddressAndPort> endpoints)
    {
        super(clock, desc.determanisticId(), State.class);
        this.desc = desc;
        this.endpoints = endpoints;
    }

    public Set<InetAddressAndPort> getParticipants()
    {
        return endpoints;
    }

    public final class Phase extends BasePhase
    {
        public void start()
        {
            updateState(State.START);
        }

        public void snapshotsSubmitted()
        {
            updateState(State.SNAPSHOT_START);
        }

        public void snapshotsCompleted()
        {
            updateState(State.SNAPSHOT_COMPLETE);
        }

        public void validationSubmitted()
        {
            updateState(State.VALIDATION_START);
        }

        public void validationCompleted()
        {
            updateState(State.VALIDATION_COMPLETE);
        }

        public void streamSubmitted()
        {
            updateState(State.STREAM_START);
        }
    }
}
