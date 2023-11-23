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

import java.util.Objects;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.utils.Clock;

public class SyncState extends AbstractState<SyncState.State, SyncState.Id>
{
    public enum State
    { ACCEPT, PLANNING, START }

    public final Phase phase = new Phase();

    public SyncState(Clock clock, RepairJobDesc desc, InetAddressAndPort initiator, InetAddressAndPort src, InetAddressAndPort dst)
    {
        super(clock, new Id(desc, initiator, src, dst), State.class);
    }

    public final class Phase extends BaseSkipPhase
    {
        public void accept()
        {
            updateState(State.ACCEPT);
        }

        public void planning()
        {
            updateState(State.PLANNING);
        }

        public void start()
        {
            updateState(State.START);
        }
    }

    public static class Id
    {
        public final RepairJobDesc desc;
        public final InetAddressAndPort initiator, src, dst;

        public Id(RepairJobDesc desc, InetAddressAndPort initiator, InetAddressAndPort src, InetAddressAndPort dst)
        {
            this.desc = desc;
            this.initiator = initiator;
            this.src = src;
            this.dst = dst;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Id id = (Id) o;
            return desc.equals(id.desc) && initiator.equals(id.initiator) && src.equals(id.src) && dst.equals(id.dst);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(desc, initiator, src, dst);
        }
    }
}
