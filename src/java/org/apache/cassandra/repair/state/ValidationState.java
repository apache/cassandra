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

import java.util.UUID;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.utils.Clock;

public class ValidationState extends AbstractState<ValidationState.State, UUID>
{
    public enum State
    { ACCEPT, START, SENDING_TREES }

    public final Phase phase = new Phase();
    public final RepairJobDesc desc;
    public final InetAddressAndPort initiator;
    public long estimatedPartitions;
    public long estimatedTotalBytes;
    public long partitionsProcessed;
    public long bytesRead;

    public ValidationState(Clock clock, RepairJobDesc desc, InetAddressAndPort initiator)
    {
        // UUID is used to make the validations table easier for users to lookup by a single key rather than a composite key
        super(clock, desc.determanisticId(), State.class);
        this.desc = desc;
        this.initiator = initiator;
    }

    public float getProgress()
    {
        int currentState = this.currentState;
        if (currentState == INIT)
            return 0.0F;
        if (currentState == COMPLETE)
            return 1.0F;
        if (estimatedPartitions == 0) // mostly to avoid / 0
            return 0.0f;
        return Math.min(0.99F, partitionsProcessed / (float) estimatedPartitions);
    }

    public final class Phase extends BaseSkipPhase
    {
        public void accept()
        {
            updateState(State.ACCEPT);
        }

        public void start(long estimatedPartitions, long estimatedTotalBytes)
        {
            updateState(State.START);
            ValidationState.this.estimatedPartitions = estimatedPartitions;
            ValidationState.this.estimatedTotalBytes = estimatedTotalBytes;
        }

        public void sendingTrees()
        {
            updateState(State.SENDING_TREES);
        }
    }
}
