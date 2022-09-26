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

package org.apache.cassandra.simulator.systems;

import org.apache.cassandra.simulator.utils.ChanceRange;
import org.apache.cassandra.simulator.utils.LongRange;

public class NetworkConfig
{
    public static class PhaseConfig
    {
        final ChanceRange dropChance, delayChance;
        final LongRange normalLatency, delayLatency;

        public PhaseConfig(ChanceRange dropChance, ChanceRange delayChance, LongRange normalLatency, LongRange delayLatency)
        {
            this.dropChance = dropChance;
            this.delayChance = delayChance;
            this.normalLatency = normalLatency;
            this.delayLatency = delayLatency;
        }
    }

    final PhaseConfig normal;
    final PhaseConfig flaky;
    final ChanceRange partitionChance;
    final ChanceRange flakyChance;
    final LongRange reconfigureInterval;

    public NetworkConfig(PhaseConfig normal, PhaseConfig flaky, ChanceRange partitionChance, ChanceRange flakyChance, LongRange reconfigureInterval)
    {
        this.normal = normal;
        this.flaky = flaky;
        this.partitionChance = partitionChance;
        this.flakyChance = flakyChance;
        this.reconfigureInterval = reconfigureInterval;
    }
}
