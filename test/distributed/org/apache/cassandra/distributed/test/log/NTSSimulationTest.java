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

package org.apache.cassandra.distributed.test.log;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;

import static org.apache.cassandra.distributed.test.log.MetadataChangeSimulationTest.simulate;
import static org.apache.cassandra.harry.sut.TokenPlacementModel.NtsReplicationFactor;

public class NTSSimulationTest extends CMSTestBase
{
    private static final Logger logger = LoggerFactory.getLogger(NTSSimulationTest.class);
    private long seed;
    private Random rng;
    static
    {
        DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
    }

    @Before
    public void setup()
    {
        seed = System.nanoTime();
        logger.info("SEED: {}", seed);
        rng = new Random(seed);
    }

    @Test
    public void simulateNTS() throws Throwable
    {
        // TODO: right now, we pick a candidate only if there is enough rf to execute operation
        // but the problem is that if we start multiple operations that would take us under rf, we will screw up the placements
        // this was not happening before, and test is crafted now to disallow such states, but this is a bug.
        // we should either forbid this, or allow it, but make it work.
        for (int concurrency : new int[]{ 1, 3, 5 })
        {
            for (int rf : new int[]{ 2, 3, 5 })
            {
                for (int trans = 0; trans < rf; trans++)
                {
                    simulate(seed, rng, 50, 0, new NtsReplicationFactor(3, rf, trans), concurrency);
                }
            }
        }
    }
}
