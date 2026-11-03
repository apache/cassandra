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

package org.apache.cassandra.tcm;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.metrics.TCMMetrics;
import org.apache.cassandra.tcm.log.Entry;
import org.apache.cassandra.tcm.log.LocalLog;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AbstractLocalProcessorClockTest
{
    private static final WithProperties properties = new WithProperties().set(CassandraRelevantProperties.CLOCK_GLOBAL,
                                                                              NegativeClock.class.getName());

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
        assertTrue(Clock.Global.nanoTime() < 0);
    }

    @AfterClass
    public static void cleanup()
    {
        properties.close();
    }

    @Test
    public void unlimitedRetryAdmitsWithNegativeClock()
    {
        try (LocalLog log = LocalLog.logSpec().sync()
                                    .withInitialState(new ClusterMetadata(Murmur3Partitioner.instance)).createLog())
        {
            log.readyUnchecked();
            AtomicLongBackedProcessor processor = new AtomicLongBackedProcessor(log);
            log.unsafeBootstrapForTesting(FBUtilities.getBroadcastAddressAndPort());
            Commit.Result result = processor.commit(new Entry.Id(1), CustomTransformation.make(1), null,
                                                    Retry.withNoTimeLimit(TCMMetrics.instance.commitRetries,
                                                                          Retry.unsafeRetryIndefinitely()));
            assertEquals(Epoch.FIRST.nextEpoch(), result.success().epoch);
        }
    }

    public static class NegativeClock extends Clock.Default
    {
        // just makes sure that we return a negative timestamp
        // checks that we handle Long.MAX_VALUE (no timelimit on the retries) in acquireProposalLock
        @Override
        public long nanoTime()
        {
            return -100;
        }
    }
}
