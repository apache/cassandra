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
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.utils.Clock;

import static org.junit.Assert.assertTrue;

public class AbstractLocalProcessorClockTest
{
    // Unit tests fork per class; install the clock before Cassandra initializes Clock.Global.
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
    public void unlimitedRetryAdmitsWithNegativeClock() throws Exception
    {
        try (LocalLog log = LocalLog.logSpec().sync()
                                    .withInitialState(new ClusterMetadata(Murmur3Partitioner.instance)).createLog())
        {
            log.readyUnchecked();
            AbstractLocalProcessor processor = new AbstractLocalProcessor(log)
            {
                @Override
                protected boolean acceptCommit(ClusterMetadata metadata)
                {
                    return true;
                }

                @Override
                protected boolean tryCommitOne(Entry.Id id, Transformation transform, Epoch previous, Epoch next)
                {
                    return true;
                }

                @Override
                public ClusterMetadata fetchLogAndWait(Epoch epoch, Retry retry)
                {
                    throw new AssertionError("Unexpected catch-up");
                }
            };
            Transformation transformation = new Transformation()
            {
                @Override
                public Kind kind()
                {
                    return Kind.CUSTOM;
                }

                @Override
                public Result execute(ClusterMetadata metadata)
                {
                    return Transformation.success(metadata.transformer(), LockedRanges.AffectedRanges.EMPTY);
                }
            };
            Commit.Result result = processor.commit(new Entry.Id(1), transformation, null,
                                                    Retry.withNoTimeLimit(TCMMetrics.instance.commitRetries,
                                                                          Retry.unsafeRetryIndefinitely()));
            assertTrue(result.toString(), result.isSuccess());
        }
    }

    public static class NegativeClock extends Clock.Default
    {
        private final long origin = System.nanoTime(); // checkstyle: permit system clock

        @Override
        public long nanoTime()
        {
            return System.nanoTime() - origin - java.util.concurrent.TimeUnit.DAYS.toNanos(36500); // checkstyle: permit system clock
        }
    }
}
