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

package org.apache.cassandra.service.paxos.cleanup;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.AbstractPaxosRepair;
import org.apache.cassandra.service.paxos.Ballot;
import org.apache.cassandra.service.paxos.BallotGenerator;
import org.apache.cassandra.service.paxos.cleanup.PaxosTableRepairs.KeyRepair;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.service.paxos.Ballot.Flag.NONE;

public class PaxosTableRepairsTest
{
    private static DecoratedKey dk(int k)
    {
        return Murmur3Partitioner.instance.decorateKey(ByteBufferUtil.bytes(k));
    }

    private static final DecoratedKey DK1 = dk(1);
    private static final DecoratedKey DK2 = dk(2);

    private static class MockRepair extends AbstractPaxosRepair
    {
        private static State STARTED = new State();
        private boolean reportCompleted = false;
        private boolean failOnStart = false;

        public MockRepair(DecoratedKey key)
        {
            super(key, null);
        }

        public State restart(State state, long waitUntil)
        {
            if (failOnStart)
                throw new RuntimeException();
            return STARTED;
        }

        void complete()
        {
            set(DONE);
        }

        public boolean isComplete()
        {
            return reportCompleted || super.isComplete();
        }
    }

    private static class MockTableRepairs extends PaxosTableRepairs
    {
        @Override
        MockRepair createRepair(DecoratedKey key, Ballot incompleteBallot, ConsistencyLevel consistency, TableMetadata cfm)
        {
            return new MockRepair(key);
        }

        MockRepair startOrGetOrQueue(DecoratedKey key, int i)
        {
            return (MockRepair) startOrGetOrQueue(key, BallotGenerator.Global.atUnixMicros(i, NONE), ConsistencyLevel.SERIAL, null, r -> {});
        }
    }

    /**
     * repairs with different keys shouldn't interfere with each other
     */
    @Test
    public void testMultipleRepairs()
    {
        MockTableRepairs repairs = new MockTableRepairs();

        MockRepair repair1 = repairs.startOrGetOrQueue(DK1, 0);
        MockRepair repair2 = repairs.startOrGetOrQueue(DK2, 1);

        Assert.assertTrue(repair1.isStarted());
        Assert.assertTrue(repair2.isStarted());
        Assert.assertTrue(repairs.hasActiveRepairs(DK1));
        Assert.assertTrue(repairs.hasActiveRepairs(DK2));

        repair1.complete();
        repair2.complete();

        // completing the repairs should have cleaned repairs map
        Assert.assertFalse(repairs.hasActiveRepairs(DK1));
        Assert.assertFalse(repairs.hasActiveRepairs(DK2));
    }

    @Test
    public void testRepairQueueing()
    {
        MockTableRepairs repairs = new MockTableRepairs();

        MockRepair repair1 = repairs.startOrGetOrQueue(DK1, 0);
        MockRepair repair2 = repairs.startOrGetOrQueue(DK1, 1);
        MockRepair repair3 = repairs.startOrGetOrQueue(DK1, 2);

        Assert.assertTrue(repair1.isStarted());
        Assert.assertFalse(repair2.isStarted());
        Assert.assertFalse(repair3.isStarted());

        KeyRepair keyRepair = repairs.getKeyRepairUnsafe(DK1);
        Assert.assertEquals(repair1, keyRepair.activeRepair());
        Assert.assertTrue(keyRepair.queueContains(repair2));
        Assert.assertTrue(keyRepair.queueContains(repair3));

        repair1.complete();
        Assert.assertTrue(repair2.isStarted());
        Assert.assertTrue(repairs.hasActiveRepairs(DK1));
        Assert.assertEquals(repair2, keyRepair.activeRepair());
        Assert.assertTrue(keyRepair.queueContains(repair3));

        repair2.complete();
        Assert.assertTrue(repair3.isStarted());
        Assert.assertTrue(repairs.hasActiveRepairs(DK1));

        // completing the final repair should cleanup the map
        repair3.complete();
        Assert.assertFalse(repairs.hasActiveRepairs(DK1));
    }

    @Test
    public void testRepairCancellation()
    {
        MockTableRepairs repairs = new MockTableRepairs();

        MockRepair repair1 = repairs.startOrGetOrQueue(DK1, 0);
        MockRepair repair2 = repairs.startOrGetOrQueue(DK1, 1);
        MockRepair repair3 = repairs.startOrGetOrQueue(DK1, 2);

        Assert.assertTrue(repair1.isStarted());
        Assert.assertFalse(repair2.isStarted());
        Assert.assertFalse(repair3.isStarted());
        Assert.assertTrue(repairs.hasActiveRepairs(DK1));

        repairs.clear();
        Assert.assertTrue(repair2.isComplete());
        Assert.assertTrue(repair3.isComplete());
        Assert.assertFalse(repairs.hasActiveRepairs(DK1));

        MockRepair repair4 = repairs.startOrGetOrQueue(DK1, 0);
        Assert.assertTrue(repair4.isStarted());
        Assert.assertTrue(repairs.hasActiveRepairs(DK1));
        repair4.complete();
    }

    @Test
    public void testQueuedCancellation()
    {
        // if a queued repair is cancelled, it should be removed without affecting the active repair
        MockTableRepairs repairs = new MockTableRepairs();
        MockRepair repair1 = repairs.startOrGetOrQueue(DK1, 0);
        MockRepair repair2 = repairs.startOrGetOrQueue(DK1, 1);
        MockRepair repair3 = repairs.startOrGetOrQueue(DK1, 2);

        KeyRepair keyRepair = repairs.getKeyRepairUnsafe(DK1);
        Assert.assertEquals(repair1, keyRepair.activeRepair());
        Assert.assertTrue(keyRepair.queueContains(repair2));

        repair2.cancel();
        Assert.assertEquals(repair1, keyRepair.activeRepair());
        Assert.assertFalse(keyRepair.queueContains(repair2));

        repair1.complete();
        Assert.assertEquals(repair3, keyRepair.activeRepair());
        Assert.assertFalse(keyRepair.queueContains(repair2));
    }

    @Test
    public void testFailureToStart()
    {
        // if an exception is thrown during repair scheduling, the next repair should be scheduled or things should be cleaned up
        MockTableRepairs repairs = new MockTableRepairs();
        MockRepair repair1 = repairs.startOrGetOrQueue(DK1, 0);
        MockRepair repair2 = repairs.startOrGetOrQueue(DK1, 1);
        MockRepair repair3 = repairs.startOrGetOrQueue(DK1, 2);

        repair2.failOnStart = true;
        KeyRepair keyRepair = repairs.getKeyRepairUnsafe(DK1);
        Assert.assertEquals(repair1, keyRepair.activeRepair());
        Assert.assertTrue(keyRepair.queueContains(repair2));
        Assert.assertTrue(keyRepair.queueContains(repair3));
        Assert.assertFalse(repair2.isComplete());

        repair1.complete();
        Assert.assertEquals(repair3, keyRepair.activeRepair());
        Assert.assertFalse(keyRepair.queueContains(repair2));
        Assert.assertTrue(repair2.isComplete());
    }

    @Test
    public void testCompletedQueuedRepair()
    {
        // if a queued repair has been somehow completed (or cancelled) without also being removed, it should be skipped
        MockTableRepairs repairs = new MockTableRepairs();
        MockRepair repair1 = repairs.startOrGetOrQueue(DK1, 0);
        MockRepair repair2 = repairs.startOrGetOrQueue(DK1, 1);
        MockRepair repair3 = repairs.startOrGetOrQueue(DK1, 2);

        repair2.reportCompleted = true;
        KeyRepair keyRepair = repairs.getKeyRepairUnsafe(DK1);
        Assert.assertEquals(repair1, keyRepair.activeRepair());
        Assert.assertTrue(keyRepair.queueContains(repair2));
        Assert.assertTrue(keyRepair.queueContains(repair3));

        repair1.complete();
        Assert.assertEquals(repair3, keyRepair.activeRepair());
        Assert.assertFalse(keyRepair.queueContains(repair2));
    }

    @Test
    public void testEviction()
    {
        MockTableRepairs repairs = new MockTableRepairs();
        MockRepair repair1 = repairs.startOrGetOrQueue(DK1, 0);
        MockRepair repair2 = repairs.startOrGetOrQueue(DK1, 1);

        repairs.evictHungRepairs(System.nanoTime());
        KeyRepair keyRepair = repairs.getKeyRepairUnsafe(DK1);
        Assert.assertTrue(repair1.isComplete());
        Assert.assertEquals(repair2, keyRepair.activeRepair());
    }

    @Test
    public void testClearRepairs()
    {
        MockTableRepairs repairs = new MockTableRepairs();
        MockRepair repair1 = repairs.startOrGetOrQueue(DK1, 0);
        MockRepair repair2 = repairs.startOrGetOrQueue(DK1, 1);

        KeyRepair keyRepair = repairs.getKeyRepairUnsafe(DK1);
        Assert.assertEquals(repair1, keyRepair.activeRepair());

        repairs.clear();
        Assert.assertEquals(0, keyRepair.pending());
        Assert.assertNull(repairs.getKeyRepairUnsafe(DK1));
    }
}
