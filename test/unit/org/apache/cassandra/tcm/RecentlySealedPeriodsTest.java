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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RecentlySealedPeriodsTest
{
    @Test
    public void searchEmptyIndexReturnsMinPeriod()
    {
        Random random = new Random(System.nanoTime());
        RecentlySealedPeriods idx = RecentlySealedPeriods.EMPTY;
        for (int i=0; i<1000; i++)
        {
            Epoch e = Epoch.create(Math.abs(random.nextLong()));
            assertEquals(Sealed.EMPTY, idx.lookupEpochForSnapshot(e));
            assertEquals(Sealed.EMPTY, idx.lookupPeriodForReplication(e));
        }
    }

    @Test
    public void buildAndSearchIndex()
    {
        RecentlySealedPeriods idx = RecentlySealedPeriods.EMPTY;
        for (int i=1; i<=10;i++)
            idx = idx.with(Epoch.create((i*3)+1), i);

        assertEquals(10, idx.array().length);
        // lowest indexed period is 1
        assertEquals(1, idx.array()[0].period);
        // greatest indexed period is 10
        assertEquals(10, idx.array()[idx.array().length - 1].period);

        assertEquals(7, idx.lookupPeriodForReplication(Epoch.create(20)).period);
        // looking an epoch which is the max in a sealed period, should return the next period (for replication purposes)
        assertEquals(5, idx.lookupPeriodForReplication(Epoch.create(13)).period);
        // lookup an epoch < the minimum indexed value
        assertEquals(Sealed.EMPTY.period, idx.lookupPeriodForReplication(Epoch.create(1)).period);
        // lookup an epoch > the maximum indexed value
        assertEquals(Sealed.EMPTY.period, idx.lookupPeriodForReplication(Epoch.create(35)).period);
        // lookup the max epoch in the lowest indexed period (should return the next-lowest)
        assertEquals(2, idx.lookupPeriodForReplication(Epoch.create(4)).period);

        // add an additional entry
        idx = idx.with(Epoch.create(100), 11);
        // reassert the boundaries
        assertEquals(10, idx.array().length);
        // lowest indexed period is now 2
        assertEquals(2, idx.array()[0].period);
        // greatest indexed period is now 11
        assertEquals(11, idx.array()[idx.array().length - 1].period);

        // lookup an epoch within the (new) maximum sealed period
        assertEquals(11, idx.lookupPeriodForReplication(Epoch.create(76)).period);

        // repeat the previous lookups
        assertEquals(7, idx.lookupPeriodForReplication(Epoch.create(20)).period);
        assertEquals(5, idx.lookupPeriodForReplication(Epoch.create(13)).period);

        // lookup an epoch < the minimum indexed value
        assertEquals(Sealed.EMPTY.period, idx.lookupPeriodForReplication(Epoch.create(1)).period);
        // lookup an epoch > the maximum indexed value
        assertEquals(Sealed.EMPTY.period, idx.lookupPeriodForReplication(Epoch.create(101)).period);
        // the previous lower bound in the index should've been dropped out
        // so repeating that lookup should now return the min period
        assertEquals(Sealed.EMPTY.period, idx.lookupPeriodForReplication(Epoch.create(4)).period);
    }

    @Test
    public void sortAtBuildTime()
    {
        List<Long> keys = new ArrayList<>(10);
        for (int i=1; i<=10; i++)
            keys.add((long) (i * 10));
        Collections.shuffle(keys);
        RecentlySealedPeriods idx = RecentlySealedPeriods.EMPTY;
        for (Long epoch : keys)
            idx = idx.with(Epoch.create(epoch), epoch/10);

        Sealed[] index = idx.array();
        for(int i=0; i<index.length; i++)
        {
            Sealed entry = index[i];
            assertEquals((i+1)*10, entry.epoch.getEpoch());
            assertEquals((i+1), entry.period);
        }
    }
}
