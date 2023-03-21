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

package org.apache.cassandra.db.compaction;

import java.net.UnknownHostException;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DiskBoundaries;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class CompactionStrategyManagerBoundaryReloadTest extends CQLTester
{
    @Test
    public void testNoReload()
    {
        createTable("create table %s (id int primary key)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<List<AbstractCompactionStrategy>> strategies = cfs.getCompactionStrategyManager().getStrategies();
        DiskBoundaries db = cfs.getDiskBoundaries();
        StorageService.instance.getTokenMetadata().invalidateCachedRings();
        // make sure the strategy instances are the same (no reload)
        assertTrue(isSame(strategies, cfs.getCompactionStrategyManager().getStrategies()));
        // but disk boundaries are not .equal (ring version changed)
        assertNotEquals(db, cfs.getDiskBoundaries());
        assertTrue(db.isEquivalentTo(cfs.getDiskBoundaries()));

        db = cfs.getDiskBoundaries();
        alterTable("alter table %s with comment = 'abcd'");
        assertTrue(isSame(strategies, cfs.getCompactionStrategyManager().getStrategies()));
        // disk boundaries don't change because of alter
        assertEquals(db, cfs.getDiskBoundaries());
    }

    @Test
    public void testReload() throws UnknownHostException
    {
        createTable("create table %s (id int primary key)");
        ColumnFamilyStore cfs = getCurrentColumnFamilyStore();
        List<List<AbstractCompactionStrategy>> strategies = cfs.getCompactionStrategyManager().getStrategies();
        DiskBoundaries db = cfs.getDiskBoundaries();
        TokenMetadata tmd = StorageService.instance.getTokenMetadata();
        tmd.updateNormalToken(tmd.partitioner.getMinimumToken(), InetAddressAndPort.getByName("127.0.0.1"));
        tmd.updateNormalToken(tmd.partitioner.getMaximumToken(), InetAddressAndPort.getByName("127.0.0.2"));
        // make sure the strategy instances have been reloaded
        assertFalse(isSame(strategies,
                           cfs.getCompactionStrategyManager().getStrategies()));
        assertNotEquals(db, cfs.getDiskBoundaries());
        db = cfs.getDiskBoundaries();

        strategies = cfs.getCompactionStrategyManager().getStrategies();
        alterTable("alter table %s with compaction = {'class': 'SizeTieredCompactionStrategy', 'enabled': false}");
        assertFalse(isSame(strategies,
                           cfs.getCompactionStrategyManager().getStrategies()));
        assertEquals(db, cfs.getDiskBoundaries());

    }

    private boolean isSame(List<List<AbstractCompactionStrategy>> a, List<List<AbstractCompactionStrategy>> b)
    {
        if (a.size() != b.size())
            return false;
        for (int i = 0; i < a.size(); i++)
        {
            if (a.get(i).size() != b.get(i).size())
                return false;
            for (int j = 0; j < a.get(i).size(); j++)
                if (a.get(i).get(j) != b.get(i).get(j))
                    return false;
        }
        return true;
    }
}
