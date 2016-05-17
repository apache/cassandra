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

package org.apache.cassandra.service;


import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.Sets;

import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.io.sstable.SSTableReader;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.concurrent.Refs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ActiveRepairServiceTest extends SchemaLoader
{

    private static final String KEYSPACE1 = "Keyspace1";
    private static final String CF = "Standard1";

    @Test
    public void testGetActiveRepairedSSTableRefs()
    {
        ColumnFamilyStore store = prepareColumnFamilyStore();
        Set<SSTableReader> original = store.getUnrepairedSSTables();

        UUID prsId = UUID.randomUUID();
        ActiveRepairService.instance.registerParentRepairSession(prsId, Collections.singletonList(store), null);
        ActiveRepairService.ParentRepairSession prs = ActiveRepairService.instance.getParentRepairSession(prsId);

        //add all sstables to parent repair session
        prs.addSSTables(store.metadata.cfId, original);

        //retrieve all sstable references from parent repair sessions
        Refs<SSTableReader> refs = prs.getActiveRepairedSSTableRefs(store.metadata.cfId);
        Set<SSTableReader> retrieved = Sets.newHashSet(refs.iterator());
        assertEquals(original, retrieved);
        refs.release();

        //remove 1 sstable from data data tracker
        Set<SSTableReader> newLiveSet = new HashSet<>(original);
        Iterator<SSTableReader> it = newLiveSet.iterator();
        SSTableReader removed = it.next();
        it.remove();
        store.getDataTracker().replaceWithNewInstances(Collections.singleton(removed), Collections.EMPTY_SET);

        //retrieve sstable references from parent repair session again - removed sstable must not be present
        refs = prs.getActiveRepairedSSTableRefs(store.metadata.cfId);
        retrieved = Sets.newHashSet(refs.iterator());
        assertEquals(newLiveSet, retrieved);
        assertFalse(retrieved.contains(removed));
        refs.release();
    }

    private ColumnFamilyStore prepareColumnFamilyStore()
    {
        Keyspace keyspace = Keyspace.open(KEYSPACE1);
        ColumnFamilyStore store = keyspace.getColumnFamilyStore(CF);
        store.truncateBlocking();
        store.disableAutoCompaction();
        long timestamp = System.currentTimeMillis();
        //create 10 sstables
        for (int i = 0; i < 10; i++)
        {
            DecoratedKey key = Util.dk(Integer.toString(i));
            Mutation rm = new Mutation(KEYSPACE1, key.getKey());
            for (int j = 0; j < 10; j++)
                rm.add("Standard1", Util.cellname(Integer.toString(j)),
                       ByteBufferUtil.EMPTY_BYTE_BUFFER,
                       timestamp,
                       0);
            rm.apply();
            store.forceBlockingFlush();
        }
        return store;
    }
}
