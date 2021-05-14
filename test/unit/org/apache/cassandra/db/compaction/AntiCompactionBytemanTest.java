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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Uninterruptibles;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.Util;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.RangesAtEndpoint;
import org.apache.cassandra.locator.Replica;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.FBUtilities;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static org.apache.cassandra.utils.TimeUUID.Generator.nextTimeUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(BMUnitRunner.class)
public class AntiCompactionBytemanTest extends CQLTester
{
    @Test
    @BMRules(rules = { @BMRule(name = "Insert delay after first prepareToCommit",
             targetClass = "CompactionManager",
             targetMethod = "antiCompactGroup",
             condition = "not flagged(\"done\")",
             targetLocation = "AFTER INVOKE prepareToCommit",
             action = "Thread.sleep(2000);") } )
    public void testRedundantTransitions() throws Throwable
    {
        createTable("create table %s (id int primary key, i int)");
        execute("insert into %s (id, i) values (1, 1)");
        execute("insert into %s (id, i) values (2, 1)");
        execute("insert into %s (id, i) values (3, 1)");
        flush();
        UntypedResultSet res = execute("select token(id) as tok from %s");
        Iterator<UntypedResultSet.Row> it = res.iterator();
        List<Long> tokens = new ArrayList<>();
        while (it.hasNext())
        {
            UntypedResultSet.Row r = it.next();
            tokens.add(r.getLong("tok"));
        }
        tokens.sort(Long::compareTo);

        long first = tokens.get(0) - 10;
        long last = tokens.get(0) + 10;
        Range<Token> toRepair = new Range<>(new Murmur3Partitioner.LongToken(first), new Murmur3Partitioner.LongToken(last));
        first = tokens.get(1) - 10;
        last = tokens.get(1) + 10;
        Range<Token> pending = new Range<>(new Murmur3Partitioner.LongToken(first), new Murmur3Partitioner.LongToken(last));

        RangesAtEndpoint ranges = new RangesAtEndpoint.Builder(FBUtilities.getBroadcastAddressAndPort()).add(Replica.fullReplica(FBUtilities.getBroadcastAddressAndPort(), toRepair))
                                                                                                        .add(Replica.transientReplica(InetAddressAndPort.getByName("127.0.0.1"), pending)).build();

        AtomicBoolean failed = new AtomicBoolean(false);
        AtomicBoolean finished = new AtomicBoolean(false);

        Thread t = new Thread(() -> {
            while (!finished.get())
            {
                UntypedResultSet result = null;
                try
                {
                    result = execute("select id from %s");
                }
                catch (Throwable throwable)
                {
                    failed.set(true);
                    throw new RuntimeException(throwable);
                }

                Iterator<UntypedResultSet.Row> rowIter = result.iterator();
                Set<Integer> ids = new HashSet<>();
                while (rowIter.hasNext())
                {
                    UntypedResultSet.Row r = rowIter.next();
                    ids.add(r.getInt("id"));
                }
                if (!Sets.newHashSet(1,2,3).equals(ids))
                {
                    failed.set(true);
                    return;
                }
                Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
            }
        });
        t.start();
        assertEquals(1, getCurrentColumnFamilyStore().getLiveSSTables().size());
        SSTableReader sstableBefore = getCurrentColumnFamilyStore().getLiveSSTables().iterator().next();

        try (LifecycleTransaction txn = getCurrentColumnFamilyStore().getTracker().tryModify(getCurrentColumnFamilyStore().getLiveSSTables(), OperationType.ANTICOMPACTION))
        {
            CompactionManager.instance.antiCompactGroup(getCurrentColumnFamilyStore(), ranges, txn, nextTimeUUID(), () -> false);
        }
        finished.set(true);
        t.join();
        assertFalse(failed.get());
        assertFalse(getCurrentColumnFamilyStore().getLiveSSTables().contains(sstableBefore));
        Util.assertOnDiskState(getCurrentColumnFamilyStore(), 3);
    }
}
