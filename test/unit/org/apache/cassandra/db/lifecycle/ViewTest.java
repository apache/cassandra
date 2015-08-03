/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db.lifecycle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.MockSchema;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.ImmutableSet.of;
import static com.google.common.collect.Iterables.concat;
import static java.util.Collections.singleton;
import static org.apache.cassandra.db.lifecycle.Helpers.emptySet;

public class ViewTest
{
    @BeforeClass
    public static void setUp()
    {
        MockSchema.cleanup();
    }

    @Test
    public void testSSTablesInBounds()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        View initialView = fakeView(0, 5, cfs);
        for (int i = 0 ; i < 5 ; i++)
        {
            for (int j = i ; j < 5 ; j++)
            {
                PartitionPosition min = MockSchema.readerBounds(i);
                PartitionPosition max = MockSchema.readerBounds(j);
                for (boolean minInc : new boolean[] { true })//, false} )
                {
                    for (boolean maxInc : new boolean[] { true })//, false} )
                    {
                        if (i == j && !(minInc && maxInc))
                            continue;

                        AbstractBounds<PartitionPosition> bounds = AbstractBounds.bounds(min, minInc, max, maxInc);
                        List<SSTableReader> r = ImmutableList.copyOf(initialView.sstablesInBounds(SSTableSet.LIVE,bounds.left, bounds.right));
                        Assert.assertEquals(String.format("%d(%s) %d(%s)", i, minInc, j, maxInc), j - i + (minInc ? 0 : -1) + (maxInc ? 1 : 0), r.size());
                    }
                }
            }
        }
    }

    @Test
    public void testCompaction()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        View initialView = fakeView(0, 5, cfs);
        View cur = initialView;
        List<SSTableReader> readers = ImmutableList.copyOf(initialView.sstables);
        Assert.assertTrue(View.permitCompacting(readers).apply(cur));
        // check we permit compacting duplicates in the predicate, so we don't spin infinitely if there is a screw up
        Assert.assertTrue(View.permitCompacting(ImmutableList.copyOf(concat(readers, readers))).apply(cur));
        // check we fail in the application in the presence of duplicates
        testFailure(View.updateCompacting(emptySet(), concat(readers.subList(0, 1), readers.subList(0, 1))), cur);

        // do lots of trivial checks that the compacting set and related methods behave properly for a simple update
        cur = View.updateCompacting(emptySet(), readers.subList(0, 2)).apply(cur);
        Assert.assertTrue(View.permitCompacting(readers.subList(2, 5)).apply(cur));
        Assert.assertFalse(View.permitCompacting(readers.subList(0, 2)).apply(cur));
        Assert.assertFalse(View.permitCompacting(readers.subList(0, 1)).apply(cur));
        Assert.assertFalse(View.permitCompacting(readers.subList(1, 2)).apply(cur));
        Assert.assertTrue(readers.subList(2, 5).containsAll(copyOf(cur.getUncompacting(readers))));
        Assert.assertEquals(3, copyOf(cur.getUncompacting(readers)).size());
        Assert.assertTrue(ImmutableSet.copyOf(cur.sstables(SSTableSet.NONCOMPACTING)).containsAll(readers.subList(2, 5)));
        Assert.assertEquals(3, ImmutableSet.copyOf(cur.sstables(SSTableSet.NONCOMPACTING)).size());

        // check marking already compacting readers fails with an exception
        testFailure(View.updateCompacting(emptySet(), readers.subList(0, 1)), cur);
        testFailure(View.updateCompacting(emptySet(), readers.subList(1, 2)), cur);
        testFailure(View.updateCompacting(copyOf(readers.subList(0, 1)), readers.subList(1, 2)), cur);

        // make equivalents of readers.subList(0, 3) that are different instances
        SSTableReader r0 = MockSchema.sstable(0, cfs), r1 = MockSchema.sstable(1, cfs), r2 = MockSchema.sstable(2, cfs);
        // attempt to mark compacting a version not in the live set
        testFailure(View.updateCompacting(emptySet(), of(r2)), cur);
        // update one compacting, one non-compacting, of the liveset to another instance of the same readers;
        // confirm liveset changes but compacting does not
        cur = View.updateLiveSet(copyOf(readers.subList(1, 3)), of(r1, r2)).apply(cur);
        Assert.assertSame(readers.get(0), cur.sstablesMap.get(r0));
        Assert.assertSame(r1, cur.sstablesMap.get(r1));
        Assert.assertSame(r2, cur.sstablesMap.get(r2));
        testFailure(View.updateCompacting(emptySet(), readers.subList(2, 3)), cur);
        Assert.assertSame(readers.get(1), Iterables.getFirst(Iterables.filter(cur.compacting, Predicates.equalTo(r1)), null));

        // unmark compacting, and check our methods are all correctly updated
        cur = View.updateCompacting(copyOf(readers.subList(0, 1)), emptySet()).apply(cur);
        Assert.assertTrue(View.permitCompacting(concat(readers.subList(0, 1), of(r2), readers.subList(3, 5))).apply(cur));
        Assert.assertFalse(View.permitCompacting(readers.subList(1, 2)).apply(cur));
        testFailure(View.updateCompacting(emptySet(), readers.subList(1, 2)), cur);
        testFailure(View.updateCompacting(copyOf(readers.subList(0, 2)), emptySet()), cur);
        Assert.assertTrue(copyOf(concat(readers.subList(0, 1), readers.subList(2, 5))).containsAll(copyOf(cur.getUncompacting(readers))));
        Assert.assertEquals(4, copyOf(cur.getUncompacting(readers)).size());
        Set<SSTableReader> nonCompacting = ImmutableSet.copyOf(cur.sstables(SSTableSet.NONCOMPACTING));
        Assert.assertTrue(nonCompacting.containsAll(readers.subList(2, 5)));
        Assert.assertTrue(nonCompacting.containsAll(readers.subList(0, 1)));
        Assert.assertEquals(4, nonCompacting.size());
    }

    private static void testFailure(Function<View, ?> function, View view)
    {
        boolean failed = true;
        try
        {
            function.apply(view);
            failed = false;
        }
        catch (Throwable t)
        {
        }
        Assert.assertTrue(failed);
    }

    @Test
    public void testFlushing()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        View initialView = fakeView(1, 0, cfs);
        View cur = initialView;
        Memtable memtable1 = initialView.getCurrentMemtable();
        Memtable memtable2 = MockSchema.memtable(cfs);

        cur = View.switchMemtable(memtable2).apply(cur);
        Assert.assertEquals(2, cur.liveMemtables.size());
        Assert.assertEquals(memtable1, cur.liveMemtables.get(0));
        Assert.assertEquals(memtable2, cur.getCurrentMemtable());

        Memtable memtable3 = MockSchema.memtable(cfs);
        cur = View.switchMemtable(memtable3).apply(cur);
        Assert.assertEquals(3, cur.liveMemtables.size());
        Assert.assertEquals(0, cur.flushingMemtables.size());
        Assert.assertEquals(memtable1, cur.liveMemtables.get(0));
        Assert.assertEquals(memtable2, cur.liveMemtables.get(1));
        Assert.assertEquals(memtable3, cur.getCurrentMemtable());

        testFailure(View.replaceFlushed(memtable2, null), cur);

        cur = View.markFlushing(memtable2).apply(cur);
        Assert.assertTrue(cur.flushingMemtables.contains(memtable2));
        Assert.assertEquals(2, cur.liveMemtables.size());
        Assert.assertEquals(1, cur.flushingMemtables.size());
        Assert.assertEquals(memtable2, cur.flushingMemtables.get(0));
        Assert.assertEquals(memtable1, cur.liveMemtables.get(0));
        Assert.assertEquals(memtable3, cur.getCurrentMemtable());

        cur = View.markFlushing(memtable1).apply(cur);
        Assert.assertEquals(1, cur.liveMemtables.size());
        Assert.assertEquals(2, cur.flushingMemtables.size());
        Assert.assertEquals(memtable1, cur.flushingMemtables.get(0));
        Assert.assertEquals(memtable2, cur.flushingMemtables.get(1));
        Assert.assertEquals(memtable3, cur.getCurrentMemtable());

        cur = View.replaceFlushed(memtable2, null).apply(cur);
        Assert.assertEquals(1, cur.liveMemtables.size());
        Assert.assertEquals(1, cur.flushingMemtables.size());
        Assert.assertEquals(memtable1, cur.flushingMemtables.get(0));
        Assert.assertEquals(memtable3, cur.getCurrentMemtable());

        SSTableReader sstable = MockSchema.sstable(1, cfs);
        cur = View.replaceFlushed(memtable1, singleton(sstable)).apply(cur);
        Assert.assertEquals(0, cur.flushingMemtables.size());
        Assert.assertEquals(1, cur.liveMemtables.size());
        Assert.assertEquals(memtable3, cur.getCurrentMemtable());
        Assert.assertEquals(1, cur.sstables.size());
        Assert.assertEquals(sstable, cur.sstablesMap.get(sstable));
    }

    static View fakeView(int memtableCount, int sstableCount, ColumnFamilyStore cfs)
    {
        List<Memtable> memtables = new ArrayList<>();
        List<SSTableReader> sstables = new ArrayList<>();
        for (int i = 0 ; i < memtableCount ; i++)
            memtables.add(MockSchema.memtable(cfs));
        for (int i = 0 ; i < sstableCount ; i++)
            sstables.add(MockSchema.sstable(i, cfs));
        return new View(ImmutableList.copyOf(memtables), Collections.<Memtable>emptyList(), Helpers.identityMap(sstables),
                        Collections.<SSTableReader, SSTableReader>emptyMap(), SSTableIntervalTree.build(sstables));
    }
}
