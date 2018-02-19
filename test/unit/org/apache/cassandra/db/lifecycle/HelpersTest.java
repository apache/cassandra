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
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.BeforeClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.MockSchema;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.format.SSTableReader;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class HelpersTest
{

    @BeforeClass
    public static void setUp()
    {
        DatabaseDescriptor.daemonInitialization();
        MockSchema.cleanup();
    }

    static Set<Integer> a = set(1, 2, 3);
    static Set<Integer> b = set(4, 5, 6);
    static Set<Integer> c = set(7, 8, 9);
    static Set<Integer> abc = set(1, 2, 3, 4, 5, 6, 7, 8, 9);

    // this also tests orIn
    @Test
    public void testFilterIn()
    {
        check(Helpers.filterIn(abc, a), a);
        check(Helpers.filterIn(abc, a, c), set(1, 2, 3, 7, 8, 9));
        check(Helpers.filterIn(a, c), set());
    }

    // this also tests notIn
    @Test
    public void testFilterOut()
    {
        check(Helpers.filterOut(abc, a), set(4, 5, 6, 7, 8, 9));
        check(Helpers.filterOut(abc, b), set(1, 2, 3, 7, 8, 9));
        check(Helpers.filterOut(a, a), set());
    }

    @Test
    public void testConcatUniq()
    {
        check(Helpers.concatUniq(a, b, a, c, b, a), abc);
    }

    @Test
    public void testIdentityMap()
    {
        Integer one = new Integer(1);
        Integer two = new Integer(2);
        Integer three = new Integer(3);
        Map<Integer, Integer> identity = Helpers.identityMap(set(one, two, three));
        Assert.assertEquals(3, identity.size());
        Assert.assertSame(one, identity.get(1));
        Assert.assertSame(two, identity.get(2));
        Assert.assertSame(three, identity.get(3));
    }

    @Test
    public void testReplace()
    {
        boolean failure;
        failure = false;
        try
        {
            Helpers.replace(abc, a, c);
        }
        catch (AssertionError e)
        {
            failure = true;
        }
        Assert.assertTrue(failure);

        failure = false;
        try
        {
            Helpers.replace(a, abc, c);
        }
        catch (AssertionError e)
        {
            failure = true;
        }
        Assert.assertTrue(failure);

        failure = false;
        try
        {
            Map<Integer, Integer> notIdentity = ImmutableMap.of(1, new Integer(1), 2, 2, 3, 3);
            Helpers.replace(notIdentity, a, b);
        }
        catch (AssertionError e)
        {
            failure = true;
        }
        Assert.assertTrue(failure);

        // check it actually works when correct values provided
        check(Helpers.replace(a, a, b), b);
    }

    private static Set<Integer> set(Integer ... contents)
    {
        return ImmutableSet.copyOf(contents);
    }

    private static void check(Iterable<Integer> check, Set<Integer> expected)
    {
        Assert.assertEquals(expected, ImmutableSet.copyOf(check));
    }

    @Test
    public void testSetupDeletionNotification()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        Iterable<SSTableReader> readers = Lists.newArrayList(MockSchema.sstable(1, cfs), MockSchema.sstable(2, cfs));
        Throwable accumulate = Helpers.setReplaced(readers, null);
        Assert.assertNull(accumulate);
        for (SSTableReader reader : readers)
            Assert.assertTrue(reader.isReplaced());
        accumulate = Helpers.setReplaced(readers, null);
        assertNotNull(accumulate);
    }

    @Test
    public void testMarkObsolete()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        LogTransaction txnLogs = new LogTransaction(OperationType.UNKNOWN);
        Iterable<SSTableReader> readers = Lists.newArrayList(MockSchema.sstable(1, cfs), MockSchema.sstable(2, cfs));
        Iterable<SSTableReader> readersToKeep = Lists.newArrayList(MockSchema.sstable(3, cfs), MockSchema.sstable(4, cfs));

        List<LogTransaction.Obsoletion> obsoletions = new ArrayList<>();
        Helpers.prepareForObsoletion(readers, txnLogs, obsoletions, null);
        assertNotNull(obsoletions);
        assertEquals(2, obsoletions.size());

        Throwable accumulate = Helpers.markObsolete(obsoletions, null);
        Assert.assertNull(accumulate);
        for (SSTableReader reader : readers)
            Assert.assertTrue(reader.isMarkedCompacted());

        for (SSTableReader reader : readersToKeep)
            Assert.assertFalse(reader.isMarkedCompacted());

        accumulate = Helpers.markObsolete(obsoletions, null);
        assertNotNull(accumulate);

        txnLogs.finish();
    }

    @Test
    public void testObsoletionPerformance()
    {
        ColumnFamilyStore cfs = MockSchema.newCFS();
        LogTransaction txnLogs = new LogTransaction(OperationType.UNKNOWN);
        List<SSTableReader> readers = new ArrayList<>();

        for (int i = 0; i < 10000; i++)
        {
            readers.add(MockSchema.sstable(i + 1, cfs));
        }
        long start = System.currentTimeMillis();

        Helpers.prepareForObsoletion(readers.subList(0, 500), txnLogs, new ArrayList<>(),null );
        txnLogs.finish();
        long time = System.currentTimeMillis() - start;
        assertTrue(time < 20000);
    }
}
