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
package org.apache.cassandra.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import com.google.common.collect.Lists;

import org.junit.AfterClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.MockSchema;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.btree.BTreeSet;

public class ColumnsTest
{

    private static CFMetaData cfMetaData = MockSchema.newCFS().metadata;

    @Test
    public void testContainsWithoutAndMergeTo()
    {
        for (RandomColumns randomColumns : random())
            testContainsWithoutAndMergeTo(randomColumns.columns, randomColumns.definitions);
    }

    private void testContainsWithoutAndMergeTo(Columns columns, List<ColumnDefinition> definitions)
    {
        // pick some arbitrary groupings of columns to remove at-once (to avoid factorial complexity)
        // whatever is left after each removal, we perform this logic on again, recursively
        List<List<ColumnDefinition>> removeGroups = shuffleAndGroup(Lists.newArrayList(definitions));
        for (List<ColumnDefinition> defs : removeGroups)
        {
            Columns subset = columns;
            for (ColumnDefinition def : defs)
                subset = subset.without(def);
            Assert.assertEquals(columns.columnCount() - defs.size(), subset.columnCount());
            List<ColumnDefinition> remainingDefs = Lists.newArrayList(columns);
            remainingDefs.removeAll(defs);

            // test contents after .without
            assertContents(subset, remainingDefs);

            // test .contains
            assertSubset(columns, subset);

            // test .mergeTo
            Columns otherSubset = columns;
            for (ColumnDefinition def : remainingDefs)
            {
                otherSubset = otherSubset.without(def);
                assertContents(otherSubset.mergeTo(subset), definitions);
            }

            testContainsWithoutAndMergeTo(subset, remainingDefs);
        }
    }

    private void assertSubset(Columns superset, Columns subset)
    {
        Assert.assertTrue(superset.contains(superset));
        Assert.assertTrue(superset.contains(subset));
        Assert.assertFalse(subset.contains(superset));
    }

    private static void assertContents(Columns columns, List<ColumnDefinition> defs)
    {
        Assert.assertEquals(defs, Lists.newArrayList(columns));
        boolean hasSimple = false, hasComplex = false;
        int firstComplexIdx = 0;
        int i = 0;
        Iterator<ColumnDefinition> simple = columns.simpleColumns();
        Iterator<ColumnDefinition> complex = columns.complexColumns();
        Iterator<ColumnDefinition> all = columns.iterator();
        Predicate<ColumnDefinition> predicate = columns.inOrderInclusionTester();
        for (ColumnDefinition def : defs)
        {
            Assert.assertEquals(def, all.next());
            Assert.assertTrue(columns.contains(def));
            Assert.assertTrue(predicate.test(def));
            if (def.isSimple())
            {
                hasSimple = true;
                Assert.assertEquals(i, columns.simpleIdx(def));
                Assert.assertEquals(def, simple.next());
                ++firstComplexIdx;
            }
            else
            {
                Assert.assertFalse(simple.hasNext());
                hasComplex = true;
                Assert.assertEquals(i - firstComplexIdx, columns.complexIdx(def));
                Assert.assertEquals(def, complex.next());
            }
            i++;
        }
        Assert.assertEquals(defs.isEmpty(), columns.isEmpty());
        Assert.assertFalse(simple.hasNext());
        Assert.assertFalse(complex.hasNext());
        Assert.assertFalse(all.hasNext());
        Assert.assertEquals(hasSimple, columns.hasSimple());
        Assert.assertEquals(hasComplex, columns.hasComplex());
    }

    private static <V> List<List<V>> shuffleAndGroup(List<V> list)
    {
        // first shuffle
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0 ; i < list.size() - 1 ; i++)
        {
            int j = random.nextInt(i, list.size());
            V v = list.get(i);
            list.set(i, list.get(j));
            list.set(j, v);
        }

        // then group
        List<List<V>> result = new ArrayList<>();
        for (int i = 0 ; i < list.size() ;)
        {
            List<V> group = new ArrayList<>();
            int maxCount = list.size() - i;
            int count = maxCount <= 2 ? maxCount : random.nextInt(1, maxCount);
            for (int j = 0 ; j < count ; j++)
                group.add(list.get(i + j));
            i += count;
            result.add(group);
        }
        return result;
    }

    @AfterClass
    public static void cleanup()
    {
        MockSchema.cleanup();
    }

    private static class RandomColumns
    {
        final Columns columns;
        final List<ColumnDefinition> definitions;

        private RandomColumns(List<ColumnDefinition> definitions)
        {
            this.columns = Columns.from(BTreeSet.of(definitions));
            this.definitions = definitions;
        }
    }

    private static List<RandomColumns> random()
    {
        List<RandomColumns> random = new ArrayList<>();
        for (int i = 1 ; i <= 3 ; i++)
        {
            random.add(random(i, i - 1, i - 1, i - 1));
            random.add(random(i - 1, i, i - 1, i - 1));
            random.add(random(i - 1, i - 1, i, i - 1));
            random.add(random(i - 1, i - 1, i - 1, i));
        }
        return random;
    }

    private static RandomColumns random(int pkCount, int clCount, int regularCount, int complexCount)
    {
        List<Character> chars = new ArrayList<>();
        for (char c = 'a' ; c <= 'z' ; c++)
            chars.add(c);

        List<ColumnDefinition> result = new ArrayList<>();
        addPartition(select(chars, pkCount), result);
        addClustering(select(chars, clCount), result);
        addRegular(select(chars, regularCount), result);
        addComplex(select(chars, complexCount), result);
        Collections.sort(result);
        return new RandomColumns(result);
    }

    private static List<Character> select(List<Character> chars, int count)
    {
        List<Character> result = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0 ; i < count ; i++)
        {
            int v = random.nextInt(chars.size());
            result.add(chars.get(v));
            chars.remove(v);
        }
        return result;
    }

    private static void addPartition(List<Character> chars, List<ColumnDefinition> results)
    {
        addSimple(ColumnDefinition.Kind.PARTITION_KEY, chars, results);
    }

    private static void addClustering(List<Character> chars, List<ColumnDefinition> results)
    {
        addSimple(ColumnDefinition.Kind.CLUSTERING, chars, results);
    }

    private static void addRegular(List<Character> chars, List<ColumnDefinition> results)
    {
        addSimple(ColumnDefinition.Kind.REGULAR, chars, results);
    }

    private static void addSimple(ColumnDefinition.Kind kind, List<Character> chars, List<ColumnDefinition> results)
    {
        for (Character c : chars)
            results.add(new ColumnDefinition(cfMetaData, ByteBufferUtil.bytes(c.toString()), UTF8Type.instance, null, kind));
    }

    private static void addComplex(List<Character> chars, List<ColumnDefinition> results)
    {
        for (Character c : chars)
            results.add(new ColumnDefinition(cfMetaData, ByteBufferUtil.bytes(c.toString()), SetType.getInstance(UTF8Type.instance, true), null, ColumnDefinition.Kind.REGULAR));
    }
}
