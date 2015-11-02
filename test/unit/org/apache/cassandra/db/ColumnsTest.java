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

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.junit.AfterClass;
import org.junit.Test;

import junit.framework.Assert;
import org.apache.cassandra.MockSchema;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class ColumnsTest
{

    private static final CFMetaData cfMetaData = MockSchema.newCFS().metadata;

    // this tests most of our functionality, since each subset we perform
    // reasonably comprehensive tests of basic functionality against
    @Test
    public void testContainsWithoutAndMergeTo()
    {
        for (ColumnsCheck randomColumns : randomSmall(true))
            testContainsWithoutAndMergeTo(randomColumns);
    }

    private void testContainsWithoutAndMergeTo(ColumnsCheck input)
    {
        // pick some arbitrary groupings of columns to remove at-once (to avoid factorial complexity)
        // whatever is left after each removal, we perform this logic on again, recursively
        List<List<ColumnDefinition>> removeGroups = shuffleAndGroup(Lists.newArrayList(input.definitions));
        for (List<ColumnDefinition> defs : removeGroups)
        {
            ColumnsCheck subset = input.remove(defs);

            // test contents after .without
            subset.assertContents();

            // test .contains
            assertSubset(input.columns, subset.columns);

            // test .mergeTo
            Columns otherSubset = input.columns;
            for (ColumnDefinition def : subset.definitions)
            {
                otherSubset = otherSubset.without(def);
                assertContents(otherSubset.mergeTo(subset.columns), input.definitions);
            }

            testContainsWithoutAndMergeTo(subset);
        }
    }

    private void assertSubset(Columns superset, Columns subset)
    {
        Assert.assertTrue(superset.containsAll(superset));
        Assert.assertTrue(superset.containsAll(subset));
        Assert.assertFalse(subset.containsAll(superset));
    }

    @Test
    public void testSerialize() throws IOException
    {
        testSerialize(Columns.NONE, Collections.emptyList());
        for (ColumnsCheck randomColumns : randomSmall(false))
            testSerialize(randomColumns.columns, randomColumns.definitions);
    }

    private void testSerialize(Columns columns, List<ColumnDefinition> definitions) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Columns.serializer.serialize(columns, out);
            Assert.assertEquals(Columns.serializer.serializedSize(columns), out.buffer().remaining());
            Columns deserialized = Columns.serializer.deserialize(new DataInputBuffer(out.buffer(), false), mock(columns));
            Assert.assertEquals(columns, deserialized);
            Assert.assertEquals(columns.hashCode(), deserialized.hashCode());
            assertContents(deserialized, definitions);
        }
    }

    @Test
    public void testSerializeSmallSubset() throws IOException
    {
        for (ColumnsCheck randomColumns : randomSmall(true))
            testSerializeSubset(randomColumns);
    }

    @Test
    public void testSerializeHugeSubset() throws IOException
    {
        for (ColumnsCheck randomColumns : randomHuge())
            testSerializeSubset(randomColumns);
    }

    @Test
    public void testContainsAllWithLargeNumberOfColumns()
    {
        List<String> names = new ArrayList<>();
        for (int i = 0; i < 50; i++)
            names.add("clustering_" + i);

        List<ColumnDefinition> defs = new ArrayList<>();
        addClustering(names, defs);

        Columns columns = Columns.from(new HashSet<>(defs));

        defs = new ArrayList<>();
        addClustering(names.subList(0, 8), defs);

        Columns subset = Columns.from(new HashSet<>(defs));

        Assert.assertTrue(columns.containsAll(subset));
    }

    private void testSerializeSubset(ColumnsCheck input) throws IOException
    {
        testSerializeSubset(input.columns, input.columns, input.definitions);
        testSerializeSubset(input.columns, Columns.NONE, Collections.emptyList());
        List<List<ColumnDefinition>> removeGroups = shuffleAndGroup(Lists.newArrayList(input.definitions));
        for (List<ColumnDefinition> defs : removeGroups)
        {
            Collections.sort(defs);
            ColumnsCheck subset = input.remove(defs);
            testSerializeSubset(input.columns, subset.columns, subset.definitions);
        }
    }

    private void testSerializeSubset(Columns superset, Columns subset, List<ColumnDefinition> subsetDefinitions) throws IOException
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            Columns.serializer.serializeSubset(subset, superset, out);
            Assert.assertEquals(Columns.serializer.serializedSubsetSize(subset, superset), out.buffer().remaining());
            Columns deserialized = Columns.serializer.deserializeSubset(superset, new DataInputBuffer(out.buffer(), false));
            Assert.assertEquals(subset, deserialized);
            Assert.assertEquals(subset.hashCode(), deserialized.hashCode());
            assertContents(deserialized, subsetDefinitions);
        }
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
                Assert.assertEquals(def, columns.getSimple(i));
                Assert.assertEquals(def, simple.next());
                ++firstComplexIdx;
            }
            else
            {
                Assert.assertFalse(simple.hasNext());
                hasComplex = true;
                Assert.assertEquals(i - firstComplexIdx, columns.complexIdx(def));
                Assert.assertEquals(def, columns.getComplex(i - firstComplexIdx));
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

        // check select order
        if (!columns.hasSimple() || !columns.getSimple(0).kind.isPrimaryKeyKind())
        {
            List<ColumnDefinition> selectOrderDefs = new ArrayList<>(defs);
            Collections.sort(selectOrderDefs, (a, b) -> a.name.bytes.compareTo(b.name.bytes));
            List<ColumnDefinition> selectOrderColumns = new ArrayList<>();
            Iterators.addAll(selectOrderColumns, columns.selectOrderIterator());
            Assert.assertEquals(selectOrderDefs, selectOrderColumns);
        }
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

        // then group (logarithmically, to ensure our recursive functions don't explode the state space)
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

    private static class ColumnsCheck
    {
        final Columns columns;
        final List<ColumnDefinition> definitions;

        private ColumnsCheck(Columns columns, List<ColumnDefinition> definitions)
        {
            this.columns = columns;
            this.definitions = definitions;
        }

        private ColumnsCheck(List<ColumnDefinition> definitions)
        {
            this.columns = Columns.from(BTreeSet.of(definitions));
            this.definitions = definitions;
        }

        ColumnsCheck remove(List<ColumnDefinition> remove)
        {
            Columns subset = columns;
            for (ColumnDefinition def : remove)
                subset = subset.without(def);
            Assert.assertEquals(columns.size() - remove.size(), subset.size());
            List<ColumnDefinition> remainingDefs = Lists.newArrayList(columns);
            remainingDefs.removeAll(remove);
            return new ColumnsCheck(subset, remainingDefs);
        }

        void assertContents()
        {
            ColumnsTest.assertContents(columns, definitions);
        }
    }

    private static List<ColumnsCheck> randomHuge()
    {
        List<ColumnsCheck> result = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        result.add(randomHuge(random.nextInt(64, 128), 0, 0, 0));
        result.add(randomHuge(0, random.nextInt(64, 128), 0, 0));
        result.add(randomHuge(0, 0, random.nextInt(64, 128), 0));
        result.add(randomHuge(0, 0, 0, random.nextInt(64, 128)));
        result.add(randomHuge(random.nextInt(64, 128), random.nextInt(64, 128), 0, 0));
        result.add(randomHuge(0, random.nextInt(64, 128), random.nextInt(64, 128), 0));
        result.add(randomHuge(0, 0, random.nextInt(64, 128), random.nextInt(64, 128)));
        result.add(randomHuge(random.nextInt(64, 128), random.nextInt(64, 128), random.nextInt(64, 128), 0));
        result.add(randomHuge(0, random.nextInt(64, 128), random.nextInt(64, 128), random.nextInt(64, 128)));
        result.add(randomHuge(random.nextInt(64, 128), random.nextInt(64, 128), random.nextInt(64, 128), random.nextInt(64, 128)));
        return result;
    }

    private static List<ColumnsCheck> randomSmall(boolean permitMultiplePartitionKeys)
    {
        List<ColumnsCheck> random = new ArrayList<>();
        for (int i = 1 ; i <= 3 ; i++)
        {
            int pkCount = permitMultiplePartitionKeys ? i - 1 : 1;
            if (permitMultiplePartitionKeys)
                random.add(randomSmall(i, i - 1, i - 1, i - 1));
            random.add(randomSmall(0, 0, i, i)); // both kinds of regular, no PK
            random.add(randomSmall(pkCount, i, i - 1, i - 1)); // PK + clustering, few or none regular
            random.add(randomSmall(pkCount, i - 1, i, i - 1)); // PK + few or none clustering, some regular, few or none complex
            random.add(randomSmall(pkCount, i - 1, i - 1, i)); // PK + few or none clustering or regular, some complex
        }
        return random;
    }

    private static ColumnsCheck randomSmall(int pkCount, int clCount, int regularCount, int complexCount)
    {
        List<String> names = new ArrayList<>();
        for (char c = 'a' ; c <= 'z' ; c++)
            names .add(Character.toString(c));

        List<ColumnDefinition> result = new ArrayList<>();
        addPartition(select(names, pkCount), result);
        addClustering(select(names, clCount), result);
        addRegular(select(names, regularCount), result);
        addComplex(select(names, complexCount), result);
        Collections.sort(result);
        return new ColumnsCheck(result);
    }

    private static List<String> select(List<String> names, int count)
    {
        List<String> result = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0 ; i < count ; i++)
        {
            int v = random.nextInt(names.size());
            result.add(names.get(v));
            names.remove(v);
        }
        return result;
    }

    private static ColumnsCheck randomHuge(int pkCount, int clCount, int regularCount, int complexCount)
    {
        List<ColumnDefinition> result = new ArrayList<>();
        Set<String> usedNames = new HashSet<>();
        addPartition(names(pkCount, usedNames), result);
        addClustering(names(clCount, usedNames), result);
        addRegular(names(regularCount, usedNames), result);
        addComplex(names(complexCount, usedNames), result);
        Collections.sort(result);
        return new ColumnsCheck(result);
    }

    private static List<String> names(int count, Set<String> usedNames)
    {
        List<String> names = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0 ; i < count ; i++)
        {
            builder.setLength(0);
            for (int j = 0 ; j < 3 || usedNames.contains(builder.toString()) ; j++)
                builder.append((char) random.nextInt('a', 'z' + 1));
            String name = builder.toString();
            names.add(name);
            usedNames.add(name);
        }
        return names;
    }

    private static void addPartition(List<String> names, List<ColumnDefinition> results)
    {
        for (String name : names)
            results.add(ColumnDefinition.partitionKeyDef(cfMetaData, bytes(name), UTF8Type.instance, 0));
    }

    private static void addClustering(List<String> names, List<ColumnDefinition> results)
    {
        int i = 0;
        for (String name : names)
            results.add(ColumnDefinition.clusteringDef(cfMetaData, bytes(name), UTF8Type.instance, i++));
    }

    private static void addRegular(List<String> names, List<ColumnDefinition> results)
    {
        for (String name : names)
            results.add(ColumnDefinition.regularDef(cfMetaData, bytes(name), UTF8Type.instance));
    }

    private static <V> void addComplex(List<String> names, List<ColumnDefinition> results)
    {
        for (String name : names)
            results.add(ColumnDefinition.regularDef(cfMetaData, bytes(name), SetType.getInstance(UTF8Type.instance, true)));
    }

    private static CFMetaData mock(Columns columns)
    {
        if (columns.isEmpty())
            return cfMetaData;
        CFMetaData.Builder builder = CFMetaData.Builder.create(cfMetaData.ksName, cfMetaData.cfName);
        boolean hasPartitionKey = false;
        for (ColumnDefinition def : columns)
        {
            switch (def.kind)
            {
                case PARTITION_KEY:
                    builder.addPartitionKey(def.name, def.type);
                    hasPartitionKey = true;
                    break;
                case CLUSTERING:
                    builder.addClusteringColumn(def.name, def.type);
                    break;
                case REGULAR:
                    builder.addRegularColumn(def.name, def.type);
                    break;
            }
        }
        if (!hasPartitionKey)
            builder.addPartitionKey("219894021498309239rufejsfjdksfjheiwfhjes", UTF8Type.instance);
        return builder.build();
    }
}
