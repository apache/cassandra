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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import org.apache.cassandra.db.commitlog.CommitLog;

import org.junit.AfterClass;
import org.junit.Test;

import org.junit.Assert;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.MockSchema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.btree.BTreeSet;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

public class ColumnsTest
{
    static
    {
        DatabaseDescriptor.daemonInitialization();
        CommitLog.instance.start();
    }

    private static final TableMetadata TABLE_METADATA = MockSchema.newCFS().metadata();

    @Test
    public void testDeserializeCorruption() throws IOException
    {
        ColumnsCheck check = randomSmall(1, 0, 3, 0);
        Columns superset = check.columns;
        List<ColumnMetadata> minus1 = new ArrayList<>(check.definitions);
        minus1.remove(3);
        Columns minus2 = check.columns
                .without(check.columns.getSimple(3))
                .without(check.columns.getSimple(2));
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            // serialize a subset
            Columns.serializer.serializeSubset(minus1, superset, out);
            try (DataInputBuffer in = new DataInputBuffer(out.toByteArray()))
            {
                Columns.serializer.deserializeSubset(minus2, in);
                Assert.assertFalse(true);
            }
            catch (IOException e)
            {
            }
        }
    }

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
        List<List<ColumnMetadata>> removeGroups = shuffleAndGroup(Lists.newArrayList(input.definitions));
        for (List<ColumnMetadata> defs : removeGroups)
        {
            ColumnsCheck subset = input.remove(defs);

            // test contents after .without
            subset.assertContents();

            // test .contains
            assertSubset(input.columns, subset.columns);

            // test .mergeTo
            Columns otherSubset = input.columns;
            for (ColumnMetadata def : subset.definitions)
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

    private void testSerialize(Columns columns, List<ColumnMetadata> definitions) throws IOException
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
            names.add("regular_" + i);

        List<ColumnMetadata> defs = new ArrayList<>();
        addRegular(names, defs);

        Columns columns = Columns.from(new HashSet<>(defs));

        defs = new ArrayList<>();
        addRegular(names.subList(0, 8), defs);

        Columns subset = Columns.from(new HashSet<>(defs));

        Assert.assertTrue(columns.containsAll(subset));
    }

    @Test
    public void testStaticColumns()
    {
        testColumns(ColumnMetadata.Kind.STATIC);
    }

    @Test
    public void testRegularColumns()
    {
        testColumns(ColumnMetadata.Kind.REGULAR);
    }

    private void testColumns(ColumnMetadata.Kind kind)
    {
        List<ColumnMetadata> definitions = ImmutableList.of(
            def("a", UTF8Type.instance, kind),
            def("b", SetType.getInstance(UTF8Type.instance, true), kind),
            def("c", UTF8Type.instance, kind),
            def("d", SetType.getInstance(UTF8Type.instance, true), kind),
            def("e", UTF8Type.instance, kind),
            def("f", SetType.getInstance(UTF8Type.instance, true), kind),
            def("g", UTF8Type.instance, kind),
            def("h", SetType.getInstance(UTF8Type.instance, true), kind)
        );
        Columns columns = Columns.from(definitions);

        // test simpleColumnCount()
        Assert.assertEquals(4, columns.simpleColumnCount());

        // test simpleColumns()
        List<ColumnMetadata> simpleColumnsExpected =
            ImmutableList.of(definitions.get(0), definitions.get(2), definitions.get(4), definitions.get(6));
        List<ColumnMetadata> simpleColumnsActual = new ArrayList<>();
        Iterators.addAll(simpleColumnsActual, columns.simpleColumns());
        Assert.assertEquals(simpleColumnsExpected, simpleColumnsActual);

        // test complexColumnCount()
        Assert.assertEquals(4, columns.complexColumnCount());

        // test complexColumns()
        List<ColumnMetadata> complexColumnsExpected =
            ImmutableList.of(definitions.get(1), definitions.get(3), definitions.get(5), definitions.get(7));
        List<ColumnMetadata> complexColumnsActual = new ArrayList<>();
        Iterators.addAll(complexColumnsActual, columns.complexColumns());
        Assert.assertEquals(complexColumnsExpected, complexColumnsActual);

        // test size()
        Assert.assertEquals(8, columns.size());

        // test selectOrderIterator()
        List<ColumnMetadata> columnsExpected = definitions;
        List<ColumnMetadata> columnsActual = new ArrayList<>();
        Iterators.addAll(columnsActual, columns.selectOrderIterator());
        Assert.assertEquals(columnsExpected, columnsActual);
    }

    private void testSerializeSubset(ColumnsCheck input) throws IOException
    {
        testSerializeSubset(input.columns, input.columns, input.definitions);
        testSerializeSubset(input.columns, Columns.NONE, Collections.emptyList());
        List<List<ColumnMetadata>> removeGroups = shuffleAndGroup(Lists.newArrayList(input.definitions));
        for (List<ColumnMetadata> defs : removeGroups)
        {
            Collections.sort(defs);
            ColumnsCheck subset = input.remove(defs);
            testSerializeSubset(input.columns, subset.columns, subset.definitions);
        }
    }

    private void testSerializeSubset(Columns superset, Columns subset, List<ColumnMetadata> subsetDefinitions) throws IOException
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

    private static void assertContents(Columns columns, List<ColumnMetadata> defs)
    {
        Assert.assertEquals(defs, Lists.newArrayList(columns));
        boolean hasSimple = false, hasComplex = false;
        int firstComplexIdx = 0;
        int i = 0;
        Iterator<ColumnMetadata> simple = columns.simpleColumns();
        Iterator<ColumnMetadata> complex = columns.complexColumns();
        Iterator<ColumnMetadata> all = columns.iterator();
        Predicate<ColumnMetadata> predicate = columns.inOrderInclusionTester();
        for (ColumnMetadata def : defs)
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
            List<ColumnMetadata> selectOrderDefs = new ArrayList<>(defs);
            Collections.sort(selectOrderDefs, (a, b) -> a.name.bytes.compareTo(b.name.bytes));
            List<ColumnMetadata> selectOrderColumns = new ArrayList<>();
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
        final List<ColumnMetadata> definitions;

        private ColumnsCheck(Columns columns, List<ColumnMetadata> definitions)
        {
            this.columns = columns;
            this.definitions = definitions;
        }

        private ColumnsCheck(List<ColumnMetadata> definitions)
        {
            this.columns = Columns.from(BTreeSet.of(definitions));
            this.definitions = definitions;
        }

        ColumnsCheck remove(List<ColumnMetadata> remove)
        {
            Columns subset = columns;
            for (ColumnMetadata def : remove)
                subset = subset.without(def);
            Assert.assertEquals(columns.size() - remove.size(), subset.size());
            List<ColumnMetadata> remainingDefs = Lists.newArrayList(columns);
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

        List<ColumnMetadata> result = new ArrayList<>();
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
        List<ColumnMetadata> result = new ArrayList<>();
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

    private static void addPartition(List<String> names, List<ColumnMetadata> results)
    {
        for (String name : names)
            results.add(ColumnMetadata.partitionKeyColumn(TABLE_METADATA, bytes(name), UTF8Type.instance, 0));
    }

    private static void addClustering(List<String> names, List<ColumnMetadata> results)
    {
        int i = 0;
        for (String name : names)
            results.add(ColumnMetadata.clusteringColumn(TABLE_METADATA, bytes(name), UTF8Type.instance, i++));
    }

    private static void addRegular(List<String> names, List<ColumnMetadata> results)
    {
        for (String name : names)
            results.add(ColumnMetadata.regularColumn(TABLE_METADATA, bytes(name), UTF8Type.instance));
    }

    private static void addComplex(List<String> names, List<ColumnMetadata> results)
    {
        for (String name : names)
            results.add(ColumnMetadata.regularColumn(TABLE_METADATA, bytes(name), SetType.getInstance(UTF8Type.instance, true)));
    }

    private static ColumnMetadata def(String name, AbstractType<?> type, ColumnMetadata.Kind kind)
    {
        return new ColumnMetadata(TABLE_METADATA, bytes(name), type, ColumnMetadata.NO_POSITION, kind, null);
    }

    private static TableMetadata mock(Columns columns)
    {
        if (columns.isEmpty())
            return TABLE_METADATA;

        TableMetadata.Builder builder = TableMetadata.builder(TABLE_METADATA.keyspace, TABLE_METADATA.name);
        boolean hasPartitionKey = false;
        for (ColumnMetadata def : columns)
        {
            switch (def.kind)
            {
                case PARTITION_KEY:
                    builder.addPartitionKeyColumn(def.name, def.type);
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
            builder.addPartitionKeyColumn("219894021498309239rufejsfjdksfjheiwfhjes", UTF8Type.instance);
        return builder.build();
    }
}
