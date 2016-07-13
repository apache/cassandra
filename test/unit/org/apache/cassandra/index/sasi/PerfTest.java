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
package org.apache.cassandra.index.sasi;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.*;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import org.apache.cassandra.*;
import org.apache.cassandra.config.*;
import org.apache.cassandra.cql3.*;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.*;
import org.apache.cassandra.db.marshal.*;
import org.apache.cassandra.db.partitions.*;
import org.apache.cassandra.db.rows.*;
import org.apache.cassandra.dht.*;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.schema.*;
import org.apache.cassandra.service.*;
import org.apache.cassandra.utils.*;

@Ignore
public class PerfTest
{
//    private static final IPartitioner PARTITIONER;
//    final int iterations = 30;
//    final int partitions = 5;
//
//    static {
//        System.setProperty("cassandra.config", "file:///Users/oleksandrpetrov/foss/java/cassandra/test/conf/cassandra-murmur.yaml");
//        PARTITIONER = Murmur3Partitioner.instance;
//    }
//
//    private static final String KS_NAME = "sasi";
//    private static final String CF_NAME = "test_cf";
//    private static final String CLUSTERING_CF_NAME_1 = "clustering_test_cf_1";
//    private static final String CLUSTERING_CF_NAME_2 = "clustering_test_cf_2";
//    private static final String STATIC_CF_NAME = "static_sasi_test_cf";
//
//    @BeforeClass
//    public static void loadSchema() throws ConfigurationException
//    {
//        SchemaLoader.loadSchema();
//        MigrationManager.announceNewKeyspace(KeyspaceMetadata.create(KS_NAME,
//                                                                     KeyspaceParams.simpleTransient(1),
//                                                                     Tables.of(SchemaLoader.sasiCFMD(KS_NAME, CF_NAME),
//                                                                               SchemaLoader.clusteringSASICFMD(KS_NAME, CLUSTERING_CF_NAME_1),
//                                                                               SchemaLoader.clusteringSASICFMD(KS_NAME, CLUSTERING_CF_NAME_2, "location"),
//                                                                               SchemaLoader.staticSASICFMD(KS_NAME, STATIC_CF_NAME))));
//    }
//
//    @After
//    public void cleanUp()
//    {
//        Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).truncateBlocking();
//        cleanupData();
//    }
//
//    private void cleanupData()
//    {
//        Keyspace ks = Keyspace.open(KS_NAME);
//        ks.getColumnFamilyStore(CF_NAME).truncateBlocking();
//        ks.getColumnFamilyStore(CLUSTERING_CF_NAME_1).truncateBlocking();
//    }
//
//    private static ColumnFamilyStore loadData(Map<String, Pair<String, Integer>> data, boolean forceFlush)
//    {
//        return loadData(data, System.currentTimeMillis(), forceFlush);
//    }
//
//    private static ColumnFamilyStore loadData(Map<String, Pair<String, Integer>> data, long timestamp, boolean forceFlush)
//    {
//        for (Map.Entry<String, Pair<String, Integer>> e : data.entrySet())
//            newMutation(e.getKey(), e.getValue().left, null, e.getValue().right, timestamp).apply();
//
//        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME);
//
//        if (forceFlush)
//            store.forceBlockingFlush();
//
//        return store;
//    }
//
//    private static UnfilteredPartitionIterator getIndexed(ColumnFamilyStore store, ColumnFilter columnFilter, DecoratedKey startKey, int maxResults, Expression... expressions)
//    {
//        DataRange range = (startKey == null)
//                          ? DataRange.allData(PARTITIONER)
//                          : DataRange.forKeyRange(new Range<>(startKey, PARTITIONER.getMinimumToken().maxKeyBound()));
//
//        RowFilter filter = RowFilter.create();
//        for (Expression e : expressions)
//            filter.add(store.metadata.getColumnDefinition(e.name), e.op, e.value);
//
//        ReadCommand command = new PartitionRangeReadCommand(store.metadata,
//                                                            FBUtilities.nowInSeconds(),
//                                                            columnFilter,
//                                                            filter,
//                                                            DataLimits.thriftLimits(maxResults, DataLimits.NO_LIMIT),
//                                                            range,
//                                                            Optional.empty());
//
//        return command.executeLocally(command.executionController());
//    }
//
//    private static Mutation newMutation(String key, String firstName, String lastName, int age, long timestamp)
//    {
//        Mutation rm = new Mutation(KS_NAME, decoratedKey(AsciiType.instance.decompose(key)));
//        List<Cell> cells = new ArrayList<>(3);
//
//        if (age >= 0)
//            cells.add(buildCell(ByteBufferUtil.bytes("age"), Int32Type.instance.decompose(age), timestamp));
//        if (firstName != null)
//            cells.add(buildCell(ByteBufferUtil.bytes("first_name"), UTF8Type.instance.decompose(firstName), timestamp));
//        if (lastName != null)
//            cells.add(buildCell(ByteBufferUtil.bytes("last_name"), UTF8Type.instance.decompose(lastName), timestamp));
//
//        update(rm, cells);
//        return rm;
//    }
//
//    private UntypedResultSet executeCQL(String cfName, String query, Object... values)
//    {
//        return QueryProcessor.executeOnceInternal(String.format(query, KS_NAME, cfName), values);
//    }
//
//    private static DecoratedKey decoratedKey(ByteBuffer key)
//    {
//        return PARTITIONER.decorateKey(key);
//    }
//
//    private static Row buildRow(Collection<Cell> cells)
//    {
//        return buildRow(cells.toArray(new Cell[cells.size()]));
//    }
//
//    private static Row buildRow(Cell... cells)
//    {
//        Row.Builder rowBuilder = BTreeRow.sortedBuilder();
//        rowBuilder.newRow(Clustering.EMPTY);
//        for (Cell c : cells)
//            rowBuilder.addCell(c);
//        return rowBuilder.build();
//    }
//
//    private static Cell buildCell(ByteBuffer name, ByteBuffer value, long timestamp)
//    {
//        CFMetaData cfm = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).metadata;
//        return BufferCell.live(cfm.getColumnDefinition(name), timestamp, value);
//    }
//
//    private static void update(Mutation rm, List<Cell> cells)
//    {
//        CFMetaData metadata = Keyspace.open(KS_NAME).getColumnFamilyStore(CF_NAME).metadata;
//        rm.add(PartitionUpdate.singleRowUpdate(metadata, rm.key(), buildRow(cells)));
//    }
//
//    private static class Expression
//    {
//        public final ByteBuffer name;
//        public final Operator op;
//        public final ByteBuffer value;
//
//        public Expression(ByteBuffer name, Operator op, ByteBuffer value)
//        {
//            this.name = name;
//            this.op = op;
//            this.value = value;
//        }
//    }
//
//    @Test
//    public void smallRows()
//    {
//        lamePerformanceTest("10 keys rows, 10 byte values ", new String(new byte[10]), 10);
//    }
//
//    @Test
//    public void smallRows1()
//    {
//        lamePerformanceTest("10 keys rows, 100 byte values ", new String(new byte[100]), 10);
//    }
//
//    @Test
//    public void smallRows3()
//    {
//        lamePerformanceTest("10 keys rows, 1000 byte values ", new String(new byte[1000]), 10);
//    }
//
//    @Test
//    public void mediumRows()
//    {
//        lamePerformanceTest("100 keys rows 10 byte values ", new String(new byte[10]), 100);
//    }
//
//    @Test
//    public void mediumRows2()
//    {
//        lamePerformanceTest("100 keys rows 100 byte values ", new String(new byte[100]), 100);
//    }
//
//    @Test
//    public void mediumRows3()
//    {
//        lamePerformanceTest("100 keys rows 1000 byte values ", new String(new byte[1000]), 100);
//    }
//
//    @Test
//    public void largeRows()
//    {
//        lamePerformanceTest("1000 keys rows 10 byte values ", new String(new byte[10]), 1000);
//    }
//
//    @Test
//    public void largeRows2()
//    {
//        lamePerformanceTest("1000 keys rows 100 byte values ", new String(new byte[100]), 1000);
//    }
//
//    @Test
//    public void largeRows3()
//    {
//        lamePerformanceTest("1000 keys rows 1000 byte values ", new String(new byte[1000]), 1000);
//    }
//
//    private void lamePerformanceTest(String prefix, String p, int keys)
//    {
//        ColumnFamilyStore store = Keyspace.open(KS_NAME).getColumnFamilyStore(CLUSTERING_CF_NAME_1);
//
//        for (int i = 0; i < partitions; i++)
//        {
//            for (int j = 0; j < keys; j++)
//            {
//                executeCQL(CLUSTERING_CF_NAME_1,
//                           "INSERT INTO %s.%s (name, location, age, height, score) VALUES (?, ?, ?, ?, ?)",
//                           p + Integer.toString(i),
//                           p + Integer.toString(j),
//                           j,
//                           j,
//                           (double) j);
//            }
//        }
//
//
//        store.forceBlockingFlush();
//
//        int[] timeDiff = runQueries(keys, "SELECT * FROM %s.%s WHERE location = ? ALLOW FILTERING", (i) -> p + Integer.toString(i));
//        System.out.println(prefix + " " + Arrays.toString(timeDiff));
//
//        timeDiff = runQueries(keys, "SELECT * FROM %s.%s WHERE height = ? ALLOW FILTERING", (i) -> i);
//        System.out.println(prefix + " " + Arrays.toString(timeDiff));
//
//        System.out.flush();
//    }
//
//    private int[] runQueries(int keys, String query, Function<Integer, Object>... queryArgs)
//    {
//        int[] sum = new int[iterations];
//        for (int j = 0; j < iterations; j++)
//        {
//            UntypedResultSet results;
//
//            long startTime = System.currentTimeMillis();
//            for (int i = 0; i < keys; i++)
//            {
//                Object[] objects = new Object[queryArgs.length];
//                for (int k = 0; k < queryArgs.length; k++)
//                {
//                    objects[k] = queryArgs[k].apply(i);
//                }
//                int size = executeCQL(CLUSTERING_CF_NAME_1, query, objects).size();
//                assert size == 5 : "Size was: " + size;
//
//            }
//            long endTime = System.currentTimeMillis();
//
//            sum[j] = (int) (endTime - startTime);
//        }
//        return sum;
//    }
}

