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

package org.apache.cassandra.db.virtual;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;

import org.junit.Test;

import accord.utils.Invariants;
import accord.utils.LargeBitSet;
import accord.utils.RandomSource;
import accord.utils.RandomTestRunner;
import accord.utils.UnhandledEnum;
import net.openhft.chronicle.core.util.IntBiPredicate;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.schema.CreateTableStatement;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.schema.TableMetadata;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.db.virtual.AbstractLazyVirtualTable.OnTimeout.FAIL;
import static org.apache.cassandra.db.virtual.LazyVirtualTableTest.Cmp.CMPS;
import static org.apache.cassandra.db.virtual.VirtualTable.Sorted.ASC;
import static org.apache.cassandra.db.virtual.VirtualTable.Sorted.DESC;
import static org.apache.cassandra.db.virtual.VirtualTable.Sorted.UNSORTED;

public class LazyVirtualTableTest extends CQLTester
{
    private static final String KEYSPACE = "system_accord_not_really";

    @Test
    public void test()
    {
        DatabaseDescriptor.setRpcTimeout(TimeUnit.MINUTES.toMillis(5L));
        RandomTestRunner.test().withSeed(3606318398900145565L).check(this::testOne);
        for (int i = 0 ; i < 10 ; ++i)
            RandomTestRunner.test().check(this::testOne);
    }

    private void testOne(RandomSource rnd)
    {
        QueryProcessor.clearInternalStatementsCache();
        QueryProcessor.clearPreparedStatementsCache();
        int keyCount = rnd.nextInt(1, 5);
        int valCount = rnd.nextInt(0, 3);
        int rowCount = rnd.nextInt(0, 12) - 1;
        rowCount = rowCount < 0 ? 0 : rnd.nextInt(1 << rowCount, 2 << rowCount);
        int[] keyDomains = new int[keyCount];
        Arrays.fill(keyDomains, 1);
        int uniqueKeys = Math.max(1, rowCount * rnd.nextInt(2, 5));
        for (int k = keyDomains.length - 1 ; k > 0 && uniqueKeys > 1 ; --k)
        {
            int maxScale = 31 - Integer.numberOfLeadingZeros(uniqueKeys);
            if (1 << maxScale != uniqueKeys)
                ++maxScale;
            int avgScale = maxScale / (1 + k);
            int scale = rnd.nextBiasedInt(0, avgScale, maxScale);
            keyDomains[k] = rnd.nextInt(1 << scale, Math.min(uniqueKeys, 2 << scale));
            uniqueKeys = (uniqueKeys + keyDomains[k] - 1) / keyDomains[k];
        }
        keyDomains[0] = uniqueKeys;
        int[] valDomains = new int[valCount];
        Arrays.fill(valDomains, Integer.MAX_VALUE);
        int queryCount = rnd.nextInt(1, Math.max(2, Math.min(1000, rowCount)));
        testOne(rnd, keyDomains, valDomains, rowCount, queryCount);
    }

    private void testOne(RandomSource rnd, int[] keyDomains, int[] valDomains, int rowCount, int queryLoops)
    {
        {
            int uniqueKeys = 1;
            for (int keyDomain : keyDomains)
                uniqueKeys *= keyDomain;
            Invariants.require(uniqueKeys >= 2 * rowCount);
        }

        int keyCount = keyDomains.length;
        int valCount = valDomains.length;
        String[] keyNames = IntStream.rangeClosed(1, keyCount).mapToObj(i -> "k" + i).toArray(String[]::new);
        String[] valNames = IntStream.rangeClosed(1, valCount).mapToObj(i -> "v" + i).toArray(String[]::new);

        int[][] allKeys = new int[rowCount][], allVals = new int[rowCount][];
        {
            TreeMap<int[], int[]> unique = new TreeMap<>(Arrays::compare);
            for (int r = 0 ; r < rowCount ; ++r)
            {
                int[] vals = new int[valCount];
                for (int v = 0 ; v < valCount ; ++v)
                    vals[v] = rnd.nextInt(valDomains[v]);

                int[] keys = new int[keyCount];
                do
                {
                    for (int k = 0 ; k < keyCount ; ++k)
                        keys[k] = rnd.nextInt(keyDomains[k]);
                }
                while (null != unique.putIfAbsent(keys, vals));
            }

            int count = 0;
            for (Map.Entry<int[], int[]> e : unique.entrySet())
            {
                allKeys[count] = e.getKey();
                allVals[count] = e.getValue();
                ++count;
            }
        }

        class TestTable extends AbstractLazyVirtualTable
        {
            final int[][] copyOfKeys = new int[rowCount][], copyOfVals = new int[rowCount][];
            TestTable(Sorted sorted, Sorted sortedByPartitionKey)
            {
                super(LazyVirtualTableTest.metadata(keyCount, valCount, sorted + "_" + sortedByPartitionKey), FAIL, sorted, sortedByPartitionKey);
                IntUnaryOperator index;
                switch (sorted)
                {
                    default: throw new UnhandledEnum(sorted);
                    case SORTED: throw new UnsupportedOperationException(); // TODO (desired): test this
                    case ASC:
                        index = i -> i;
                        break;
                    case UNSORTED:
                        int[] indexes = IntStream.range(0, rowCount).toArray();
                        index = i -> indexes[i];
                        switch (sortedByPartitionKey)
                        {
                            default: throw new UnhandledEnum(sortedByPartitionKey);
                            case DESC: case SORTED: throw new UnsupportedOperationException(); // TODO (desired): test this
                            case UNSORTED:
                                shuffle(indexes, 0, indexes.length, rnd);
                                break;

                            case ASC:
                                int prev = 0;
                                for (int i = 1; i < rowCount ; ++i)
                                {
                                    if (allKeys[prev][0] != allKeys[i][0])
                                    {
                                        shuffle(indexes, prev, i, rnd);
                                        prev = i;
                                    }
                                }
                                shuffle(indexes, prev, indexes.length, rnd);
                        }
                        break;
                    case DESC:
                        index = i -> rowCount - (i + 1);
                        break;
                }
                for (int i = 0 ; i < rowCount ; ++i)
                {
                    copyOfKeys[i] = allKeys[index.applyAsInt(i)];
                    copyOfVals[i] = allVals[index.applyAsInt(i)];
                }
            }

            @Override
            protected void collect(PartitionsCollector collector)
            {
                for (int i = 0 ; i < copyOfKeys.length ; ++i)
                {
                    int[] keys = copyOfKeys[i];
                    int[] vals = copyOfVals[i];
                    Object[] ckeys = new Object[keyCount - 1];
                    for (int k = 1; k < keyCount ; ++k)
                        ckeys[k - 1] = keys[k];
                    collector.partition(keys[0])
                             .collect(rows -> rows.add(ckeys).lazyCollect(cols -> {
                                 for (int j = 0 ; j < vals.length ; ++j)
                                     cols.add(valNames[j], vals[j]);
                             }));
                }
            }
        }

        List<VirtualTable> tables = new ArrayList<>();
        tables.add(new TestTable(ASC, ASC));
        tables.add(new TestTable(DESC, DESC));
        tables.add(new TestTable(UNSORTED, ASC));
        tables.add(new TestTable(UNSORTED, UNSORTED));
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KEYSPACE, tables));

        LargeBitSet matchingRows = new LargeBitSet(rowCount);
        int fieldCount = keyCount + valCount;
        LargeBitSet filteringKeys = new LargeBitSet(fieldCount);
        StringBuilder where = new StringBuilder();
        for (int q = 0 ; q < queryLoops ; q++)
        {
            matchingRows.setRange(0, rowCount);
            filteringKeys.setRange(0, fieldCount);
            where.setLength(0);
            do
            {
                int f;
                do
                {
                    int min = filteringKeys.nextSetBit(0);
                    f = rnd.nextInt(min, fieldCount);
                } while (!filteringKeys.unset(f));

                int val;
                if (rowCount == 0 || rnd.nextBoolean())
                {
                    val = rnd.nextInt(f < keyCount ? keyDomains[f] : valDomains[f - keyCount]);
                }
                else
                {
                    int i = rnd.nextInt(rowCount);
                    val = f < keyCount ? allKeys[i][f] : allVals[i][f - keyCount];
                }
                Cmp cmp = rnd.pick(CMPS);
                if (where.length() > 0)
                    where.append(" AND ");

                where.append(f < keyCount ? keyNames[f] : valNames[f - keyCount]);
                where.append(' ');
                where.append(cmp.str);
                where.append(' ');
                where.append(val);

                IntBiPredicate pred = cmp.predicate();
                for (int i = matchingRows.nextSetBit(0); i >= 0; i = matchingRows.nextSetBit(i + 1, -1))
                {
                    int rowVal = f < keyCount ? allKeys[i][f] : allVals[i][f - keyCount];
                    if (!pred.test(rowVal, val))
                        matchingRows.unset(i);
                }

                int matchCount = matchingRows.getSetBitCount();
                int limit = rnd.nextBoolean() ? 0 : rnd.nextInt(1, Math.max(2, 2 * matchCount));
                for (VirtualTable table : tables)
                {
                    UntypedResultSet results = execute("select * from " + table.metadata() + " where " + where + (limit <= 0 ? "" : " LIMIT " + limit));

                    int i = -1;
                    for (UntypedResultSet.Row row : results)
                    {
                        i = matchingRows.nextSetBit(i + 1);

                        for (int k = 0 ; k < keyCount ; ++k)
                            Assertions.assertThat(allKeys[i][k]).isEqualTo(row.getInt(keyNames[k]));

                        for (int v = 0 ; v < valCount ; ++v)
                            Assertions.assertThat(allVals[i][v]).isEqualTo(row.getInt(valNames[v]));
                    }

                    i = matchingRows.nextSetBit(i + 1);
                    if (limit >= matchCount || limit == 0) Assertions.assertThat(-1).isEqualTo(i);
                    else Assertions.assertThat(i).isNotEqualTo(-1);
                    Assertions.assertThat(results.size()).isEqualTo(limit == 0 ? matchCount : Math.min(matchCount, limit));
                }
            } while (matchingRows.getSetBitCount() > 0 && filteringKeys.getSetBitCount() > 0);
        }

        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KEYSPACE, tables));
    }

    enum Cmp
    {
        LT("<"), LE("<="), NE("!="), EQ("="), GE(">="), GT(">");

        static final Cmp[] CMPS = values();
        final String str;

        Cmp(String str)
        {
            this.str = str;
        }

        IntBiPredicate predicate()
        {
            switch (this)
            {
                default: throw new UnhandledEnum(this);
                case LT: return (a, b) -> a <  b;
                case LE: return (a, b) -> a <= b;
                case NE: return (a, b) -> a != b;
                case EQ: return (a, b) -> a == b;
                case GE: return (a, b) -> a >= b;
                case GT: return (a, b) -> a >  b;
            }
        }
    }

    private static TableMetadata metadata(int keys, int vals, String name)
    {
        return CreateTableStatement.parse(createTable(keys, vals, name), KEYSPACE)
                                   .comment("")
                                   .kind(TableMetadata.Kind.VIRTUAL)
                                   .partitioner(new LocalPartitioner(Int32Type.instance))
                                   .build();
    }

    private static String createTable(int keys, int vals, String name)
    {
        StringBuilder sb = new StringBuilder("CREATE TABLE ");
        sb.append(name).append(" (");
        for (int i = 1 ; i <= keys ; ++i)
            sb.append('k').append(i).append(" int, ");
        for (int i = 1 ; i <= vals ; ++i)
            sb.append('v').append(i).append(" int, ");
        sb.append(" PRIMARY KEY (");
        for (int i = 1 ; i <= keys ; ++i)
        {
            if (i > 1) sb.append(", ");
            sb.append('k').append(i);
        }
        sb.append("))");
        return sb.toString();
    }

    private static void shuffle(int[] array, int start, int end, RandomSource rnd)
    {
        while (end - start > 1)
        {
            int i = start + rnd.nextInt(end - start);
            int tmp = array[start];
            array[start] = array[i];
            array[i] = tmp;
            ++start;
        }
    }
}
