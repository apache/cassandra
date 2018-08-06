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

package org.apache.cassandra.tools.fqltool;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.junit.Test;

import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.ValueIn;
import org.apache.cassandra.audit.FullQueryLogger;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.tools.fqltool.commands.Replay;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class FQLReplayTest
{
    public FQLReplayTest()
    {
        Util.initDatabaseDescriptor();
    }

    @Test
    public void testOrderedReplay() throws IOException
    {
        File f = generateQueries(100, true);
        int queryCount = 0;
        try (ChronicleQueue queue = ChronicleQueueBuilder.single(f).build();
             FQLQueryIterator iter = new FQLQueryIterator(queue.createTailer(), 101))
        {
            long last = -1;
            while (iter.hasNext())
            {
                FQLQuery q = iter.next();
                assertTrue(q.queryStartTime >= last);
                last = q.queryStartTime;
                queryCount++;
            }
        }
        assertEquals(100, queryCount);
    }
    @Test
    public void testMergingIterator() throws IOException
    {
        File f = generateQueries(100, false);
        File f2 = generateQueries(100, false);
        int queryCount = 0;
        try (ChronicleQueue queue = ChronicleQueueBuilder.single(f).build();
             ChronicleQueue queue2 = ChronicleQueueBuilder.single(f2).build();
             FQLQueryIterator iter = new FQLQueryIterator(queue.createTailer(), 101);
             FQLQueryIterator iter2 = new FQLQueryIterator(queue2.createTailer(), 101);
             MergeIterator<FQLQuery, List<FQLQuery>> merger = MergeIterator.get(Lists.newArrayList(iter, iter2), FQLQuery::compareTo, new Replay.Reducer()))
        {
            long last = -1;

            while (merger.hasNext())
            {
                List<FQLQuery> qs = merger.next();
                assertEquals(2, qs.size());
                assertEquals(0, qs.get(0).compareTo(qs.get(1)));
                assertTrue(qs.get(0).queryStartTime >= last);
                last = qs.get(0).queryStartTime;
                queryCount++;
            }
        }
        assertEquals(100, queryCount);
    }

    @Test
    public void testFQLQueryReader() throws IOException
    {
        FQLQueryReader reader = new FQLQueryReader();

        try (ChronicleQueue queue = ChronicleQueueBuilder.single(generateQueries(1000, true)).build())
        {
            ExcerptTailer tailer = queue.createTailer();
            int queryCount = 0;
            while (tailer.readDocument(reader))
            {
                assertNotNull(reader.getQuery());
                if (reader.getQuery() instanceof FQLQuery.Single)
                {
                    assertTrue(reader.getQuery().keyspace == null || reader.getQuery().keyspace.equals("querykeyspace"));
                }
                else
                {
                    assertEquals("someks", reader.getQuery().keyspace);
                }
                queryCount++;
            }
            assertEquals(1000, queryCount);
        }
    }

    @Test
    public void testStoringResults() throws Throwable
    {
        File tmpDir = Files.createTempDirectory("results").toFile();
        File queryDir = Files.createTempDirectory("queries").toFile();

        ResultHandler.ComparableResultSet res = createResultSet(10, 10, true);
        ResultStore rs = new ResultStore(Collections.singletonList(tmpDir), queryDir);
        try
        {
            FQLQuery query = new FQLQuery.Single("abc", 3, QueryOptions.DEFAULT, 12345, 11111, 22, "select * from abc", Collections.emptyList());
            rs.storeColumnDefinitions(query, Collections.singletonList(res.getColumnDefinitions()));
            Iterator<ResultHandler.ComparableRow> it = res.iterator();
            while (it.hasNext())
            {
                List<ResultHandler.ComparableRow> row = Collections.singletonList(it.next());
                rs.storeRows(row);
            }
            // this marks the end of the result set:
            rs.storeRows(Collections.singletonList(null));
        }
        finally
        {
            rs.close();
        }

        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> resultSets = readResultFile(tmpDir, queryDir);
        assertEquals(1, resultSets.size());
        assertEquals(res, resultSets.get(0).right);

    }

    @Test
    public void testCompareColumnDefinitions()
    {
        ResultHandler.ComparableResultSet res = createResultSet(10, 10, false);
        ResultComparator rc = new ResultComparator();

        List<ResultHandler.ComparableColumnDefinitions> colDefs = new ArrayList<>(100);
        List<String> targetHosts = new ArrayList<>(100);
        for (int i = 0; i < 100; i++)
        {
            targetHosts.add("host"+i);
            colDefs.add(res.getColumnDefinitions());
        }
        assertTrue(rc.compareColumnDefinitions(targetHosts, null, colDefs));
        colDefs.set(50, createResultSet(9, 9, false).getColumnDefinitions());
        assertFalse(rc.compareColumnDefinitions(targetHosts, null, colDefs));
    }

    @Test
    public void testCompareEqualRows()
    {
        ResultComparator rc = new ResultComparator();

        ResultHandler.ComparableResultSet res = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res2 = createResultSet(10, 10, false);
        List<ResultHandler.ComparableResultSet> toCompare = Lists.newArrayList(res, res2);
        List<Iterator<ResultHandler.ComparableRow>> iters = toCompare.stream().map(Iterable::iterator).collect(Collectors.toList());

        while (true)
        {
            List<ResultHandler.ComparableRow> rows = ResultHandler.rows(iters);
            assertTrue(rc.compareRows(Lists.newArrayList("eq1", "eq2"), null, rows));
            if (rows.stream().allMatch(Objects::isNull))
                break;
        }
    }

    @Test
    public void testCompareRowsDifferentCount()
    {
        ResultComparator rc = new ResultComparator();
        ResultHandler.ComparableResultSet res = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res2 = createResultSet(10, 10, false);
        List<ResultHandler.ComparableResultSet> toCompare = Lists.newArrayList(res, res2, createResultSet(10, 11, false));
        List<Iterator<ResultHandler.ComparableRow>> iters = toCompare.stream().map(Iterable::iterator).collect(Collectors.toList());
        boolean foundMismatch = false;
        while (true)
        {
            List<ResultHandler.ComparableRow> rows = ResultHandler.rows(iters);
            if (rows.stream().allMatch(Objects::isNull))
                break;
            if (!rc.compareRows(Lists.newArrayList("eq1", "eq2", "diff"), null, rows))
            {
                foundMismatch = true;
            }
        }
        assertTrue(foundMismatch);
    }

    @Test
    public void testCompareRowsDifferentContent()
    {
        ResultComparator rc = new ResultComparator();
        ResultHandler.ComparableResultSet res = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res2 = createResultSet(10, 10, false);
        List<ResultHandler.ComparableResultSet> toCompare = Lists.newArrayList(res, res2, createResultSet(10, 10, true));
        List<Iterator<ResultHandler.ComparableRow>> iters = toCompare.stream().map(Iterable::iterator).collect(Collectors.toList());
        while (true)
        {
            List<ResultHandler.ComparableRow> rows = ResultHandler.rows(iters);
            if (rows.stream().allMatch(Objects::isNull))
                break;
            assertFalse(rows.toString(), rc.compareRows(Lists.newArrayList("eq1", "eq2", "diff"), null, rows));
        }
    }

    @Test
    public void testCompareRowsDifferentColumnCount()
    {
        ResultComparator rc = new ResultComparator();
        ResultHandler.ComparableResultSet res = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res2 = createResultSet(10, 10, false);
        List<ResultHandler.ComparableResultSet> toCompare = Lists.newArrayList(res, res2, createResultSet(11, 10, false));
        List<Iterator<ResultHandler.ComparableRow>> iters = toCompare.stream().map(Iterable::iterator).collect(Collectors.toList());
        while (true)
        {
            List<ResultHandler.ComparableRow> rows = ResultHandler.rows(iters);
            if (rows.stream().allMatch(Objects::isNull))
                break;
            assertFalse(rows.toString(), rc.compareRows(Lists.newArrayList("eq1", "eq2", "diff"), null, rows));
        }
    }

    @Test
    public void testResultHandler() throws IOException
    {
        List<String> targetHosts = Lists.newArrayList("hosta", "hostb", "hostc");
        File tmpDir = Files.createTempDirectory("testresulthandler").toFile();
        File queryDir = Files.createTempDirectory("queries").toFile();
        List<File> resultPaths = new ArrayList<>();
        targetHosts.forEach(host -> { File f = new File(tmpDir, host); f.mkdir(); resultPaths.add(f);});
        ResultHandler rh = new ResultHandler(targetHosts, resultPaths, queryDir);
        ResultHandler.ComparableResultSet res = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res2 = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res3 = createResultSet(10, 10, false);
        List<ResultHandler.ComparableResultSet> toCompare = Lists.newArrayList(res, res2, res3);
        rh.handleResults(new FQLQuery.Single("abcabc", 3, QueryOptions.DEFAULT, 1111, 2222, 3333, "select * from xyz", Collections.emptyList()), toCompare);
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results1 = readResultFile(resultPaths.get(0), queryDir);
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results2 = readResultFile(resultPaths.get(1), queryDir);
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results3 = readResultFile(resultPaths.get(2), queryDir);
        assertEquals(results1, results2);
        assertEquals(results1, results3);
        assertEquals(Iterables.getOnlyElement(results3).right, res);
    }

    @Test
    public void testResultHandlerWithDifference() throws IOException
    {
        List<String> targetHosts = Lists.newArrayList("hosta", "hostb", "hostc");
        File tmpDir = Files.createTempDirectory("testresulthandler").toFile();
        File queryDir = Files.createTempDirectory("queries").toFile();
        List<File> resultPaths = new ArrayList<>();
        targetHosts.forEach(host -> { File f = new File(tmpDir, host); f.mkdir(); resultPaths.add(f);});
        ResultHandler rh = new ResultHandler(targetHosts, resultPaths, queryDir);
        ResultHandler.ComparableResultSet res = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res2 = createResultSet(10, 5, false);
        ResultHandler.ComparableResultSet res3 = createResultSet(10, 10, false);
        List<ResultHandler.ComparableResultSet> toCompare = Lists.newArrayList(res, res2, res3);
        rh.handleResults(new FQLQuery.Single("aaa", 3, QueryOptions.DEFAULT, 123123, 11111, 22222, "select * from abcabc", Collections.emptyList()), toCompare);
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results1 = readResultFile(resultPaths.get(0), queryDir);
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results2 = readResultFile(resultPaths.get(1), queryDir);
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results3 = readResultFile(resultPaths.get(2), queryDir);
        assertEquals(results1, results3);
        assertEquals(results2.get(0).right, res2);
    }

    @Test
    public void testResultHandlerMultipleResultSets() throws IOException
    {
        List<String> targetHosts = Lists.newArrayList("hosta", "hostb", "hostc");
        File tmpDir = Files.createTempDirectory("testresulthandler").toFile();
        File queryDir = Files.createTempDirectory("queries").toFile();
        List<File> resultPaths = new ArrayList<>();
        targetHosts.forEach(host -> { File f = new File(tmpDir, host); f.mkdir(); resultPaths.add(f);});
        ResultHandler rh = new ResultHandler(targetHosts, resultPaths, queryDir);
        List<Pair<FQLQuery, List<ResultHandler.ComparableResultSet>>> resultSets = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 10; i++)
        {
            List<ResultHandler.ComparableResultSet> results = new ArrayList<>();
            List<ByteBuffer> values = Collections.singletonList(ByteBufferUtil.bytes(i * 50));
            for (int jj = 0; jj < targetHosts.size(); jj++)
            {
                results.add(createResultSet(5, 1 + random.nextInt(10), true));
            }
            FQLQuery q = new FQLQuery.Single("abc"+i,
                                             3,
                                             QueryOptions.forInternalCalls(values),
                                             i * 1000,
                                             12345,
                                             54321,
                                             "select * from xyz where id = "+i,
                                             values);
            resultSets.add(Pair.create(q, results));
        }
        for (int i = 0; i < resultSets.size(); i++)
            rh.handleResults(resultSets.get(i).left, resultSets.get(i).right);

        for (int i = 0; i < targetHosts.size(); i++)
            compareWithFile(resultPaths, queryDir, resultSets, i);
    }

    @Test
    public void testResultHandlerFailedQuery() throws IOException
    {
        List<String> targetHosts = Lists.newArrayList("hosta", "hostb", "hostc", "hostd");
        File tmpDir = Files.createTempDirectory("testresulthandler").toFile();
        File queryDir = Files.createTempDirectory("queries").toFile();
        List<File> resultPaths = new ArrayList<>();
        targetHosts.forEach(host -> { File f = new File(tmpDir, host); f.mkdir(); resultPaths.add(f);});
        ResultHandler rh = new ResultHandler(targetHosts, resultPaths, queryDir);
        List<Pair<FQLQuery, List<ResultHandler.ComparableResultSet>>> resultSets = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 10; i++)
        {
            List<ResultHandler.ComparableResultSet> results = new ArrayList<>();
            List<ByteBuffer> values = Collections.singletonList(ByteBufferUtil.bytes(i * 50));
            for (int jj = 0; jj < targetHosts.size(); jj++)
            {
                results.add(createResultSet(5, 1 + random.nextInt(10), true));
            }
            results.set(0, FakeResultSet.failed(new RuntimeException("testing abc")));
            results.set(3, FakeResultSet.failed(new RuntimeException("testing abc")));
            FQLQuery q = new FQLQuery.Single("abc"+i,
                                             3,
                                             QueryOptions.forInternalCalls(values),
                                             i * 1000,
                                             i * 12345,
                                             i * 54321,
                                             "select * from xyz where id = "+i,
                                             values);
            resultSets.add(Pair.create(q, results));
        }
        for (int i = 0; i < resultSets.size(); i++)
            rh.handleResults(resultSets.get(i).left, resultSets.get(i).right);

        for (int i = 0; i < targetHosts.size(); i++)
            compareWithFile(resultPaths, queryDir, resultSets, i);
    }


    @Test
    public void testCompare()
    {
        FQLQuery q1 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 123, 111, 222, "aaaa", Collections.emptyList());
        FQLQuery q2 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 123, 111, 222,"aaaa", Collections.emptyList());

        assertEquals(0, q1.compareTo(q2));
        assertEquals(0, q2.compareTo(q1));

        FQLQuery q3 = new FQLQuery.Batch("abc", 0, QueryOptions.DEFAULT, 123, 111, 222, com.datastax.driver.core.BatchStatement.Type.UNLOGGED, Collections.emptyList(), Collections.emptyList());
        // single queries before batch queries
        assertTrue(q1.compareTo(q3) < 0);
        assertTrue(q3.compareTo(q1) > 0);

        // check that smaller query time
        FQLQuery q4 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 124, 111, 222, "aaaa", Collections.emptyList());
        assertTrue(q1.compareTo(q4) < 0);
        assertTrue(q4.compareTo(q1) > 0);

        FQLQuery q5 = new FQLQuery.Batch("abc", 0, QueryOptions.DEFAULT, 124, 111, 222, com.datastax.driver.core.BatchStatement.Type.UNLOGGED, Collections.emptyList(), Collections.emptyList());
        assertTrue(q1.compareTo(q5) < 0);
        assertTrue(q5.compareTo(q1) > 0);

        FQLQuery q6 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 123, 111, 222, "aaaa", Collections.singletonList(ByteBufferUtil.bytes(10)));
        FQLQuery q7 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 123, 111, 222, "aaaa", Collections.emptyList());
        assertTrue(q6.compareTo(q7) > 0);
        assertTrue(q7.compareTo(q6) < 0);

        FQLQuery q8 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 123, 111, 222, "aaaa", Collections.singletonList(ByteBufferUtil.bytes("a")));
        FQLQuery q9 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 123, 111, 222, "aaaa", Collections.singletonList(ByteBufferUtil.bytes("b")));
        assertTrue(q8.compareTo(q9) < 0);
        assertTrue(q9.compareTo(q8) > 0);
    }

    private File generateQueries(int count, boolean random) throws IOException
    {
        Random r = new Random();
        File dir = Files.createTempDirectory("chronicle").toFile();
        try (ChronicleQueue readQueue = ChronicleQueueBuilder.single(dir).build())
        {
            ExcerptAppender appender = readQueue.acquireAppender();

            for (int i = 0; i < count; i++)
            {
                long timestamp = random ? Math.abs(r.nextLong() % 10000) : i;
                if (random ? r.nextBoolean() : i % 2 == 0)
                {
                    String query = "abcdefghijklm " + i;
                    QueryState qs = r.nextBoolean() ? queryState() : queryState("querykeyspace");
                    FullQueryLogger.Query  q = new FullQueryLogger.Query(query, QueryOptions.DEFAULT, qs, timestamp);
                    appender.writeDocument(q);
                    q.release();
                }
                else
                {
                    int batchSize = random ? r.nextInt(99) + 1 : i + 1;
                    List<String> queries = new ArrayList<>(batchSize);
                    List<List<ByteBuffer>> values = new ArrayList<>(batchSize);
                    for (int jj = 0; jj < (random ? r.nextInt(batchSize) : 10); jj++)
                    {
                        queries.add("aaaaaa batch "+i+":"+jj);
                        values.add(Collections.emptyList());
                    }
                    FullQueryLogger.Batch batch = new FullQueryLogger.Batch(BatchStatement.Type.UNLOGGED,
                                                                            queries,
                                                                            values,
                                                                            QueryOptions.DEFAULT,
                                                                            queryState("someks"),
                                                                            timestamp);
                    appender.writeDocument(batch);
                    batch.release();
                }
            }
        }
        return dir;
    }

    private QueryState queryState()
    {
        return QueryState.forInternalCalls();
    }

    private QueryState queryState(String keyspace)
    {
        ClientState clientState = ClientState.forInternalCalls(keyspace);
        return new QueryState(clientState);
    }

    private static ResultHandler.ComparableResultSet createResultSet(int columnCount, int rowCount, boolean random)
    {
        List<Pair<String, String>> columnDefs = new ArrayList<>(columnCount);
        Random r = new Random();
        for (int i = 0; i < columnCount; i++)
        {
            columnDefs.add(Pair.create("a" + i, "int"));
        }
        List<List<String>> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++)
        {
            List<String> row = new ArrayList<>(columnCount);
            for (int jj = 0; jj < columnCount; jj++)
                row.add(i + " col " + jj + (random ? r.nextInt() : ""));
            rows.add(row);
        }
        return new FakeResultSet(columnDefs, rows);
    }

    private static void compareWithFile(List<File> dirs, File resultDir, List<Pair<FQLQuery, List<ResultHandler.ComparableResultSet>>> resultSets, int idx)
    {
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results1 = readResultFile(dirs.get(idx), resultDir);
        for (int i = 0; i < results1.size(); i++)
        {
            assertEquals(results1.get(i).left, resultSets.get(i).left);
            assertEquals(results1.get(i).right, resultSets.get(i).right.get(idx));
        }
    }

    private static List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> readResultFile(File dir, File queryDir)
    {
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> resultSets = new ArrayList<>();
        try (ChronicleQueue q = ChronicleQueueBuilder.single(dir).build();
             ChronicleQueue queryQ = ChronicleQueueBuilder.single(queryDir).build())
        {
            ExcerptTailer tailer = q.createTailer();
            ExcerptTailer queryTailer = queryQ.createTailer();
            List<Pair<String, String>> columnDefinitions = new ArrayList<>();
            List<List<String>> rowColumns = new ArrayList<>();
            AtomicBoolean allRowsRead = new AtomicBoolean(false);
            AtomicBoolean failedQuery = new AtomicBoolean(false);
            while (tailer.readDocument(wire -> {
                String type = wire.read("type").text();
                if (type.equals("column_definitions"))
                {
                    int columnCount = wire.read("column_count").int32();
                    for (int i = 0; i < columnCount; i++)
                    {
                        ValueIn vi = wire.read("column_definition");
                        String name = vi.text();
                        String dataType = vi.text();
                        columnDefinitions.add(Pair.create(name, dataType));
                    }
                }
                else if (type.equals("row"))
                {
                    int rowColumnCount = wire.read("row_column_count").int32();
                    List<String> r = new ArrayList<>(rowColumnCount);
                    for (int i = 0; i < rowColumnCount; i++)
                    {
                        byte[] b = wire.read("column").bytes();
                        r.add(new String(b));
                    }
                    rowColumns.add(r);
                }
                else if (type.equals("end_resultset"))
                {
                    allRowsRead.set(true);
                }
                else if (type.equals("query_failed"))
                {
                    failedQuery.set(true);
                }
            }))
            {
                if (allRowsRead.get())
                {
                    FQLQueryReader reader = new FQLQueryReader();
                    queryTailer.readDocument(reader);
                    resultSets.add(Pair.create(reader.getQuery(), failedQuery.get() ? FakeResultSet.failed(new RuntimeException("failure"))
                                                                                    : new FakeResultSet(ImmutableList.copyOf(columnDefinitions), ImmutableList.copyOf(rowColumns))));
                    allRowsRead.set(false);
                    failedQuery.set(false);
                    columnDefinitions.clear();
                    rowColumns.clear();
                }
            }
        }
        return resultSets;
    }

    private static class FakeResultSet implements ResultHandler.ComparableResultSet
    {
        private final List<Pair<String, String>> cdStrings;
        private final List<List<String>> rows;
        private final Throwable ex;

        public FakeResultSet(List<Pair<String, String>> cdStrings, List<List<String>> rows)
        {
            this(cdStrings, rows, null);
        }

        public FakeResultSet(List<Pair<String, String>> cdStrings, List<List<String>> rows, Throwable ex)
        {
            this.cdStrings = cdStrings;
            this.rows = rows;
            this.ex = ex;
        }

        public static FakeResultSet failed(Throwable ex)
        {
            return new FakeResultSet(null, null, ex);
        }

        public ResultHandler.ComparableColumnDefinitions getColumnDefinitions()
        {
            return new FakeComparableColumnDefinitions(cdStrings, wasFailed());
        }

        public boolean wasFailed()
        {
            return getFailureException() != null;
        }

        public Throwable getFailureException()
        {
            return ex;
        }

        public Iterator<ResultHandler.ComparableRow> iterator()
        {
            if (wasFailed())
                return Collections.emptyListIterator();
            return new AbstractIterator<ResultHandler.ComparableRow>()
            {
                Iterator<List<String>> iter = rows.iterator();
                protected ResultHandler.ComparableRow computeNext()
                {
                    if (iter.hasNext())
                        return new FakeComparableRow(iter.next(), cdStrings);
                    return endOfData();
                }
            };
        }

        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof FakeResultSet)) return false;
            FakeResultSet that = (FakeResultSet) o;
            if (wasFailed() && that.wasFailed())
                return true;
            return Objects.equals(cdStrings, that.cdStrings) &&
                   Objects.equals(rows, that.rows);
        }

        public int hashCode()
        {
            return Objects.hash(cdStrings, rows);
        }

        public String toString()
        {
            return "FakeResultSet{" +
                   "cdStrings=" + cdStrings +
                   ", rows=" + rows +
                   '}';
        }
    }

    private static class FakeComparableRow implements ResultHandler.ComparableRow
    {
        private final List<String> row;
        private final List<Pair<String, String>> cds;

        public FakeComparableRow(List<String> row, List<Pair<String,String>> cds)
        {
            this.row = row;
            this.cds = cds;
        }

        public ByteBuffer getBytesUnsafe(int i)
        {
            return ByteBufferUtil.bytes(row.get(i));
        }

        public ResultHandler.ComparableColumnDefinitions getColumnDefinitions()
        {
            return new FakeComparableColumnDefinitions(cds, false);
        }

        public boolean equals(Object other)
        {
            if (!(other instanceof FakeComparableRow))
                return false;
            return row.equals(((FakeComparableRow)other).row);
        }

        public String toString()
        {
            return row.toString();
        }
    }

    private static class FakeComparableColumnDefinitions implements ResultHandler.ComparableColumnDefinitions
    {
        private final List<ResultHandler.ComparableDefinition> defs;
        private final boolean failed;
        public FakeComparableColumnDefinitions(List<Pair<String, String>> cds, boolean failed)
        {
            defs = cds != null ? cds.stream().map(FakeComparableDefinition::new).collect(Collectors.toList()) : null;
            this.failed = failed;
        }

        public List<ResultHandler.ComparableDefinition> asList()
        {
            if (wasFailed())
                return Collections.emptyList();
            return defs;
        }

        public boolean wasFailed()
        {
            return failed;
        }

        public int size()
        {
            return defs.size();
        }

        public Iterator<ResultHandler.ComparableDefinition> iterator()
        {
            if (wasFailed())
                return Collections.emptyListIterator();
            return new AbstractIterator<ResultHandler.ComparableDefinition>()
            {
                Iterator<ResultHandler.ComparableDefinition> iter = defs.iterator();
                protected ResultHandler.ComparableDefinition computeNext()
                {
                    if (iter.hasNext())
                        return iter.next();
                    return endOfData();
                }
            };
        }
        public boolean equals(Object other)
        {
            if (!(other instanceof FakeComparableColumnDefinitions))
                return false;
            return defs.equals(((FakeComparableColumnDefinitions)other).defs);
        }

        public String toString()
        {
            return defs.toString();
        }
    }

    private static class FakeComparableDefinition implements ResultHandler.ComparableDefinition
    {
        private final Pair<String, String> p;

        public FakeComparableDefinition(Pair<String, String> p)
        {
            this.p = p;
        }
        public String getType()
        {
            return p.right;
        }

        public String getName()
        {
            return p.left;
        }

        public boolean equals(Object other)
        {
            if (!(other instanceof FakeComparableDefinition))
                return false;
            return p.equals(((FakeComparableDefinition)other).p);
        }

        public String toString()
        {
            return getName() + ':' + getType();
        }
    }
}
