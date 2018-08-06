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

import io.netty.buffer.ByteBuf;
import net.openhft.chronicle.bytes.BytesStore;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.ChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.ValueIn;
import org.apache.cassandra.audit.FullQueryLogger;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.tools.fqltool.commands.Replay;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Pair;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class FQLReplayTest
{
    @Test
    public void testOrderedReplay() throws IOException
    {
        File f = generateQueries(100, true);
        int queryCount = 0;
        try (ChronicleQueue queue = ChronicleQueueBuilder.single(f).build();
             FQLQueryIterator iter = new FQLQueryIterator(queue.createTailer(), 101, false))
        {
            long last = -1;
            while (iter.hasNext())
            {
                FQLQuery q = iter.next();
                assertTrue(q.queryTime >= last);
                last = q.queryTime;
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
             FQLQueryIterator iter = new FQLQueryIterator(queue.createTailer(), 101, false);
             FQLQueryIterator iter2 = new FQLQueryIterator(queue2.createTailer(), 101, false);
             MergeIterator<FQLQuery, List<FQLQuery>> merger = MergeIterator.get(Lists.newArrayList(iter, iter2), FQLQuery::compareTo, new Replay.Reducer()))
        {
            long last = -1;

            while (merger.hasNext())
            {
                List<FQLQuery> qs = merger.next();
                assertEquals(2, qs.size());
                assertEquals(0, qs.get(0).compareTo(qs.get(1)));
                assertTrue(qs.get(0).queryTime >= last);
                last = qs.get(0).queryTime;
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
                    assertTrue(reader.getQuery().keyspace == null || reader.getQuery().keyspace.equals("somekeyspace"));
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

        ResultHandler.ComparableResultSet res = createResultSet(10, 10, true);
        ResultStore rs = new ResultStore(Collections.singletonList(tmpDir));
        try
        {
            rs.storeColumnDefinitions(Collections.singletonList(res.getColumnDefinitions()));
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

        List<ResultHandler.ComparableResultSet> resultSets = readResultFile(tmpDir);
        assertEquals(1, resultSets.size());
        assertEquals(res, resultSets.get(0));

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
        List<File> resultPaths = new ArrayList<>();
        targetHosts.forEach(host -> { File f = new File(tmpDir, host); f.mkdir(); resultPaths.add(f);});
        ResultHandler rh = new ResultHandler(targetHosts, resultPaths);
        ResultHandler.ComparableResultSet res = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res2 = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res3 = createResultSet(10, 10, false);
        List<ResultHandler.ComparableResultSet> toCompare = Lists.newArrayList(res, res2, res3);
        rh.handle(null, toCompare);
        List<ResultHandler.ComparableResultSet> results1 = readResultFile(resultPaths.get(0));
        List<ResultHandler.ComparableResultSet> results2 = readResultFile(resultPaths.get(1));
        List<ResultHandler.ComparableResultSet> results3 = readResultFile(resultPaths.get(2));
        assertEquals(results1, results2);
        assertEquals(results1, results3);
        assertEquals(Iterables.getOnlyElement(results3), res);
    }


    @Test
    public void testResultHandlerWithDifference() throws IOException
    {
        List<String> targetHosts = Lists.newArrayList("hosta", "hostb", "hostc");
        File tmpDir = Files.createTempDirectory("testresulthandler").toFile();
        List<File> resultPaths = new ArrayList<>();
        targetHosts.forEach(host -> { File f = new File(tmpDir, host); f.mkdir(); resultPaths.add(f);});
        ResultHandler rh = new ResultHandler(targetHosts, resultPaths);
        ResultHandler.ComparableResultSet res = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res2 = createResultSet(10, 5, false);
        ResultHandler.ComparableResultSet res3 = createResultSet(10, 10, false);
        List<ResultHandler.ComparableResultSet> toCompare = Lists.newArrayList(res, res2, res3);
        rh.handle(null, toCompare);
        List<ResultHandler.ComparableResultSet> results1 = readResultFile(resultPaths.get(0));
        List<ResultHandler.ComparableResultSet> results2 = readResultFile(resultPaths.get(1));
        List<ResultHandler.ComparableResultSet> results3 = readResultFile(resultPaths.get(2));
        assertEquals(results1, results3);
        assertEquals(results2.get(0), res2);
    }


    @Test
    public void testResultHandlerMultipleResultSets() throws IOException
    {
        List<String> targetHosts = Lists.newArrayList("hosta", "hostb", "hostc");
        File tmpDir = Files.createTempDirectory("testresulthandler").toFile();
        List<File> resultPaths = new ArrayList<>();
        targetHosts.forEach(host -> { File f = new File(tmpDir, host); f.mkdir(); resultPaths.add(f);});
        ResultHandler rh = new ResultHandler(targetHosts, resultPaths);
        List<List<ResultHandler.ComparableResultSet>> resultSets = new ArrayList<>();
        Random random = new Random();
        for (int i = 0; i < 10; i++)
        {
            List<ResultHandler.ComparableResultSet> results = new ArrayList<>();
            for (int jj = 0; jj < targetHosts.size(); jj++)
                results.add(createResultSet(5, 1 + random.nextInt(10), true));
            resultSets.add(results);
        }

        for (int i = 0; i < resultSets.size(); i++)
            rh.handle(null, resultSets.get(i));

        for (int i = 0; i < targetHosts.size(); i++)
            compareWithFile(resultPaths, resultSets, i);
    }

    @Test
    public void testCompare()
    {
        FQLQuery q1 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 123, "aaaa", Collections.emptyList());
        FQLQuery q2 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 123, "aaaa", Collections.emptyList());

        assertEquals(0, q1.compareTo(q2));
        assertEquals(0, q2.compareTo(q1));

        FQLQuery q3 = new FQLQuery.Batch("abc", 0, QueryOptions.DEFAULT, 123, com.datastax.driver.core.BatchStatement.Type.UNLOGGED, Collections.emptyList(), Collections.emptyList());
        // single queries before batch queries
        assertTrue(q1.compareTo(q3) < 0);
        assertTrue(q3.compareTo(q1) > 0);

        // check that smaller query time
        FQLQuery q4 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 124, "aaaa", Collections.emptyList());
        assertTrue(q1.compareTo(q4) < 0);
        assertTrue(q4.compareTo(q1) > 0);

        FQLQuery q5 = new FQLQuery.Batch("abc", 0, QueryOptions.DEFAULT, 124, com.datastax.driver.core.BatchStatement.Type.UNLOGGED, Collections.emptyList(), Collections.emptyList());
        assertTrue(q1.compareTo(q5) < 0);
        assertTrue(q5.compareTo(q1) > 0);

        FQLQuery q6 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 123, "aaaa", Collections.singletonList(ByteBufferUtil.bytes(10)));
        FQLQuery q7 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 123, "aaaa", Collections.emptyList());
        assertTrue(q6.compareTo(q7) > 0);
        assertTrue(q7.compareTo(q6) < 0);

        FQLQuery q8 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 123, "aaaa", Collections.singletonList(ByteBufferUtil.bytes("a")));
        FQLQuery q9 = new FQLQuery.Single("abc", 0, QueryOptions.DEFAULT, 123, "aaaa", Collections.singletonList(ByteBufferUtil.bytes("b")));
        assertTrue(q8.compareTo(q9) < 0);
        assertTrue(q9.compareTo(q8) > 0);
    }

    @Test
    public void testLegacyFiles() throws IOException
    {
        // make sure we can read pre-CASSANDRA-14656 files
        File dir = Files.createTempDirectory("legacy").toFile();
        try (ChronicleQueue queue = ChronicleQueueBuilder.single(dir).build())
        {
            ExcerptAppender appender = queue.acquireAppender();
            int optionsSize = QueryOptions.codec.encodedSize(QueryOptions.DEFAULT, ProtocolVersion.CURRENT);
            ByteBuf queryOptionsBuffer = CBUtil.allocator.buffer(optionsSize, optionsSize);
            QueryOptions.codec.encode(QueryOptions.DEFAULT, queryOptionsBuffer, ProtocolVersion.CURRENT);
            for (int i = 0; i < 10; i++)
            {
                appender.writeDocument(wire ->
                                       {
                                           wire.write("type").text("single");
                                           wire.write("protocol-version").int32(ProtocolVersion.CURRENT.asInt());
                                           wire.write("query-options").bytes(BytesStore.wrap(queryOptionsBuffer.nioBuffer()));
                                           wire.write("query-time").int64(123456);
                                           // don't write keyspace
                                           wire.write("query").text("Q U E R Y");
                                       });
            }
        }

        try (ChronicleQueue queue = ChronicleQueueBuilder.single(dir).readOnly(true).build();
             FQLQueryIterator iter = new FQLQueryIterator(queue.createTailer(), 10, true))
        {
            int queryCount = 0;
            while (iter.hasNext())
            {
                FQLQuery query = iter.next();
                assertNull(query.keyspace);
                assertEquals(123456, query.queryTime);
                assertEquals("Q U E R Y", ((FQLQuery.Single)query).query);
                queryCount++;
            }
            assertEquals(10, queryCount);
        }
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
                    FullQueryLogger.WeighableMarshallableQuery q = new FullQueryLogger.WeighableMarshallableQuery(query, (random && r.nextBoolean()) ? "somekeyspace" : null, QueryOptions.DEFAULT, timestamp);
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
                    FullQueryLogger.WeighableMarshallableBatch batch = new FullQueryLogger.WeighableMarshallableBatch(BatchStatement.Type.UNLOGGED.toString(),
                                                                                                                      "someks",
                                                                                                                      queries,
                                                                                                                      values,
                                                                                                                      QueryOptions.DEFAULT,
                                                                                                                      timestamp);
                    appender.writeDocument(batch);
                    batch.release();
                }
            }
        }
        return dir;
    }

    private ResultHandler.ComparableResultSet createResultSet(int columnCount, int rowCount, boolean random)
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

    private void compareWithFile(List<File> dirs, List<List<ResultHandler.ComparableResultSet>> resultSets, int idx)
    {
        List<ResultHandler.ComparableResultSet> results1 = readResultFile(dirs.get(idx));

        for (int i = 0; i < results1.size(); i++)
        {
            assertEquals(results1.get(i), resultSets.get(i).get(idx));
        }
    }

    private List<ResultHandler.ComparableResultSet> readResultFile(File dir)
    {
        List<ResultHandler.ComparableResultSet> resultSets = new ArrayList<>();
        try (ChronicleQueue q = ChronicleQueueBuilder.single(dir).build())
        {
            ExcerptTailer tailer = q.createTailer();
            List<Pair<String, String>> columnDefinitions = new ArrayList<>();
            List<List<String>> rowColumns = new ArrayList<>();
            AtomicBoolean allRowsRead = new AtomicBoolean(false);
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
            }))
            {
                if (allRowsRead.get())
                {
                    resultSets.add(new FakeResultSet(ImmutableList.copyOf(columnDefinitions), ImmutableList.copyOf(rowColumns)));
                    allRowsRead.set(false);
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


        public FakeResultSet(List<Pair<String, String>> cdStrings, List<List<String>> rows)
        {
            this.cdStrings = cdStrings;
            this.rows = rows;
        }

        public ResultHandler.ComparableColumnDefinitions getColumnDefinitions()
        {
            return new FakeComparableColumnDefinitions(cdStrings);
        }

        public Iterator<ResultHandler.ComparableRow> iterator()
        {
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
            return new FakeComparableColumnDefinitions(cds);
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
        public FakeComparableColumnDefinitions(List<Pair<String, String>> cds)
        {
            defs = cds.stream().map(FakeComparableDefinition::new).collect(Collectors.toList());

        }
        public List<ResultHandler.ComparableDefinition> asList()
        {
            return defs;
        }

        public int size()
        {
            return defs.size();
        }

        public Iterator<ResultHandler.ComparableDefinition> iterator()
        {
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
