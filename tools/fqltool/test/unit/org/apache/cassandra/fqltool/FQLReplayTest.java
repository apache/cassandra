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

package org.apache.cassandra.fqltool;

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
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import org.junit.Test;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import net.openhft.chronicle.core.io.IORuntimeException;
import net.openhft.chronicle.queue.ChronicleQueue;
import net.openhft.chronicle.queue.impl.single.SingleChronicleQueueBuilder;
import net.openhft.chronicle.queue.ExcerptAppender;
import net.openhft.chronicle.queue.ExcerptTailer;
import net.openhft.chronicle.wire.WireOut;
import org.apache.cassandra.fql.FullQueryLogger;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.statements.BatchStatement;
import org.apache.cassandra.fqltool.commands.Compare;
import org.apache.cassandra.fqltool.commands.Replay;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MergeIterator;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.binlog.BinLog;

import static org.apache.cassandra.fqltool.QueryReplayer.ParsedTargetHost.fromString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(f).build();
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
    public void testQueryIterator() throws IOException
    {
        File f = generateQueries(100, false);
        int queryCount = 0;
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(f).build();
             FQLQueryIterator iter = new FQLQueryIterator(queue.createTailer(), 1))
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
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(f).build();
             ChronicleQueue queue2 = SingleChronicleQueueBuilder.single(f2).build();
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

        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(generateQueries(1000, true)).build())
        {
            ExcerptTailer tailer = queue.createTailer();
            int queryCount = 0;
            while (tailer.readDocument(reader))
            {
                assertNotNull(reader.getQuery());
                if (reader.getQuery() instanceof FQLQuery.Single)
                {
                    assertTrue(reader.getQuery().keyspace() == null || reader.getQuery().keyspace().equals("querykeyspace"));
                }
                else
                {
                    assertEquals("someks", reader.getQuery().keyspace());
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
        FQLQuery query = new FQLQuery.Single("abc", QueryOptions.DEFAULT.getProtocolVersion().asInt(), QueryOptions.DEFAULT, 12345, 11111, 22, "select * from abc", Collections.emptyList());
        try
        {
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

        compareResults(Collections.singletonList(Pair.create(query, res)),
                       readResultFile(tmpDir, queryDir));

    }

    private static void compareResults(List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> expected,
                                List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> other)
    {
        ResultComparator comparator = new ResultComparator();
        assertEquals(expected.size(), other.size());
        for (int i = 0; i < expected.size(); i++)
        {
            assertEquals(expected.get(i).left, other.get(i).left);
            ResultHandler.ComparableResultSet expectedResultSet = expected.get(i).right;
            ResultHandler.ComparableResultSet otherResultSet = other.get(i).right;
            List<String> hosts = Lists.newArrayList("a", "b");
            comparator.compareColumnDefinitions(hosts,
                                                expected.get(i).left,
                                                Lists.newArrayList(expectedResultSet.getColumnDefinitions(),
                                                                   otherResultSet.getColumnDefinitions()));
            Iterator<ResultHandler.ComparableRow> expectedRowIter = expectedResultSet.iterator();
            Iterator<ResultHandler.ComparableRow> otherRowIter = otherResultSet.iterator();


            while(expectedRowIter.hasNext() && otherRowIter.hasNext())
            {
                ResultHandler.ComparableRow expectedRow = expectedRowIter.next();
                ResultHandler.ComparableRow otherRow = otherRowIter.next();
                assertTrue(comparator.compareRows(hosts, expected.get(i).left, Lists.newArrayList(expectedRow, otherRow)));
            }
            assertFalse(expectedRowIter.hasNext());
            assertFalse(otherRowIter.hasNext());
        }
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

        ResultHandler.ComparableResultSet res = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res2 = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res3 = createResultSet(10, 10, false);
        List<ResultHandler.ComparableResultSet> toCompare = Lists.newArrayList(res, res2, res3);
        FQLQuery query = new FQLQuery.Single("abcabc", QueryOptions.DEFAULT.getProtocolVersion().asInt(), QueryOptions.DEFAULT, 1111, 2222, 3333, "select * from xyz", Collections.emptyList());
        try (ResultHandler rh = new ResultHandler(targetHosts, resultPaths, queryDir))
        {
            rh.handleResults(query, toCompare);
        }
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results1 = readResultFile(resultPaths.get(0), queryDir);
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results2 = readResultFile(resultPaths.get(1), queryDir);
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results3 = readResultFile(resultPaths.get(2), queryDir);
        compareResults(results1, results2);
        compareResults(results1, results3);
        compareResults(results3, Collections.singletonList(Pair.create(query, res)));
    }

    @Test
    public void testResultHandlerWithDifference() throws IOException
    {
        List<String> targetHosts = Lists.newArrayList("hosta", "hostb", "hostc");
        File tmpDir = Files.createTempDirectory("testresulthandler").toFile();
        File queryDir = Files.createTempDirectory("queries").toFile();
        List<File> resultPaths = new ArrayList<>();
        targetHosts.forEach(host -> { File f = new File(tmpDir, host); f.mkdir(); resultPaths.add(f);});

        ResultHandler.ComparableResultSet res = createResultSet(10, 10, false);
        ResultHandler.ComparableResultSet res2 = createResultSet(10, 5, false);
        ResultHandler.ComparableResultSet res3 = createResultSet(10, 10, false);
        List<ResultHandler.ComparableResultSet> toCompare = Lists.newArrayList(res, res2, res3);
        FQLQuery query = new FQLQuery.Single("aaa", QueryOptions.DEFAULT.getProtocolVersion().asInt(), QueryOptions.DEFAULT, 123123, 11111, 22222, "select * from abcabc", Collections.emptyList());
        try (ResultHandler rh = new ResultHandler(targetHosts, resultPaths, queryDir))
        {
            rh.handleResults(query, toCompare);
        }
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results1 = readResultFile(resultPaths.get(0), queryDir);
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results2 = readResultFile(resultPaths.get(1), queryDir);
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results3 = readResultFile(resultPaths.get(2), queryDir);
        compareResults(results1, results3);
        compareResults(results2, Collections.singletonList(Pair.create(query, res2)));
    }

    @Test
    public void testResultHandlerMultipleResultSets() throws IOException
    {
        List<String> targetHosts = Lists.newArrayList("hosta", "hostb", "hostc");
        File tmpDir = Files.createTempDirectory("testresulthandler").toFile();
        File queryDir = Files.createTempDirectory("queries").toFile();
        List<File> resultPaths = new ArrayList<>();
        targetHosts.forEach(host -> { File f = new File(tmpDir, host); f.mkdir(); resultPaths.add(f);});
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
            FQLQuery q = i % 2 == 0
                         ? new FQLQuery.Single("abc"+i,
                                             3,
                                             QueryOptions.forInternalCalls(values),
                                             i * 1000,
                                             12345,
                                             54321,
                                             "select * from xyz where id = "+i,
                                             values)
                         : new FQLQuery.Batch("abc"+i,
                                              3,
                                              QueryOptions.forInternalCalls(values),
                                              i * 1000,
                                              i * 54321,
                                              i * 12345,
                                              com.datastax.driver.core.BatchStatement.Type.UNLOGGED,
                                              Lists.newArrayList("select * from aaaa"),
                                              Collections.singletonList(values));

            resultSets.add(Pair.create(q, results));
        }
        try (ResultHandler rh = new ResultHandler(targetHosts, resultPaths, queryDir))
        {
            for (int i = 0; i < resultSets.size(); i++)
                rh.handleResults(resultSets.get(i).left, resultSets.get(i).right);
        }

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
            results.set(0, StoredResultSet.failed("testing abc"));
            results.set(3, StoredResultSet.failed("testing abc"));
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
        try (ResultHandler rh = new ResultHandler(targetHosts, resultPaths, queryDir))
        {
            for (int i = 0; i < resultSets.size(); i++)
                rh.handleResults(resultSets.get(i).left, resultSets.get(i).right);
        }
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

    @Test
    public void testFQLQuerySingleToStatement()
    {
        List<ByteBuffer> values = new ArrayList<>();
        for (int i = 0; i < 10; i++)
            values.add(ByteBufferUtil.bytes(i));
        FQLQuery.Single single = new FQLQuery.Single("xyz",
                                                     QueryOptions.DEFAULT.getProtocolVersion().asInt(),
                                                     QueryOptions.forInternalCalls(values),
                                                     1234,
                                                     12345,
                                                     54321,
                                                     "select * from aaa",
                                                     values);
        Statement stmt = single.toStatement();
        assertEquals(stmt.getDefaultTimestamp(), 12345);
        assertTrue(stmt instanceof SimpleStatement);
        SimpleStatement simpleStmt = (SimpleStatement)stmt;
        assertEquals("select * from aaa",simpleStmt.getQueryString(CodecRegistry.DEFAULT_INSTANCE));
        assertArrayEquals(values.toArray(), simpleStmt.getValues(com.datastax.driver.core.ProtocolVersion.fromInt(QueryOptions.DEFAULT.getProtocolVersion().asInt()), CodecRegistry.DEFAULT_INSTANCE));
    }


    @Test
    public void testFQLQueryBatchToStatement()
    {
        List<List<ByteBuffer>> values = new ArrayList<>();
        List<String> queries = new ArrayList<>();
        for (int bqCount = 0; bqCount < 10; bqCount++)
        {
            queries.add("select * from asdf where x = ? and y = " + bqCount);
            List<ByteBuffer> queryValues = new ArrayList<>();
            for (int i = 0; i < 10; i++)
                queryValues.add(ByteBufferUtil.bytes(i + ":" + bqCount));
            values.add(queryValues);
        }

        FQLQuery.Batch batch = new FQLQuery.Batch("xyz",
                                                   QueryOptions.DEFAULT.getProtocolVersion().asInt(),
                                                   QueryOptions.DEFAULT,
                                                   1234,
                                                   12345,
                                                   54321,
                                                   com.datastax.driver.core.BatchStatement.Type.UNLOGGED,
                                                   queries,
                                                   values);
        Statement stmt = batch.toStatement();
        assertEquals(stmt.getDefaultTimestamp(), 12345);
        assertTrue(stmt instanceof com.datastax.driver.core.BatchStatement);
        com.datastax.driver.core.BatchStatement batchStmt = (com.datastax.driver.core.BatchStatement)stmt;
        List<Statement> statements = Lists.newArrayList(batchStmt.getStatements());
        List<Statement> fromFQLQueries = batch.queries.stream().map(FQLQuery.Single::toStatement).collect(Collectors.toList());
        assertEquals(statements.size(), fromFQLQueries.size());
        assertEquals(12345, batchStmt.getDefaultTimestamp());
        for (int i = 0; i < statements.size(); i++)
            compareStatements(statements.get(i), fromFQLQueries.get(i));
    }

    @Test
    public void testParser() {
        QueryReplayer.ParsedTargetHost pth;
        pth = fromString("127.0.0.1");
        assertEquals("127.0.0.1", pth.host);
        assertEquals(9042, pth.port );
        assertNull(pth.user);
        assertNull(pth.password);

        pth = fromString("127.0.0.1:3333");
        assertEquals("127.0.0.1", pth.host);
        assertEquals(3333, pth.port );
        assertNull(pth.user);
        assertNull(pth.password);

        pth = fromString("aaa:bbb@127.0.0.1:3333");
        assertEquals("127.0.0.1", pth.host);
        assertEquals(3333, pth.port );
        assertEquals("aaa", pth.user);
        assertEquals("bbb", pth.password);

        pth = fromString("aaa:bbb@127.0.0.1");
        assertEquals("127.0.0.1", pth.host);
        assertEquals(9042, pth.port );
        assertEquals("aaa", pth.user);
        assertEquals("bbb", pth.password);
    }

    @Test(expected = RuntimeException.class)
    public void testNoPass()
    {
        fromString("blabla@abc.com:1234");
    }

    @Test(expected = RuntimeException.class)
    public void testBadPort()
    {
        fromString("aaa:bbb@abc.com:xyz");
    }

    @Test (expected = IORuntimeException.class)
    public void testFutureVersion() throws Exception
    {
        FQLQueryReader reader = new FQLQueryReader();
        File dir = Files.createTempDirectory("chronicle").toFile();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(dir).build())
        {
            ExcerptAppender appender = queue.acquireAppender();
            appender.writeDocument(new BinLog.ReleaseableWriteMarshallable() {
                protected long version()
                {
                    return 999;
                }

                protected String type()
                {
                    return FullQueryLogger.SINGLE_QUERY;
                }

                public void writeMarshallablePayload(WireOut wire)
                {
                    wire.write("future-field").text("future_value");
                }

                public void release()
                {

                }
            });

            ExcerptTailer tailer = queue.createTailer();
            tailer.readDocument(reader);
        }
        catch (Exception e)
        {
            assertTrue(e.getMessage().contains("Unsupported record version"));
            throw e;
        }

    }

    @Test (expected = IORuntimeException.class)
    public void testUnknownRecord() throws Exception
    {
        FQLQueryReader reader = new FQLQueryReader();
        File dir = Files.createTempDirectory("chronicle").toFile();
        try (ChronicleQueue queue = SingleChronicleQueueBuilder.single(dir).build())
        {
            ExcerptAppender appender = queue.acquireAppender();
            appender.writeDocument(new BinLog.ReleaseableWriteMarshallable() {
                protected long version()
                {
                    return FullQueryLogger.CURRENT_VERSION;
                }

                protected String type()
                {
                    return "unknown-type";
                }

                public void writeMarshallablePayload(WireOut wire)
                {
                    wire.write("unknown-field").text("unknown_value");
                }

                public void release()
                {

                }
            });

            ExcerptTailer tailer = queue.createTailer();
            tailer.readDocument(reader);
        }
        catch (Exception e)
        {
            assertTrue(e.getMessage().contains("Unsupported record type field"));
            throw e;
        }

    }

    private void compareStatements(Statement statement1, Statement statement2)
    {
        assertTrue(statement1 instanceof SimpleStatement && statement2 instanceof SimpleStatement);
        SimpleStatement simpleStmt1 = (SimpleStatement)statement1;
        SimpleStatement simpleStmt2 = (SimpleStatement)statement2;
        assertEquals(simpleStmt1.getQueryString(CodecRegistry.DEFAULT_INSTANCE), simpleStmt2.getQueryString(CodecRegistry.DEFAULT_INSTANCE));
        assertArrayEquals(simpleStmt1.getValues(com.datastax.driver.core.ProtocolVersion.fromInt(QueryOptions.DEFAULT.getProtocolVersion().asInt()), CodecRegistry.DEFAULT_INSTANCE),
                          simpleStmt2.getValues(com.datastax.driver.core.ProtocolVersion.fromInt(QueryOptions.DEFAULT.getProtocolVersion().asInt()), CodecRegistry.DEFAULT_INSTANCE));

    }

    private File generateQueries(int count, boolean random) throws IOException
    {
        Random r = new Random();
        File dir = Files.createTempDirectory("chronicle").toFile();
        try (ChronicleQueue readQueue = SingleChronicleQueueBuilder.single(dir).build())
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

    static ResultHandler.ComparableResultSet createResultSet(int columnCount, int rowCount, boolean random)
    {
        List<Pair<String, String>> columnDefs = new ArrayList<>(columnCount);
        Random r = new Random();
        for (int i = 0; i < columnCount; i++)
        {
            columnDefs.add(Pair.create("a" + i, "int"));
        }
        ResultHandler.ComparableColumnDefinitions colDefs = new StoredResultSet.StoredComparableColumnDefinitions(columnDefs, false, null);
        List<ResultHandler.ComparableRow> rows = new ArrayList<>();
        for (int i = 0; i < rowCount; i++)
        {
            List<ByteBuffer> row = new ArrayList<>(columnCount);
            for (int jj = 0; jj < columnCount; jj++)
                row.add(ByteBufferUtil.bytes(i + " col " + jj + (random ? r.nextInt() : "")));

            rows.add(new StoredResultSet.StoredComparableRow(row, colDefs));
        }
        return new StoredResultSet(colDefs, true, false, null, rows::iterator);
    }

    private static void compareWithFile(List<File> dirs, File resultDir, List<Pair<FQLQuery, List<ResultHandler.ComparableResultSet>>> resultSets, int idx)
    {
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> results1 = readResultFile(dirs.get(idx), resultDir);
        for (int i = 0; i < resultSets.size(); i++)
        {
            FQLQuery query = resultSets.get(i).left;
            List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> toCompare = Collections.singletonList(Pair.create(query, resultSets.get(i).right.get(idx)));
            compareResults(Collections.singletonList(results1.get(i)), toCompare);
        }
    }

    private static List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> readResultFile(File dir, File queryDir)
    {
        List<Pair<FQLQuery, ResultHandler.ComparableResultSet>> resultSets = new ArrayList<>();
        try (ChronicleQueue q = SingleChronicleQueueBuilder.single(dir).build();
             ChronicleQueue queryQ = SingleChronicleQueueBuilder.single(queryDir).build())
        {
            ExcerptTailer queryTailer = queryQ.createTailer();
            FQLQueryReader queryReader = new FQLQueryReader();
            Compare.StoredResultSetIterator resultSetIterator = new Compare.StoredResultSetIterator(q.createTailer());
            // we need to materialize the rows in-memory to compare them easier in these tests
            while (resultSetIterator.hasNext())
            {
                ResultHandler.ComparableResultSet resultSetFromDisk = resultSetIterator.next();
                Iterator<ResultHandler.ComparableRow> rowIterFromDisk = resultSetFromDisk.iterator();
                queryTailer.readDocument(queryReader);

                FQLQuery query = queryReader.getQuery();
                List<ResultHandler.ComparableRow> rows = new ArrayList<>();
                while (rowIterFromDisk.hasNext())
                {
                    rows.add(rowIterFromDisk.next());
                }
                resultSets.add(Pair.create(query, new StoredResultSet(resultSetFromDisk.getColumnDefinitions(),
                                                                      resultSetIterator.hasNext(),
                                                                      resultSetFromDisk.wasFailed(),
                                                                      resultSetFromDisk.getFailureException(),
                                                                      rows::iterator)));
            }
        }
        return resultSets;
    }
}
