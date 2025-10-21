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
package org.apache.cassandra.cql3;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.SchemaKeyspaceTables;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.MD5Digest;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;

import static java.util.Collections.emptyMap;
import static org.apache.cassandra.service.QueryState.forInternalCalls;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(BMUnitRunner.class)
public class PstmtPersistenceTest extends CQLTester
{
    private static final CompletableFuture<?>[] futureArray = new CompletableFuture[0];

    private static final ConcurrentMap<MD5Digest, Long> preparedStatementLoadTimestamps = new ConcurrentHashMap<>();
    private static final ConcurrentMap<MD5Digest, Long> preparedStatementRemoveTimestamps = new ConcurrentHashMap<>();

    // page size passed to preloadPreparedStatements
    private static final int PRELOAD_PAGE_SIZE = 100;

    // recorded page invocations in preloadPreparedStatements
    private static final AtomicInteger pageInvocations = new AtomicInteger();

    @Before
    public void setUp()
    {
        preparedStatementLoadTimestamps.clear();
        preparedStatementRemoveTimestamps.clear();

        QueryProcessor.clearPreparedStatements(false);
    }

    @Test
    public void testCachedPreparedStatements() throws Throwable
    {
        // need this for pstmt execution/validation tests
        requireNetwork();

        assertEquals(0, numberOfStatementsOnDisk());

        execute("CREATE KEYSPACE IF NOT EXISTS foo WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}");
        execute("CREATE TABLE foo.bar (key text PRIMARY KEY, val int)");

        ClientState clientState = ClientState.forExternalCalls(InetSocketAddress.createUnresolved("127.0.0.1", 1234));

        createTable("CREATE TABLE %s (pk int PRIMARY KEY, val text)");

        List<MD5Digest> stmtIds = new ArrayList<>();
        String statement0 = "SELECT * FROM %s WHERE keyspace_name = ?";
        String statement1 = "SELECT * FROM %s WHERE pk = ?";
        String statement2 = "SELECT * FROM %s WHERE key = ?";
        String statement3 = "SELECT * FROM %S WHERE key = ?";
        stmtIds.add(prepareStatement(statement0, SchemaConstants.SCHEMA_KEYSPACE_NAME, SchemaKeyspaceTables.TABLES, clientState));
        stmtIds.add(prepareStatement(statement1, clientState));
        stmtIds.add(prepareStatement(statement2, "foo", "bar", clientState));
        clientState.setKeyspace("foo");

        stmtIds.add(prepareStatement(statement1, clientState));
        stmtIds.add(prepareStatement(statement3, "foo", "bar", clientState));

        assertEquals(5, stmtIds.size());
        // statement1 will have two statements prepared because of `setKeyspace` usage
        assertEquals(6, QueryProcessor.preparedStatementsCount());
        assertEquals(6, numberOfStatementsOnDisk());

        QueryHandler handler = ClientState.getCQLQueryHandler();
        validatePstmts(stmtIds, handler);

        // clear prepared statements cache
        QueryProcessor.clearPreparedStatements(true);
        assertEquals(0, QueryProcessor.preparedStatementsCount());
        for (MD5Digest stmtId : stmtIds)
            Assert.assertNull(handler.getPrepared(stmtId));

        // load prepared statements and validate that these still execute fine
        QueryProcessor.instance.preloadPreparedStatements();
        validatePstmts(stmtIds, handler);


        // validate that the prepared statements are in the system table
        String queryAll = "SELECT * FROM " + SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PREPARED_STATEMENTS;
        for (UntypedResultSet.Row row : QueryProcessor.executeOnceInternal(queryAll))
        {
            MD5Digest digest = MD5Digest.wrap(ByteBufferUtil.getArray(row.getBytes("prepared_id")));
            QueryProcessor.Prepared prepared = QueryProcessor.instance.getPrepared(digest);
            Assert.assertNotNull(prepared);
        }

        // add another prepared statement and sync it to table
        prepareStatement(statement2, "foo", "bar", clientState);

        // statement1 will have two statements prepared because of `setKeyspace` usage
        assertEquals(7, numberOfStatementsInMemory());
        assertEquals(7, numberOfStatementsOnDisk());

        // drop a keyspace (prepared statements are removed - syncPreparedStatements() remove should the rows, too)
        execute("DROP KEYSPACE foo");
        assertEquals(3, numberOfStatementsInMemory());
        assertEquals(3, numberOfStatementsOnDisk());
    }

    private void validatePstmts(List<MD5Digest> stmtIds, QueryHandler handler)
    {
        QueryOptions optionsStr = QueryOptions.forInternalCalls(Collections.singletonList(UTF8Type.instance.fromString("foobar")));
        QueryOptions optionsInt = QueryOptions.forInternalCalls(Collections.singletonList(Int32Type.instance.decompose(42)));
        validatePstmt(handler, stmtIds.get(0), optionsStr);
        validatePstmt(handler, stmtIds.get(1), optionsInt);
        validatePstmt(handler, stmtIds.get(2), optionsStr);
        validatePstmt(handler, stmtIds.get(3), optionsInt);
        validatePstmt(handler, stmtIds.get(4), optionsStr);
    }

    private static void validatePstmt(QueryHandler handler, MD5Digest stmtId, QueryOptions options)
    {
        QueryProcessor.Prepared prepared = handler.getPrepared(stmtId);
        Assert.assertNotNull(prepared);
        handler.processPrepared(prepared.statement, forInternalCalls(), options, emptyMap(), Dispatcher.RequestTime.forImmediateExecution());
    }

    @Test
    public void testPstmtInvalidation() throws Throwable
    {
        ClientState clientState = ClientState.forInternalCalls();

        createTable("CREATE TABLE %s (key int primary key, val int)");

        long initialEvicted = numberOfEvictedStatements();

        for (int cnt = 1; cnt < 10000; cnt++)
        {
            prepareStatement("INSERT INTO %s (key, val) VALUES (?, ?) USING TIMESTAMP " + cnt, clientState);

            if (numberOfEvictedStatements() - initialEvicted > 0)
            {
                assertEquals("Number of statements in table and in cache don't match", numberOfStatementsInMemory(), numberOfStatementsOnDisk());

                // prepare more statements to trigger more evictions
                for (int cnt2 = cnt + 1; cnt2 < cnt + 10; cnt2++)
                    prepareStatement("INSERT INTO %s (key, val) VALUES (?, ?) USING TIMESTAMP " + cnt2, clientState);

                // each new prepared statement should have caused an eviction
                assertEquals("eviction count didn't increase by the expected number", 10, numberOfEvictedStatements() - initialEvicted);
                assertEquals("Number of statements in memory (expected) and table (actual) don't match", numberOfStatementsInMemory(), numberOfStatementsOnDisk());

                return;
            }
        }

        fail("Prepared statement eviction does not work");
    }

    @Test
    @BMRules(rules= {
             @BMRule(name = "CaptureWriteTimestamps",
                     targetClass = "SystemKeyspace",
                     targetMethod = "writePreparedStatement(String, MD5Digest, String, long)",
                     targetLocation = "AT INVOKE executeInternal",
                     action = "org.apache.cassandra.cql3.PstmtPersistenceTest.preparedStatementLoadTimestamps.put($key, $timestamp);"
             ),
             @BMRule(name = "CaptureEvictTimestamps",
                     targetClass = "QueryProcessor",
                     targetMethod = "evictPreparedStatement(MD5Digest, RemovalCause)",
                     action = "org.apache.cassandra.cql3.PstmtPersistenceTest.preparedStatementRemoveTimestamps.put($key, org.apache.cassandra.service.ClientState.getTimestamp());"
             )
    })
    public void testAsyncPstmtInvalidation() throws Throwable
    {
        ClientState clientState = ClientState.forInternalCalls();
        createTable("CREATE TABLE %s (key int primary key, val int)");

        // prepare statements concurrently in a thread pool to exercise bug encountered in CASSANDRA-19703 where
        // delete from table occurs before the insert due to early eviction.
        final ExecutorService executor = Executors.newFixedThreadPool(10);

        long initialEvicted = numberOfEvictedStatements();
        try
        {
            int initialMaxStatementsToPrepare = 10000;
            int maxStatementsToPrepare = initialMaxStatementsToPrepare;
            boolean hasEvicted = false;
            int concurrency = 100;
            List<CompletableFuture<MD5Digest>> prepareFutures = new ArrayList<>(concurrency);

            for (int cnt = 1; cnt <= maxStatementsToPrepare; cnt++)
            {
                final int localCnt = cnt;
                prepareFutures.add(CompletableFuture.supplyAsync(() -> prepareStatement("INSERT INTO %s (key, val) VALUES (?, ?) USING TIMESTAMP " + localCnt, clientState), executor));

                if (prepareFutures.size() == concurrency)
                {
                    // Await completion of current inflight futures
                    CompletableFuture.allOf(prepareFutures.toArray(futureArray)).get(10, TimeUnit.SECONDS);
                    prepareFutures.clear();
                }

                // Once we've detected evictions, prepare as many statements as we've prepared so far to initialMaxStatementsToPrepare and then stop.
                if (!hasEvicted && numberOfEvictedStatements() - initialEvicted > 0)
                {
                    maxStatementsToPrepare = Math.min(cnt * 2, initialMaxStatementsToPrepare);
                    hasEvicted = true;
                }
            }

            long evictedStatements = numberOfEvictedStatements() - initialEvicted;
            assertNotEquals("Should have evicted some prepared statements", 0, evictedStatements);

            // Recorded prepared statement removals should match metrics
            assertEquals("Actual evicted statements does not match metrics", evictedStatements, preparedStatementRemoveTimestamps.size());

            // For each prepared statement evicted, assert the time it was deleted is greater than the timestamp
            // used for when it was loaded.
            for (Map.Entry<MD5Digest, Long> evictedStatementEntry : preparedStatementRemoveTimestamps.entrySet())
            {
                MD5Digest key = evictedStatementEntry.getKey();
                long deletionTimestamp = evictedStatementEntry.getValue();
                long insertionTimestamp = preparedStatementLoadTimestamps.get(key);

                assertTrue(String.format("Expected deletion timestamp for prepared statement (%d) to be greater than insertion timestamp (%d)",
                                         deletionTimestamp, insertionTimestamp),
                           deletionTimestamp > insertionTimestamp);
            }

            // ensure the number of statements on disk match the number in memory, if number of statements on disk eclipses in memory, there was a leak.
            assertEquals("Number of statements in memory (expected) and table (actual) don't match", numberOfStatementsInMemory(), numberOfStatementsOnDisk());
        }
        finally
        {
            executor.shutdown();
        }
    }

    /**
     * Invoked whenever paging happens in testPreloadPreparedStatements, increments PAGE_INVOCATIONS when we detect
     * paging happening in the path of QueryProcessor.preloadPreparedStatements with the expected page size.
     */
    @SuppressWarnings("unused")
    private static void nextPageReadQuery(ReadQuery query, int pageSize)
    {
        TableMetadata metadata = query.metadata();
        if (metadata.keyspace.equals(SchemaConstants.SYSTEM_KEYSPACE_NAME) &&
            metadata.name.equals(SystemKeyspace.PREPARED_STATEMENTS) &&
            pageSize == PRELOAD_PAGE_SIZE)
        {
            for (StackTraceElement stackTraceElement : Thread.currentThread().getStackTrace())
            {
                if (stackTraceElement.getClassName().equals(QueryProcessor.class.getName()) && stackTraceElement.getMethodName().equals("preloadPreparedStatements"))
                {
                    pageInvocations.incrementAndGet();
                    return;
                }
            }
        }
    }

    @Test
    @BMRule(name = "CapturePageInvocations",
            targetClass = "PartitionRangeQueryPager",
            targetMethod = "nextPageReadQuery(int)",
            action = "org.apache.cassandra.cql3.PstmtPersistenceTest.nextPageReadQuery($this.query, $pageSize)")
    public void testPreloadPreparedStatements() throws Throwable
    {
        ClientState clientState = ClientState.forInternalCalls();
        createTable("CREATE TABLE %s (key int primary key, val int)");

        // Prepare more statements than the paging size to ensure paging works properly.
        int statementsToPrepare = 750;

        for (int cnt = 1; cnt <= statementsToPrepare; cnt++)
        {
            prepareStatement("INSERT INTO %s (key, val) VALUES (?, ?) USING TIMESTAMP " + cnt, clientState);
        }

        // Capture how many statements are in memory before clearing cache.
        long statementsInMemory = numberOfStatementsInMemory();
        long statementsOnDisk = numberOfStatementsOnDisk();
        assertEquals(statementsOnDisk, statementsInMemory);

        // Drop prepared statements from cache only and ensure the cache empties out.
        QueryProcessor.clearPreparedStatements(true);
        assertEquals(0, numberOfStatementsInMemory());

        // Load prepared statements and ensure the cache size matches max
        QueryProcessor.instance.preloadPreparedStatements(PRELOAD_PAGE_SIZE);

        long statementsInMemoryAfterLoading = numberOfStatementsInMemory();
        // Ensure size of cache matches statements that were on disk before preload
        assertEquals("Statements prepared - evicted (expected) does not match statements in memory (actual)",
                     statementsOnDisk, statementsInMemoryAfterLoading);

        // Number of statements on disk shold match memory
        assertEquals(statementsInMemoryAfterLoading, numberOfStatementsOnDisk());

        // Ensure only executed the expected amount of pages.
        int expectedPageInvocations = (int) Math.ceil(statementsInMemoryAfterLoading / (double) PRELOAD_PAGE_SIZE);
        assertEquals(expectedPageInvocations, pageInvocations.get());
    }

    @Test
    public void testPreloadPreparedStatementsUntilCacheFull()
    {
        QueryHandler handler = ClientState.getCQLQueryHandler();
        ClientState clientState = ClientState.forInternalCalls();
        createTable("CREATE TABLE %s (key int primary key, val int)");

        // Fill up and clear the prepared statement cache several times to load up the system.prepared_statements table.
        // This simulates a 'leak' of prepared statements akin to CASSANDRA-19703 as the system.prepared_statements
        // table is able to grow to a larger size than the in memory prepared statement cache.  In such a case we
        // should detect a possible leak and defer paging indefinitely by returning early in preloadPreparedStatements.
        int statementsLoadedWhenFull = -1;
        long accumulatedSize = 0;
        // load enough prepared statements to fill the cache 5 times.
        for (int cnt = 0; accumulatedSize < QueryProcessor.PREPARED_STATEMENT_CACHE_SIZE_BYTES * 5; cnt++)
        {
            MD5Digest id = prepareStatement("INSERT INTO %s (key, val) VALUES (?, ?) USING TIMESTAMP " + cnt, clientState);
            QueryHandler.Prepared prepared = handler.getPrepared(id);
            assertTrue(prepared.pstmntSize > -1);
            accumulatedSize += prepared.pstmntSize;
            if (statementsLoadedWhenFull == -1 && accumulatedSize > QueryProcessor.PREPARED_STATEMENT_CACHE_SIZE_BYTES)
            {
                statementsLoadedWhenFull = cnt;
            }
            // clear cache repeatedly to avoid eviction.
            QueryProcessor.clearPreparedStatements(true);
        }


        int preloadedStatements = QueryProcessor.instance.preloadPreparedStatements(PRELOAD_PAGE_SIZE);

        // Should have loaded as many statements as we detected were loaded before cache would be full.
        assertTrue(String.format("Preloaded %d statements, expected at least %d",
                                 preloadedStatements, statementsLoadedWhenFull),
                   preloadedStatements > statementsLoadedWhenFull);

        // We should only expect to load how many statements we were able to load before filling the cache
        // + a buffer of 110%, set to 1.5x just to deal with sensitivity of detecting cache filling up.
        int atMostPreloadedExpected = (int) (statementsLoadedWhenFull * 1.5);
        assertTrue(String.format("Preloaded %d statements, but only expected that we'd load at most %d",
                                 preloadedStatements, atMostPreloadedExpected),
                   preloadedStatements <= atMostPreloadedExpected);
    }

    private long numberOfStatementsOnDisk() throws Throwable
    {
        UntypedResultSet.Row row = execute("SELECT COUNT(*) FROM " + SchemaConstants.SYSTEM_KEYSPACE_NAME + '.' + SystemKeyspace.PREPARED_STATEMENTS).one();
        return row.getLong("count");
    }

    private long numberOfStatementsInMemory()
    {
        return QueryProcessor.preparedStatementsCount();
    }

    private long numberOfEvictedStatements()
    {
        return QueryProcessor.metrics.preparedStatementsEvicted.getCount();
    }

    private MD5Digest prepareStatement(String stmt, ClientState clientState)
    {
        return prepareStatement(stmt, keyspace(), currentTable(), clientState);
    }

    private MD5Digest prepareStatement(String stmt, String keyspace, String table, ClientState clientState)
    {
        return QueryProcessor.instance.prepare(String.format(stmt, keyspace + '.' + table), clientState).statementId;
    }
}
