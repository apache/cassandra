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
package org.apache.cassandra.index.sai.cql;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.service.reads.thresholds.CoordinatorWarnings;

/**
 * Compares the latency of {@code LIKE 'prefix%'} served by the SAI prefix index against the same query served by a
 * full-table {@code ALLOW FILTERING} scan (the behaviour without the prefix feature), for both a selective prefix
 * (a small subset of rows) and a non-selective prefix (every row).
 */
public class PrefixSearchStressTest extends SAITester
{
    private static final Logger logger = LoggerFactory.getLogger(PrefixSearchStressTest.class);

    private static final int ROWS = 20_000;
    private static final int GROUPS = 100;                   // 100 distinct prefixes
    private static final int ROWS_PER_GROUP = ROWS / GROUPS; // 200 rows per prefix
    private static final int QUERY_ITERATIONS = 5;

    // A selective prefix matches one group (200 rows, 1%); the broad prefix matches everything.
    private static final String SELECTIVE = "grp42x";  // depth 6 (eligible for postings_skip=3), 200 rows
    private static final String BROAD = "grp";         // depth 3 (eligible), all 20,000 rows

    @Test
    public void compareWithAndWithoutPrefixSearch() throws Throwable
    {
        // ---- WITH the SAI prefix index ----
        createTable("CREATE TABLE %s (id int PRIMARY KEY, name text)");
        createIndex("CREATE INDEX ON %s(name) USING 'sai' WITH OPTIONS = {'enable_literal_prefix_sai': 'true'}");
        populate();
        flush();

        long withSelective = timeQuery("SELECT id FROM %s WHERE name LIKE '" + SELECTIVE + "%%'", ROWS_PER_GROUP);
        long withBroad = timeQuery("SELECT id FROM %s WHERE name LIKE '" + BROAD + "%%'", ROWS);

        // ---- WITHOUT the prefix feature: a regular SAI index, LIKE served by an ALLOW FILTERING full scan ----
        createTable("CREATE TABLE %s (id int PRIMARY KEY, name text)");
        createIndex("CREATE INDEX ON %s(name) USING 'sai'");
        populate();
        flush();

        long withoutSelective = timeQuery("SELECT id FROM %s WHERE name LIKE '" + SELECTIVE + "%%' ALLOW FILTERING", ROWS_PER_GROUP);
        long withoutBroad = timeQuery("SELECT id FROM %s WHERE name LIKE '" + BROAD + "%%' ALLOW FILTERING", ROWS);

        String summary = String.format(
            "%n==================== SAI prefix-search stress ====================%n" +
            "  dataset: %,d rows, %d prefixes of %,d rows each, best of %d runs%n" +
            "  query                              | WITH prefix idx | WITHOUT (filter) | speedup%n" +
            "  -----------------------------------+-----------------+------------------+--------%n" +
            "  LIKE '%s%%' (selective, %,4d rows)  | %,12d us | %,13d us | %5.1fx%n" +
            "  LIKE '%s%%'    (broad, %,6d rows)  | %,12d us | %,13d us | %5.1fx%n" +
            "==================================================================",
            ROWS, GROUPS, ROWS_PER_GROUP, QUERY_ITERATIONS,
            SELECTIVE, ROWS_PER_GROUP, withSelective, withoutSelective, ratio(withoutSelective, withSelective),
            BROAD, ROWS, withBroad, withoutBroad, ratio(withoutBroad, withBroad));
        logger.info(summary);
        System.out.println(summary);
    }

    private static double ratio(long without, long with)
    {
        return without / Math.max(1.0, (double) with);
    }

    private void populate() throws Throwable
    {
        // name = "grpNN" + 'x' + zero-padded id. Group NN = id % GROUPS, so each of the 100 prefixes "grpNNx" has
        // ROWS_PER_GROUP rows. "grpNNx" is 6 chars (an eligible depth for postings_skip=3) with 200 rows
        // (>= minimum_postings_leaves=64), so a selective prefix gets an aggregated section (the read fast path).
        for (int i = 0; i < ROWS; i++)
            execute("INSERT INTO %s (id, name) VALUES (?, ?)", i, String.format("grp%02dx%08d", i % GROUPS, i));
    }

    /** Runs the query {@link #QUERY_ITERATIONS} times and returns the best (minimum) wall time in microseconds. */
    private long timeQuery(String cql, int expectedRows) throws Throwable
    {
        long best = Long.MAX_VALUE;
        for (int i = 0; i < QUERY_ITERATIONS; i++)
        {
            long start = System.nanoTime();
            UntypedResultSet result = runQuery(cql);
            long elapsedUs = (System.nanoTime() - start) / 1_000;
            assertRowCount(result, expectedRows);
            best = Math.min(best, elapsedUs);
        }
        return best;
    }

    /** Executes a read with the coordinator-warning lifecycle initialised (large result sets trip it otherwise). */
    private UntypedResultSet runQuery(String cql) throws Throwable
    {
        CoordinatorWarnings.init();
        try
        {
            return execute(cql);
        }
        finally
        {
            CoordinatorWarnings.reset();
        }
    }
}
