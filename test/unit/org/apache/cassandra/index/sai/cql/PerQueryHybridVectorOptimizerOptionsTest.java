/*
 * Copyright IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.index.sai.cql;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.index.sai.plan.Plan;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end integration tests for per-query SAI optimizer options ({@code WITH optimizer_options = {...}}).
 *
 * <p>This class checks how the options change the query plan and results. Parsing, validation and
 * {@code RowFilter} wiring are covered by {@link OptimizerOptionsTest}.
 *
 * <p>Covers all four option keys across real query execution:
 * <ul>
 *   <li>{@code query_optimization_level} — disables/enables the optimizer for a single query.</li>
 *   <li>{@code intersection_clause_limit} — limits the number of intersected index clauses.</li>
 *   <li>{@code use_term_statistics} — controls which selectivity estimator is used.</li>
 *   <li>{@code hybrid_sort_order} — overrides the optimizer's filter-then-sort / sort-then-filter
 *       plan decision for hybrid queries.</li>
 * </ul>
 *
 * <p>The {@code hybrid_sort_order} option is tested across all three hybrid query flavours:
 * <ul>
 *   <li><b>ANN</b>: filter-then-sort uses {@link Plan.NumericIndexScan};
 *       sort-then-filter uses {@link Plan.AnnIndexScan}.</li>
 *   <li><b>BM25</b>: filter-then-sort uses {@link Plan.NumericIndexScan};
 *       sort-then-filter uses {@link Plan.Bm25IndexScan}.</li>
 *   <li><b>Generic ORDER BY</b>: filter-then-sort uses {@link Plan.NumericIndexScan};
 *       sort-then-filter uses {@link Plan.LiteralIndexScan}.</li>
 * </ul>
 */
public class PerQueryHybridVectorOptimizerOptionsTest extends VectorTester
{
    /**
     * Creates a shared table for hybrid sort-order tests.
     * <p>
     * Data layout: {@code n=0} → 2 rows (selective WHERE); {@code n>=0} → 20 rows (non-selective WHERE).
     */
    @Before
    public void createHybridTable()
    {
        createTable("CREATE TABLE %s (k int, c int, v vector<float, 2>, n int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX vec_idx ON %s(v) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'similarity_function': 'euclidean'}");
        createIndex("CREATE CUSTOM INDEX num_idx ON %s(n) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 20; i++)
            execute("INSERT INTO %s (k, c, v, n) VALUES (0, ?, ?, ?)",
                    i,
                    vector(0f, (float) i),
                    i < 2 ? 0 : 1); // n=0 for rows 0,1 only
    }

    // -----------------------------------------------------------------------
    // sai_hybrid_sort_order = sort_then_filter
    // -----------------------------------------------------------------------

    /**
     * When the WHERE predicate is very selective (n=0 matches 2/20 rows), the optimizer normally
     * chooses filter-then-sort: it materialises keys from the numeric index, then scores them with ANN.
     * {@code sort_then_filter} must override this and produce an {@link Plan.AnnIndexScan} plan instead,
     * while still returning the correct rows.
     */
    @Test
    public void testSortThenFilterOverridesSelectiveWhereClause()
    {
        // Sanity: without override, the optimizer picks filter-then-sort for n=0 (selective).
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5",
                              Plan.NumericIndexScan.class,
                              row(0), row(1));

        // With sort_then_filter override, the plan must use AnnIndexScan (sort-then-filter),
        // and the query must still return the correct rows.
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'}",
                Plan.AnnIndexScan.class,
                row(0), row(1));
    }

    /**
     * When the plan is already sort-then-filter (non-selective WHERE), the {@code sort_then_filter}
     * override is a no-op: the plan must remain an {@link Plan.AnnIndexScan}.
     */
    @Test
    public void testSortThenFilterIsNoOpWhenAlreadySortThenFilter()
    {
        // n>=0 is non-selective: optimizer chooses sort-then-filter by default.
        assertQueryHasSubplan("SELECT c FROM %s WHERE n >= 0 ORDER BY v ANN OF [0, 0] LIMIT 5",
                              Plan.AnnIndexScan.class,
                              row(0), row(1), row(2), row(3), row(4));

        // Override should be a no-op.
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n >= 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'}",
                Plan.AnnIndexScan.class,
                row(0), row(1), row(2), row(3), row(4));
    }

    // -----------------------------------------------------------------------
    // sai_hybrid_sort_order = filter_then_sort
    // -----------------------------------------------------------------------

    /**
     * When the WHERE predicate is non-selective (n>=0 matches all 20 rows), the optimizer normally
     * chooses sort-then-filter: it streams from the ANN index.
     * {@code filter_then_sort} must override this and produce a {@link Plan.NumericIndexScan} plan
     * (filter-then-sort), while still returning correct results.
     */
    @Test
    public void testFilterThenSortOverridesNonSelectiveWhereClauseANN()
    {
        // Sanity: without override, the optimizer picks sort-then-filter for n>=0 (non-selective).
        assertQueryHasSubplan("SELECT c FROM %s WHERE n >= 0 ORDER BY v ANN OF [0, 0] LIMIT 5",
                              Plan.AnnIndexScan.class,
                              row(0), row(1), row(2), row(3), row(4));

        // With filter_then_sort override, the plan must use NumericIndexScan (filter-then-sort),
        // and the query must still return the correct rows.
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n >= 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}",
                Plan.NumericIndexScan.class,
                row(0), row(1), row(2), row(3), row(4));
    }

    /**
     * When the plan is already filter-then-sort (selective WHERE), the {@code filter_then_sort}
     * override is a no-op: the plan must remain a {@link Plan.NumericIndexScan}.
     */
    @Test
    public void testFilterThenSortIsNoOpWhenAlreadyFilterThenSort()
    {
        // n=0 is selective: optimizer already chooses filter-then-sort.
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5",
                              Plan.NumericIndexScan.class,
                              row(0), row(1));

        // Override should be a no-op.
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}",
                Plan.NumericIndexScan.class,
                row(0), row(1));
    }

    // -----------------------------------------------------------------------
    // sai_hybrid_sort_order = auto
    // -----------------------------------------------------------------------

    @Test
    public void testAutoPreservesOptimizerDecision()
    {
        // auto must not change anything — same plans as without the option.
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'auto'}",
                Plan.NumericIndexScan.class,
                row(0), row(1));

        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n >= 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'auto'}",
                Plan.AnnIndexScan.class,
                row(0), row(1), row(2), row(3), row(4));
    }

    // -----------------------------------------------------------------------
    // sai_query_optimization_level — end-to-end result correctness
    // -----------------------------------------------------------------------

    @Test
    public void testOptLevelZeroQueryReturnsCorrectResults()
    {
        // opt_level=0 must disable the optimizer but still return correct results.
        var results = execute("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                              "WITH optimizer_options = {'query_optimization_level': '0'}");
        assertThat(results.size()).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // sai_intersection_clause_limit — end-to-end result correctness
    // -----------------------------------------------------------------------

    @Test
    public void testIntersectionClauseLimitQueryReturnsCorrectResults()
    {
        // A limit of 1 restricts to a single indexed clause; the query must still return results.
        var results = execute("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                              "WITH optimizer_options = {'intersection_clause_limit': '1'}");
        assertThat(results.size()).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // sai_use_term_statistics — end-to-end result correctness
    // -----------------------------------------------------------------------

    @Test
    public void testUseTermStatisticsFalseQueryReturnsCorrectResults()
    {
        var results = execute("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                              "WITH optimizer_options = {'use_term_statistics': 'false'}");
        assertThat(results.size()).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // Combined options — all four at once
    // -----------------------------------------------------------------------

    @Test
    public void testAllOptionsTogetherReturnCorrectResults()
    {
        var results = execute("SELECT c FROM %s WHERE n = 0 ORDER BY v ANN OF [0, 0] LIMIT 5 " +
                              "WITH optimizer_options = {" +
                              "'query_optimization_level': '1', " +
                              "'intersection_clause_limit': '3', " +
                              "'use_term_statistics': 'true', " +
                              "'hybrid_sort_order': 'sort_then_filter'" +
                              "}");
        assertThat(results.size()).isEqualTo(2);
    }

    // -----------------------------------------------------------------------
    // hybrid_sort_order — BM25 hybrid queries
    //
    // Table layout (same selectivity split used by ANN tests):
    //   n=0  → 2 rows  (selective WHERE)
    //   n>=0 → 10 rows (non-selective WHERE)
    // 'apple' appears in all 10 rows, so the BM25 index is never more selective
    // than the numeric index; the optimizer's default mirrors the ANN case.
    // -----------------------------------------------------------------------

    /**
     * BM25 + selective WHERE: the optimizer picks filter-then-sort ({@link Plan.NumericIndexScan}).
     * {@code sort_then_filter} must flip this to sort-then-filter ({@link Plan.Bm25IndexScan}).
     */
    @Test
    public void testSortThenFilterOverridesSelectiveWhereClauseBM25()
    {
        createTable("CREATE TABLE %s (k int, c int, s text, n int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'index_analyzer': 'standard'}");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, s, n) VALUES (0, ?, 'apple', ?)", i, i < 2 ? 0 : 1);

        // Default: selective WHERE → filter-then-sort (NumericIndexScan).
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY s BM25 OF 'apple' LIMIT 5",
                              Plan.NumericIndexScan.class,
                              row(0), row(1));

        // Override forces sort-then-filter (Bm25IndexScan), results unchanged.
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n = 0 ORDER BY s BM25 OF 'apple' LIMIT 5 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'}",
                Plan.Bm25IndexScan.class,
                row(0), row(1));
    }

    /**
     * BM25 + non-selective WHERE: the optimizer picks sort-then-filter ({@link Plan.Bm25IndexScan}).
     * {@code filter_then_sort} must flip this to filter-then-sort ({@link Plan.NumericIndexScan}).
     */
    @Test
    public void testFilterThenSortOverridesNonSelectiveWhereClauseBM25()
    {
        createTable("CREATE TABLE %s (k int, c int, s text, n int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = {'index_analyzer': 'standard'}");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, s, n) VALUES (0, ?, 'apple', ?)", i, i < 2 ? 0 : 1);

        // Default: non-selective WHERE → sort-then-filter (Bm25IndexScan).
        assertQueryHasSubplan("SELECT c FROM %s WHERE n >= 0 ORDER BY s BM25 OF 'apple' LIMIT 5",
                              Plan.Bm25IndexScan.class,
                              row(0), row(1), row(2), row(3), row(4));

        // Override forces filter-then-sort (NumericIndexScan), results unchanged.
        assertThatPlanFor("SELECT c FROM %s WHERE n >= 0 ORDER BY s BM25 OF 'apple' LIMIT 5 " +
                          "WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}",
                          row(0), row(1), row(2), row(3), row(4)).
                uses(Plan.NumericIndexScan.class, Plan.LiteralIndexScan.class);
    }

    // -----------------------------------------------------------------------
    // hybrid_sort_order — generic ORDER BY hybrid queries
    //
    // Same table/data layout as the BM25 section above; the ordering index
    // is a plain (non-analyzed) literal index on column 's'.
    // -----------------------------------------------------------------------

    /**
     * Generic ORDER BY + selective WHERE: the optimizer picks filter-then-sort
     * ({@link Plan.NumericIndexScan}).
     * {@code sort_then_filter} must flip this to sort-then-filter ({@link Plan.LiteralIndexScan}).
     */
    @Test
    public void testSortThenFilterOverridesSelectiveWhereClauseGenericOrderBy()
    {
        createTable("CREATE TABLE %s (k int, c int, s text, n int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, s, n) VALUES (0, ?, ?, ?)",
                    i, String.valueOf((char) ('a' + i)), i < 2 ? 0 : 1);

        // Default: selective WHERE → filter-then-sort (NumericIndexScan).
        assertQueryHasSubplan("SELECT c FROM %s WHERE n = 0 ORDER BY s ASC LIMIT 5",
                              Plan.NumericIndexScan.class,
                              row(0), row(1));

        // Override forces sort-then-filter (LiteralIndexScan), results unchanged.
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n = 0 ORDER BY s ASC LIMIT 5 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'sort_then_filter'}",
                Plan.LiteralIndexScan.class,
                row(0), row(1));
    }

    /**
     * Generic ORDER BY + non-selective WHERE: the optimizer picks sort-then-filter
     * ({@link Plan.LiteralIndexScan}).
     * {@code filter_then_sort} must flip this to filter-then-sort ({@link Plan.NumericIndexScan}).
     */
    @Test
    public void testFilterThenSortOverridesNonSelectiveWhereClauseGenericOrderBy()
    {
        createTable("CREATE TABLE %s (k int, c int, s text, n int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, s, n) VALUES (0, ?, ?, ?)",
                    i, String.valueOf((char) ('a' + i)), i < 2 ? 0 : 1);

        // Default: non-selective WHERE → sort-then-filter (LiteralIndexScan).
        assertQueryHasSubplan("SELECT c FROM %s WHERE n >= 0 ORDER BY s ASC LIMIT 5",
                              Plan.LiteralIndexScan.class,
                              row(0), row(1), row(2), row(3), row(4));

        // Override forces filter-then-sort (NumericIndexScan), results unchanged.
        assertQueryHasSubplan(
                "SELECT c FROM %s WHERE n >= 0 ORDER BY s ASC LIMIT 5 " +
                "WITH optimizer_options = {'hybrid_sort_order': 'filter_then_sort'}",
                Plan.NumericIndexScan.class,
                row(0), row(1), row(2), row(3), row(4));
    }

    // -----------------------------------------------------------------------
    // hybrid_sort_order on non-hybrid queries must behave as if it were absent
    // -----------------------------------------------------------------------

    /**
     * A query with {@code ORDER BY} but no WHERE predicates is not hybrid, so every {@code hybrid_sort_order}
     * value must be accepted and produce the same plan and results as no option at all.
     */
    @Test
    public void testHybridSortOrderIsNoOpForOrderByOnlyQuery()
    {
        createTable("CREATE TABLE %s (k int, c int, s text, n int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 10; i++)
            execute("INSERT INTO %s (k, c, s, n) VALUES (0, ?, ?, ?)",
                    i, String.valueOf((char) ('a' + i)), i);

        String query = "SELECT c FROM %s ORDER BY s ASC LIMIT 5";
        assertQueryHasSubplan(query, Plan.LiteralIndexScan.class, row(0), row(1), row(2), row(3), row(4));

        for (String order : new String[]{ "auto", "sort_then_filter", "filter_then_sort" })
        {
            assertQueryHasSubplan(query + " WITH optimizer_options = {'hybrid_sort_order': '" + order + "'}",
                                  Plan.LiteralIndexScan.class,
                                  row(0), row(1), row(2), row(3), row(4));
        }
    }

    /**
     * A query with a WHERE predicate but no {@code ORDER BY} is not hybrid, so every {@code hybrid_sort_order}
     * value must be accepted and return the same rows as no option at all.
     */
    @Test
    public void testHybridSortOrderIsNoOpForFilterOnlyQuery()
    {
        createTable("CREATE TABLE %s (k int, c int, s text, n int, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");

        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (k, c, s, n) VALUES (0, ?, ?, ?)", i, "x", 1);

        String query = "SELECT c FROM %s WHERE n >= 0";
        assertQueryHasSubplan(query, Plan.NumericIndexScan.class, row(0), row(1), row(2), row(3), row(4));

        for (String order : new String[]{ "auto", "sort_then_filter", "filter_then_sort" })
        {
            assertQueryHasSubplan(query + " WITH optimizer_options = {'hybrid_sort_order': '" + order + "'}",
                                  Plan.NumericIndexScan.class,
                                  row(0), row(1), row(2), row(3), row(4));
        }
    }
}
