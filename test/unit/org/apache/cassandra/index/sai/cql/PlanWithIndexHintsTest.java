/*
 * Copyright DataStax, Inc.
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

import java.util.Arrays;

import org.apache.cassandra.net.MessagingService;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.CassandraRelevantProperties;
import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.apache.cassandra.db.filter.IndexHints;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.StorageAttachedIndex;
import org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.plan.Plan;
import org.apache.cassandra.index.sai.plan.QueryController;
import org.apache.cassandra.inject.Injections;
import org.apache.cassandra.inject.InvokePointBuilder;
import org.apache.cassandra.schema.ColumnMetadata;
import org.assertj.core.api.Assertions;

import static java.lang.String.format;

/**
 * Tests the effects of {@link org.apache.cassandra.db.filter.IndexHints} in SAI's internal query planning:
 * <ul>
 *    <li>Included indexes shouldn't be pruned in the optimized query {@link Plan}.</li>
 *    <li>Excluded indexes shouldn't be included in the query {@link Plan}.</li>
 * </ul>
 */
public class PlanWithIndexHintsTest extends SAITester.Versioned
{
    @BeforeClass
    public static void setUpClass()
    {
        CassandraRelevantProperties.DS_CURRENT_MESSAGING_VERSION.setInt(MessagingService.VERSION_DS_12);
        SAITester.setUpClass();
    }

    @Test
    public void testQueryPlanning() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 text, v2 text)");
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        // Insert some rows, with some rare values and some common values, so the query planner will tend to prefer
        // the index with the most selective predicate.
        int numRows = 1000;
        for (int i = 0; i < numRows; i++)
        {
            execute("INSERT INTO %s (k, v1, v2) VALUES (?, ?, ?)",
                    i,
                    i == 0 || i == 1 ? "rare" : "common",
                    i == 0 || i == 2 ? "rare" : "common");
        }

        beforeAndAfterFlush(() -> {

            // test some queries without any hints, so selection is based on selectivity only
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare'", 2).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v2='rare'", 2).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare'", 1).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='common'", 1).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='rare'", 1).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common'", numRows - 3).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='rare'", 3).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='common'", numRows - 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='rare'", numRows - 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='common'", numRows - 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s ORDER BY v1 LIMIT 10", 10).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s ORDER BY v2 LIMIT 10", 10).uses(idx2);

            // run the same queries as before, but with hints including either idx1 or idx2
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' WITH included_indexes = {idx1}", 2).uses(idx1);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1='rare' WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2='rare' WITH included_indexes = {idx1}", idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v2='rare' WITH included_indexes = {idx2}", 2).uses(idx2);
            for (String idx : Arrays.asList(idx1, idx2))
            {
                String otherIdx = idx.equals(idx1) ? idx2 : idx1;
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' AND v2='rare' WITH included_indexes = {%s}", idx), 1).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' AND v2='common' WITH included_indexes = {%s}", idx), 1).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' AND v2='rare' WITH included_indexes = {%s}", idx), 1).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' AND v2='common' WITH included_indexes = {%s}", idx), numRows - 3).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' OR v2='rare' WITH included_indexes = {%s}", idx), 3).uses(idx, otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' OR v2='common' WITH included_indexes = {%s}", idx), numRows - 1).uses(idx, otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' OR v2='rare' WITH included_indexes = {%s}", idx), numRows - 1).uses(idx, otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' OR v2='common' WITH included_indexes = {%s}", idx), numRows - 1).uses(idx, otherIdx);
            }
            assertThatPlanFor("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH included_indexes = {idx1}", 10).uses(idx1);
            assertNonIncludableIndexesError("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexesError("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH included_indexes = {idx2}", idx2);
            assertThatPlanFor("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH included_indexes = {idx2}", 10).uses(idx2);

            // including both idx1 and idx2
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1='rare' WITH included_indexes = {idx1,idx2}", idx2);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2='rare' WITH included_indexes = {idx1,idx2}", idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare' WITH included_indexes = {idx1,idx2}", 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='common' WITH included_indexes = {idx1,idx2}", 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='rare' WITH included_indexes = {idx1,idx2}", 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common'  WITH included_indexes = {idx1,idx2}", numRows - 3).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='rare' WITH included_indexes = {idx1,idx2}", 3).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='common' WITH included_indexes = {idx1,idx2}", numRows - 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='rare' WITH included_indexes = {idx1,idx2}", numRows - 1).uses(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='common' WITH included_indexes = {idx1,idx2}", numRows - 1).uses(idx1, idx2);
            assertNonIncludableIndexesError("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH included_indexes = {idx1,idx2}", idx2);
            assertNonIncludableIndexesError("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH included_indexes = {idx1,idx2}", idx1);

            // excluding either idx1 or idx2
            assertNeedsAllowFiltering("SELECT * FROM %s WHERE v1='rare' WITH excluded_indexes = {idx1}");
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' WITH excluded_indexes = {idx2}", 2).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v2='rare' WITH excluded_indexes = {idx1}", 2).uses(idx2);
            assertNeedsAllowFiltering("SELECT * FROM %s WHERE v2='rare' WITH excluded_indexes = {idx2}");
            for (String idx : Arrays.asList(idx1, idx2))
            {
                String otherIdx = idx.equals(idx1) ? idx2 : idx1;
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' AND v2='rare' ALLOW FILTERING WITH excluded_indexes={%s}", idx), 1).uses(otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' AND v2='common' ALLOW FILTERING WITH excluded_indexes={%s}", idx), 1).uses(otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' AND v2='rare' ALLOW FILTERING WITH excluded_indexes={%s}", idx), 1).uses(otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' AND v2='common' ALLOW FILTERING WITH excluded_indexes={%s}", idx), numRows - 3).uses(otherIdx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' OR v2='rare' ALLOW FILTERING WITH excluded_indexes={%s}", idx), 3).usesNone();
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='rare' OR v2='common'ALLOW FILTERING WITH excluded_indexes={%s}", idx), numRows - 1).usesNone();
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' OR v2='rare' ALLOW FILTERING WITH excluded_indexes={%s}", idx), numRows - 1).usesNone();
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1='common' OR v2='common' ALLOW FILTERING WITH excluded_indexes={%s}", idx), numRows - 1).usesNone();
            }
            assertANNOrderingNeedsIndex("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes={idx1}", "v1");
            assertThatPlanFor("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes={idx1}", 10).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes={idx2}", 10).uses(idx1);
            assertANNOrderingNeedsIndex("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes={idx2}", "v2");

            // excluding both idx1 and idx2
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' ALLOW FILTERING WITH excluded_indexes = {idx1,idx2}", 2).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2='rare' ALLOW FILTERING WITH excluded_indexes = {idx1,idx2}", 2).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", 1).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='common' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", 1).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='rare' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", 1).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", numRows - 3).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='rare' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", 3).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' OR v2='common'ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", numRows - 1).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='rare' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", numRows - 1).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' OR v2='common' ALLOW FILTERING WITH excluded_indexes={idx1,idx2}", numRows - 1).usesNone();
            assertANNOrderingNeedsIndex("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes={idx1,idx2}", "v1");
            assertANNOrderingNeedsIndex("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes={idx1,idx2}", "v2");

            // excluding all indexes with the wildcard should behave exactly like excluding idx1 and idx2 explicitly
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' ALLOW FILTERING WITH excluded_indexes = {*}", 2).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2='rare' ALLOW FILTERING WITH excluded_indexes = {*}", 2).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='rare' AND v2='rare' ALLOW FILTERING WITH excluded_indexes={*}", 1).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1='common' AND v2='common' ALLOW FILTERING WITH excluded_indexes={*}", numRows - 3).usesNone();
            assertANNOrderingNeedsIndex("SELECT * FROM %s ORDER BY v1 LIMIT 10 WITH excluded_indexes={*}", "v1");
            assertANNOrderingNeedsIndex("SELECT * FROM %s ORDER BY v2 LIMIT 10 WITH excluded_indexes={*}", "v2");

            // the wildcard is not allowed in included_indexes
            assertWildcardIncludedIndexesError("SELECT * FROM %s WHERE v1='rare' WITH included_indexes = {*}");

            // a wildcard exclusion conflicts with an included index that exists on the table
            assertConflictingHints("SELECT * FROM %s WHERE v1='rare' WITH included_indexes={idx1} AND excluded_indexes={*}", "idx1");
        });
    }

    @Test
    public void testQueryPlanningWithAnalyzer() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        String idx = createIndex("CREATE CUSTOM INDEX idx ON %s(v) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = row(1, "Johann Strauss");
        Object[] row2 = row(2, "Richard Strauss");
        Object[] row3 = row(3, "Levi Strauss");
        Object[] row4 = row(4, "Lévi-Strauss");
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        // equality queries using the analyzed index should emit a warning
        String warning = format(AnalyzerEqOperatorSupport.EQ_RESTRICTION_ON_ANALYZED_WARNING, 'v', idx);

        beforeAndAfterFlush(() -> {

            // eq without any hints
            assertThatPlanFor("SELECT * FROM %s WHERE v='Strauss'", row1, row2, row3, row4).uses(idx).warns(warning);
            assertThatPlanFor("SELECT * FROM %s WHERE v='Levi'", row3).uses(idx).warns(warning);
            assertThatPlanFor("SELECT * FROM %s WHERE v='Lévi-Strauss'", row4).uses(idx).warns(warning);

            // match without any hints
            assertThatPlanFor("SELECT * FROM %s WHERE v:'Strauss'", row1, row2, row3, row4).uses(idx).doesntWarn();
            assertThatPlanFor("SELECT * FROM %s WHERE v:'Levi'", row3).uses(idx).doesntWarn();
            assertThatPlanFor("SELECT * FROM %s WHERE v:'Lévi-Strauss'", row4).uses(idx).doesntWarn();

            // eq including the index
            assertThatPlanFor("SELECT * FROM %s WHERE v='Strauss' WITH included_indexes = {idx}", row1, row2, row3, row4).uses(idx).warns(warning);
            assertThatPlanFor("SELECT * FROM %s WHERE v='Levi' WITH included_indexes = {idx}", row3).uses(idx).warns(warning);
            assertThatPlanFor("SELECT * FROM %s WHERE v='Lévi-Strauss' WITH included_indexes = {idx}", row4).uses(idx).warns(warning);

            // match including the index
            assertThatPlanFor("SELECT * FROM %s WHERE v:'Strauss' WITH included_indexes = {idx}", row1, row2, row3, row4).uses(idx).doesntWarn();
            assertThatPlanFor("SELECT * FROM %s WHERE v:'Levi' WITH included_indexes = {idx}", row3).uses(idx).doesntWarn();
            assertThatPlanFor("SELECT * FROM %s WHERE v:'Lévi-Strauss' WITH included_indexes = {idx}", row4).uses(idx).doesntWarn();

            // eq excluding the index
            assertThatPlanFor("SELECT * FROM %s WHERE v='Strauss' ALLOW FILTERING WITH excluded_indexes={idx}", 0).usesNone().doesntWarn();
            assertThatPlanFor("SELECT * FROM %s WHERE v='Levi' ALLOW FILTERING WITH excluded_indexes={idx}", 0).usesNone().doesntWarn();
            assertThatPlanFor("SELECT * FROM %s WHERE v='Lévi-Strauss' ALLOW FILTERING WITH excluded_indexes={idx}", row4).usesNone().doesntWarn();

            // match excluding the index
            assertMatchNeedsIndex("SELECT * FROM %s WHERE v:'Strauss' ALLOW FILTERING WITH excluded_indexes={idx}", "v", "Strauss");
            assertMatchNeedsIndex("SELECT * FROM %s WHERE v:'Levi' ALLOW FILTERING WITH excluded_indexes={idx}", "v", "Levi");
            assertMatchNeedsIndex("SELECT * FROM %s WHERE v:'Lévi-Strauss' ALLOW FILTERING WITH excluded_indexes={idx}", "v", "Lévi-Strauss");

            // if the tested version supports BM25...
            if (version.onOrAfter(Version.BM25_EARLIEST))
            {
                // BM25 without any hints
                assertThatPlanFor("SELECT * FROM %s ORDER BY v BM25 OF 'Strauss' LIMIT 10", row1, row2, row3, row4).uses(idx).doesntWarn();
                assertThatPlanFor("SELECT * FROM %s ORDER BY v BM25 OF 'Strauss' LIMIT 2", row1, row2).uses(idx).doesntWarn();

                // BM25 including the index
                assertThatPlanFor("SELECT * FROM %s ORDER BY v BM25 OF 'Strauss' LIMIT 10 WITH included_indexes = {idx}", row1, row2, row3, row4).uses(idx).doesntWarn();
                assertThatPlanFor("SELECT * FROM %s ORDER BY v BM25 OF 'Strauss' LIMIT 2 WITH included_indexes = {idx}", row1, row2).uses(idx).doesntWarn();

                // BM25 excluding the index
                assertBM25RequiresAnAnalyzedIndex("SELECT * FROM %s ORDER BY v BM25 OF 'Strauss' LIMIT 10 WITH excluded_indexes={idx}", "v");
                assertBM25RequiresAnAnalyzedIndex("SELECT * FROM %s ORDER BY v BM25 OF 'Strauss' LIMIT 2 WITH excluded_indexes={idx}", "v");
            }
        });
    }

    @Test
    public void testQueryPlanningWithNumericQueries() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int)");
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, v1, v2) VALUES (?, ?, ?)";
        Object[] row1 = row(1, 0, 0);
        Object[] row2 = row(2, 0, 1);
        Object[] row3 = row(3, 1, 0);
        Object[] row4 = row(4, 1, 1);
        Object[] row5 = row(5, 2, 0);
        Object[] row6 = row(6, 2, 1);
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);
        execute(insert, row5);
        execute(insert, row6);

        beforeAndAfterFlush(() -> {

            // without any hints
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0", row1, row2).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1!=0", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1<2", row1, row2, row3, row4).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v1<=2", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v1<2", row1, row2, row3, row4).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0", row1, row2, row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1<=2", row1, row2, row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v2=0", row1, row3, row5).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0", row2, row4, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2<2", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2<=2", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 AND v2<=1", row2, row4, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 AND v2<1", row1, row3, row5).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v2>0", row2, row4, row6).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v2>=0", row1, row2, row3, row4, row5, row6).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0 AND v2=0", row1).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0 AND v2>0", row2).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0 AND v2>=0", row1, row2).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v2=0", row3, row5).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v2>0", row4, row6).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v2>=0", row3, row4, row5, row6).usesAnyOf(idx1, idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v2=0", row1, row3, row5).usesAnyOf(idx1, idx2);

            // with restriction in one column only and hints including idx1
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0 WITH included_indexes = {idx1}", row1, row2).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 WITH included_indexes = {idx1}", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1<2 WITH included_indexes = {idx1}", row1, row2, row3, row4).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 WITH included_indexes = {idx1}", row1, row2, row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1<=2 WITH included_indexes = {idx1}", row1, row2, row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v1<=2 WITH included_indexes = {idx1}", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v1<2 WITH included_indexes = {idx1}", row1, row2, row3, row4).uses(idx1);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2=0 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2>0 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2<2 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2>=0 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2<=2 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2>0 AND v2<=1 WITH included_indexes = {idx1}", idx1);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v2>=0 AND v2<1 WITH included_indexes = {idx1}", idx1);

            // with restriction in one column only and hints including idx2
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1=0 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1!=0 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1>0 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1<2 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1>=0 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1<=2 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1>0 AND v1<=2 WITH included_indexes = {idx2}", idx2);
            assertNonIncludableIndexesError("SELECT * FROM %s WHERE v1>=0 AND v1<2 WITH included_indexes = {idx2}", idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2=0 WITH included_indexes = {idx2}", row1, row3, row5).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 WITH included_indexes = {idx2}", row2, row4, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2<2 WITH included_indexes = {idx2}", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 WITH included_indexes = {idx2}", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2<=2 WITH included_indexes = {idx2}", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 AND v2<=1 WITH included_indexes = {idx2}", row2, row4, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 AND v2<1 WITH included_indexes = {idx2}", row1, row3, row5).uses(idx2);

            // with restrictions in both columns and hints including either idx1 or idx2
            for (String idx : Arrays.asList(idx1, idx2))
            {
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1=0 AND v2=0 WITH included_indexes = {%s}", idx), row1).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1!=0 AND v2!=0 WITH included_indexes = {%s}", idx), row4, row6).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1=0 AND v2>0 WITH included_indexes = {%s}", idx), row2).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1=0 AND v2>=0 WITH included_indexes = {%s}", idx), row1, row2).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>0 AND v2=0 WITH included_indexes = {%s}", idx), row3, row5).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>0 AND v2>0 WITH included_indexes = {%s}", idx), row4, row6).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>0 AND v2>=0 WITH included_indexes = {%s}", idx), row3, row4, row5, row6).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>=0 AND v2=0 WITH included_indexes = {%s}", idx), row1, row3, row5).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>=0 AND v2>0 WITH included_indexes = {%s}", idx), row2, row4, row6).usesAtLeast(idx);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>=0 AND v2>=0 WITH included_indexes = {%s}", idx), row1, row2, row3, row4, row5, row6).usesAtLeast(idx);
            }

            // with restriction in one column only and hints excluding idx1
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row1, row2).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1!=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1<2 ALLOW FILTERING WITH excluded_indexes = {idx1}", row1, row2, row3, row4).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 ALLOW FILTERING WITH excluded_indexes = {idx1}", row1, row2, row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1<=2 ALLOW FILTERING WITH excluded_indexes = {idx1}", row1, row2, row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v1<=2 ALLOW FILTERING WITH excluded_indexes = {idx1}", row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v1<2 ALLOW FILTERING WITH excluded_indexes = {idx1}", row1, row2, row3, row4).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2=0 WITH excluded_indexes = {idx1}", row1, row3, row5).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 WITH excluded_indexes = {idx1}", row2, row4, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2<2 WITH excluded_indexes = {idx1}", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 WITH excluded_indexes = {idx1}", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2<=2 WITH excluded_indexes = {idx1}", row1, row2, row3, row4, row5, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 AND v2<=1", row2, row4, row6).uses(idx2);
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 AND v2<1", row1, row3, row5).uses(idx2);

            // with restriction in one column only and hints excluding idx2
            assertThatPlanFor("SELECT * FROM %s WHERE v1=0 WITH excluded_indexes = {idx2}", row1, row2).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1!=0 WITH excluded_indexes = {idx2}", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 WITH excluded_indexes = {idx2}", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1<2 WITH excluded_indexes = {idx2}", row1, row2, row3, row4).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 WITH excluded_indexes = {idx2}", row1, row2, row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1<=2 WITH excluded_indexes = {idx2}", row1, row2, row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>0 AND v1<=2 WITH excluded_indexes = {idx2}", row3, row4, row5, row6).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v1>=0 AND v1<2 WITH excluded_indexes = {idx2}", row1, row2, row3, row4).uses(idx1);
            assertThatPlanFor("SELECT * FROM %s WHERE v2=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", row1, row3, row5).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 ALLOW FILTERING WITH excluded_indexes = {idx2}", row2, row4, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2<2 ALLOW FILTERING WITH excluded_indexes = {idx2}", row1, row2, row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 ALLOW FILTERING WITH excluded_indexes = {idx2}", row1, row2, row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2<=2 ALLOW FILTERING WITH excluded_indexes = {idx2}", row1, row2, row3, row4, row5, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2>0 AND v2<=1 ALLOW FILTERING WITH excluded_indexes = {idx2}", row2, row4, row6).usesNone();
            assertThatPlanFor("SELECT * FROM %s WHERE v2>=0 AND v2<1 ALLOW FILTERING WITH excluded_indexes = {idx2}", row1, row3, row5).usesNone();

            // with restrictions in both columns and hints excluding either idx1 or idx2
            for (String one : Arrays.asList(idx1, idx2))
            {
                String other = one.equals(idx1) ? idx2 : idx1;
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row1).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1!=0 AND v2!=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row4, row6).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1=0 AND v2>0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row2).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1=0 AND v2>=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row1, row2).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row3, row5).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>0 AND v2>0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row4, row6).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>0 AND v2>=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row3, row4, row5, row6).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>=0 AND v2=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row1, row3, row5).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>=0 AND v2>0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row2, row4, row6).uses(other);
                assertThatPlanFor(format("SELECT * FROM %%s WHERE v1>=0 AND v2>=0 ALLOW FILTERING WITH excluded_indexes = {%s}", one), row1, row2, row3, row4, row5, row6).uses(other);
            }
        });
    }

    @Test
    public void testQueryPlanningWithRestrictedButUnusableIndex() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int)");
        createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx3 = createIndex("CREATE CUSTOM INDEX idx3 ON %s(v3) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, v1, v2, v3) VALUES (?, ?, ?, ?)";
        Object[] row1 = row(1, 0, 0, 9);
        Object[] row2 = row(2, 0, 1, 9);
        Object[] row3 = row(3, 1, 0, 9);
        Object[] row4 = row(4, 1, 1, 9);
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        beforeAndAfterFlush(() -> {

            String query = "SELECT * FROM %s WHERE v1=0 OR v2=0 ALLOW FILTERING";
            assertThatPlanFor(query, row1, row2, row3).usesNone();
            assertNonIncludableIndexesError(query + " WITH included_indexes={idx1}");
            assertThatPlanFor(query + " WITH excluded_indexes={idx1}", row1, row2, row3).usesNone();

            query = "SELECT * FROM %s WHERE (v1=0 OR v2=0) AND v3=9 ALLOW FILTERING";
            assertThatPlanFor(query, row1, row2, row3).uses(idx3);
            assertNonIncludableIndexesError(query + " WITH included_indexes={idx1}");
            assertThatPlanFor(query + " WITH excluded_indexes={idx1}", row1, row2, row3).uses(idx3);
        });
    }

    /**
     * Tests that there will be an error when the included indexes exceed the intersection clause limit.
     */
    @Test
    public void testIntersectionClauseLimit() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 int, v2 int, v3 int, v4 int)");
        String idx1 = createIndex("CREATE CUSTOM INDEX idx1 ON %s(v1) USING 'StorageAttachedIndex'");
        String idx2 = createIndex("CREATE CUSTOM INDEX idx2 ON %s(v2) USING 'StorageAttachedIndex'");
        String idx3 = createIndex("CREATE CUSTOM INDEX idx3 ON %s(v3) USING 'StorageAttachedIndex'");
        String idx4 = createIndex("CREATE CUSTOM INDEX idx4 ON %s(v4) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, v1, v2, v3, v4) VALUES (?, ?, ?, ?, ?)";
        Object[] row1 = row(1, 0, 0, 0, 1);
        Object[] row2 = row(2, 0, 1, 0, 2);
        Object[] row3 = row(3, 1, 0, 0, 3);
        Object[] row4 = row(4, 1, 1, 0, 4);
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        int defaultIntersectionClauseLimit = CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.getInt();
        try
        {
            // disable query optimization so we can test the intersection clause limit in a more predictable way
            QueryController.QUERY_OPT_LEVEL = 0;

            beforeAndAfterFlush(() -> {
                String query = "SELECT * FROM %s WHERE v1=0 AND v2=0 AND v3 = 0";

                CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(3);
                assertThatPlanFor(query, row1).usesAnyOf(idx1, idx2, idx3);
                assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).usesAtLeast(idx1).uses(3);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).usesAtLeast(idx1, idx2).uses(3);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3);

                CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(2);
                assertThatPlanFor(query, row1).usesAnyOf(idx1, idx2, idx3);
                assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).usesAtLeast(idx1).uses(2);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3);

                CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(1);
                assertThatPlanFor(query, row1).usesAnyOf(idx1, idx2, idx3);
                assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).uses(idx1);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3);

                // test with an OR clause, so the intersection is nested
                query = "SELECT * FROM %s WHERE (v1=0 AND v2=0 AND v3 = 0) OR v4 = 0";

                CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(3);
                assertThatPlanFor(query, row1).uses(idx1, idx2, idx3, idx4);
                assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).uses(idx1, idx2, idx3, idx4);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2, idx3, idx4);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3, idx4);

                CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(2);
                assertThatPlanFor(query, row1).usesAtLeast(idx4).uses(3);
                assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).usesAtLeast(idx1, idx4).uses(3);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2, idx4);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3, idx4);

                CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(1);
                assertThatPlanFor(query, row1).usesAtLeast(idx4).uses(2);
                assertThatPlanFor(query + " WITH included_indexes={idx1}", row1).uses(idx1, idx4);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2}", row1).uses(idx1, idx2, idx4);
                assertThatPlanFor(query + " WITH included_indexes={idx1, idx2, idx3}", row1).uses(idx1, idx2, idx3, idx4);
            });
        }
        finally
        {
            CassandraRelevantProperties.SAI_INTERSECTION_CLAUSE_LIMIT.setInt(defaultIntersectionClauseLimit);
            QueryController.QUERY_OPT_LEVEL = 1;
        }
    }

    @Test
    public void testVector() throws Throwable
    {
        Assume.assumeTrue(version.onOrAfter(Version.JVECTOR_EARLIEST));

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v vector<float, 2>)");
        String idx = createIndex("CREATE CUSTOM INDEX idx ON %s(v) USING 'StorageAttachedIndex'");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        execute(insert, row(1, vector(0.1f, 0.2f)));

        beforeAndAfterFlush(() -> {

            // with ANN queries
            String query = "SELECT k FROM %s ORDER BY v ANN OF [0.1, 0.2] LIMIT 10";
            assertThatPlanFor(query, row(1)).uses(idx);
            assertThatPlanFor(query + " WITH included_indexes={idx}", row(1)).uses(idx);
            assertANNOrderingNeedsIndex(query + " WITH excluded_indexes={idx}", "v");

            // with an unsupported eq query, that can be unshaded with excluded_indexes and ALLOW FILTERING
            query = "SELECT k FROM %s WHERE v = [0.1, 0.2] LIMIT 10";
            assertUnsupportVectorOperator(query);
            assertUnsupportVectorOperator(query + " WITH included_indexes={idx}");
            assertNeedsAllowFiltering(query + " WITH excluded_indexes={idx}");
            assertThatPlanFor(query + " ALLOW FILTERING WITH excluded_indexes={idx}", row(1)).usesNone();
        });
    }

    /**
     * Test that index hints can bed used as a workaround for CNDB-12425,
     * where queries with {@code ALLOW FILTERING} stop working during index build.
     * Index hints can be used to explicitly exclude the non-queryable index.
     */
    @Test
    public void testExcludeDuringIndexBuild() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        String insert = "INSERT INTO %s (k, v) VALUES (?, ?)";
        Object[] row1 = row(1, 1);
        Object[] row2 = row(2, 2);
        execute(insert, row1);
        execute(insert, row2);

        // verify that an ALLOW FILTERING query works without any index
        String select = "SELECT * FROM %s WHERE v=1 ALLOW FILTERING";
        assertThatPlanFor(select, row1).usesNone();

        // create an index on the filtered column, blocked on its building task
        Injections.Barrier barrier = Injections.newBarrier("block_index_build", 2, false)
                                               .add(InvokePointBuilder.newInvokePoint()
                                                                      .onClass(StorageAttachedIndex.class)
                                                                      .onMethod("startInitialBuild"))
                                               .build();
        Injections.inject(barrier);
        String idx = createIndexAsync("CREATE CUSTOM INDEX idx ON %s(v) USING 'StorageAttachedIndex'");

        // verify that the previous ALLOW FILTERING query stops working, as described by CNDB-12425
        Assertions.assertThatThrownBy(() -> execute(select))
                  .isInstanceOf(ReadFailureException.class)
                  .hasMessageContaining(RequestFailureReason.INDEX_BUILD_IN_PROGRESS.name());

        // verify that the previous query still fails if we use index hints to include the non-queryable index
        Assertions.assertThatThrownBy(() -> execute(select + " WITH included_indexes={idx}"))
                  .isInstanceOf(ReadFailureException.class)
                  .hasMessageContaining(RequestFailureReason.INDEX_BUILD_IN_PROGRESS.name());

        // verify that the previous query works again is we use index hints to exclude the non-queryable index
        assertThatPlanFor(select + " WITH excluded_indexes={idx}", row1).usesNone();

        // allow the index build to complete
        barrier.countDown();
        barrier.disable();
        waitForIndexQueryable(idx);

        // verify that the previous query works again without any index hints
        assertThatPlanFor(select, row1).uses(idx);
    }

    /**
     * Test that index hints can be used to query indexes for columns in all possible table positions: partition key
     * components, clustering key components, static columns and regular columns.
     */
    @Test
    public void columnPositionsTest()
    {
        createTable("CREATE TABLE %s (" +
                    "k1 int, k2 text, k3 vector<float, 2>, " +
                    "c1 int, c2 text, c3 vector<float, 2>, " +
                    "s1 int static, s2 text static, s3 vector<float, 2> static," +
                    "r1 int, r2 text, r3 vector<float, 2>, " +
                    "PRIMARY KEY((k1, k2, k3), c1, c2, c3))");

        boolean supportsANN = version.onOrAfter(Version.JVECTOR_EARLIEST);
        boolean supportsBM25 = version.onOrAfter(Version.BM25_EARLIEST);

        // create numeric indexes in all table positions
        createIndex("CREATE CUSTOM INDEX partition_numeric ON %s(k1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX clustering_numeric ON %s(c1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX static_numeric ON %s(s1) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX regular_numeric ON %s(r1) USING 'StorageAttachedIndex'");

        // create literal indexes in all table positions
        createIndex("CREATE CUSTOM INDEX partition_literal ON %s(k2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX clustering_literal ON %s(c2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX static_literal ON %s(s2) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX regular_literal ON %s(r2) USING 'StorageAttachedIndex'");

        // create analyzed indexes in all table positions
        createIndex("CREATE CUSTOM INDEX partition_analyzed ON %s(k2) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'UNSUPPORTED', 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX clustering_analyzed ON %s(c2) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'UNSUPPORTED', 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX static_analyzed ON %s(s2) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'UNSUPPORTED', 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX regular_analyzed ON %s(r2) USING 'StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'UNSUPPORTED', 'index_analyzer': 'standard' }");

        // create ANN indexes in all table positions
        if (supportsANN)
        {
            createIndex("CREATE CUSTOM INDEX partition_ann ON %s(k3) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX clustering_ann ON %s(c3) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX static_ann ON %s(s3) USING 'StorageAttachedIndex'");
            createIndex("CREATE CUSTOM INDEX regular_ann ON %s(r3) USING 'StorageAttachedIndex'");
        }

        // insert some data
        String insert = "INSERT INTO %s (k1, k2, k3, c1, c2, c3, s1, s2, s3, r1, r2, r3) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        execute(insert, row(1, "a", vector(0.1f, 0.1f), 10, "aa", vector(0.01f, 0.01f), 100, "aaa", vector(0.001f, 0.001f), 1000, "aaaa", vector(0.0001f, 0.0001f)));
        execute(insert, row(1, "a", vector(0.1f, 0.2f), 11, "ab", vector(0.01f, 0.02f), 100, "aaa", vector(0.001f, 0.002f), 1001, "aaab", vector(0.0001f, 0.0002f)));
        execute(insert, row(2, "b", vector(0.2f, 0.1f), 10, "aa", vector(0.02f, 0.01f), 200, "bbb", vector(0.002f, 0.001f), 1000, "aaaa", vector(0.0002f, 0.0001f)));
        execute(insert, row(2, "b", vector(0.2f, 0.2f), 11, "ab", vector(0.02f, 0.02f), 200, "bbb", vector(0.002f, 0.002f), 1001, "aaab", vector(0.0002f, 0.0002f)));
        Object[] row1 = row(1, "a", 10, "aa");
        Object[] row2 = row(1, "a", 11, "ab");
        Object[] row3 = row(2, "b", 10, "aa");
        Object[] row4 = row(2, "b", 11, "ab");

        final String query = "SELECT k1, k2, c1, c2 FROM %s ";

        // query partition key columns without hints
        assertThatPlanFor(query + "WHERE k1=1", row1, row2).uses("partition_numeric");
        assertThatPlanFor(query + "WHERE k1!=1", row3, row4).uses("partition_numeric");
        assertThatPlanFor(query + "WHERE k1>1", row3, row4).uses("partition_numeric");
        assertThatPlanFor(query + "WHERE k1<2", row1, row2).uses("partition_numeric");
        assertThatPlanFor(query + "WHERE k1>=1", row1, row2, row3, row4).uses("partition_numeric");
        assertThatPlanFor(query + "WHERE k1<=2", row1, row2, row3, row4).uses("partition_numeric");
        assertThatPlanFor(query + "WHERE k2='a'", row1, row2).uses("partition_literal");
        assertThatPlanFor(query + "WHERE k2:'a'", row1, row2).uses("partition_analyzed");
        if (supportsBM25)
            assertBM25OnNonRegularColumnIsRejected(query + "ORDER BY k2 BM25 OF 'a' LIMIT 10", ColumnMetadata.Kind.PARTITION_KEY, "k2");
        if (supportsANN)
            assertThatPlanFor(query + "ORDER BY k3 ANN OF [0.1, 0.1] LIMIT 10", row1, row2, row3, row4).uses("partition_ann");

        // query partition key columns with included index
        assertThatPlanFor(query + "WHERE k1=1 WITH included_indexes={partition_numeric}", row1, row2).uses("partition_numeric");
        assertThatPlanFor(query + "WHERE k1!=1 WITH included_indexes={partition_numeric}", row3, row4).uses("partition_numeric");
        assertThatPlanFor(query + "WHERE k1>1 WITH included_indexes={partition_numeric}", row3, row4).uses("partition_numeric");
        assertThatPlanFor(query + "WHERE k1<2 WITH included_indexes={partition_numeric}", row1, row2).uses("partition_numeric");
        assertThatPlanFor(query + "WHERE k1>=1 WITH included_indexes={partition_numeric}" , row1, row2, row3, row4).uses("partition_numeric");
        assertThatPlanFor(query + "WHERE k1<=2 WITH included_indexes={partition_numeric}" , row1, row2, row3, row4).uses("partition_numeric");
        assertThatPlanFor(query + "WHERE k2='a' WITH included_indexes={partition_literal}", row1, row2).uses("partition_literal");
        assertThatPlanFor(query + "WHERE k2:'a' WITH included_indexes={partition_analyzed}", row1, row2).uses("partition_analyzed");
        if (supportsBM25)
            assertBM25OnNonRegularColumnIsRejected(query + "ORDER BY k2 BM25 OF 'a' LIMIT 10 WITH included_indexes={partition_analyzed}",
                                                   ColumnMetadata.Kind.PARTITION_KEY, "k2");
        if (supportsANN)
            assertThatPlanFor(query + "ORDER BY k3 ANN OF [0.1, 0.1] LIMIT 10 WITH included_indexes={partition_ann}", row1, row2, row3, row4).uses("partition_ann");

        // query partition key columns with excluded index
        assertThatPlanFor(query + "WHERE k1=1 ALLOW FILTERING WITH excluded_indexes={partition_numeric}", row1, row2).usesNone();
        assertThatPlanFor(query + "WHERE k1!=1 ALLOW FILTERING WITH excluded_indexes={partition_numeric}", row3, row4).usesNone();
        assertThatPlanFor(query + "WHERE k1>1 ALLOW FILTERING WITH excluded_indexes={partition_numeric}", row3, row4).usesNone();
        assertThatPlanFor(query + "WHERE k1<2 ALLOW FILTERING WITH excluded_indexes={partition_numeric}", row1, row2).usesNone();
        assertThatPlanFor(query + "WHERE k1>=1 ALLOW FILTERING WITH excluded_indexes={partition_numeric}", row1, row2, row3, row4).usesNone();
        assertThatPlanFor(query + "WHERE k1<=2 ALLOW FILTERING WITH excluded_indexes={partition_numeric}", row1, row2, row3, row4).usesNone();
        assertThatPlanFor(query + "WHERE k2='a' ALLOW FILTERING WITH excluded_indexes={partition_literal}", row1, row2).usesNone();
        assertIndexDoesNotSupportAnalyzerMatches(query + "WHERE k2:'a' ALLOW FILTERING WITH excluded_indexes={partition_analyzed}", "k2");
        if (supportsBM25)
            assertBM25RequiresAnAnalyzedIndex(query + "ORDER BY k2 BM25 OF 'a' LIMIT 10 WITH excluded_indexes={partition_analyzed}", "k2");
        if (supportsANN)
            assertANNOrderingNeedsIndex(query + "ORDER BY k3 ANN OF [0.1, 0.1] LIMIT 10 WITH excluded_indexes={partition_ann}", "k3");

        // query clustering key columns without hints
        assertThatPlanFor(query + "WHERE c1=10", row1, row3).uses("clustering_numeric");
        assertThatPlanFor(query + "WHERE c1!=10", row2, row4).uses("clustering_numeric");
        assertThatPlanFor(query + "WHERE c1>10", row2, row4).uses("clustering_numeric");
        assertThatPlanFor(query + "WHERE c1<11", row1, row3).uses("clustering_numeric");
        assertThatPlanFor(query + "WHERE c1>=10", row1, row2, row3, row4).uses("clustering_numeric");
        assertThatPlanFor(query + "WHERE c1<=11", row1, row2, row3, row4).uses("clustering_numeric");
        assertThatPlanFor(query + "WHERE c2='aa'", row1, row3).uses("clustering_literal");
        assertCannotBeRestrictedByClustering(query + "WHERE c2:'aa'", "c2");
        if (supportsBM25)
            assertBM25OnNonRegularColumnIsRejected(query + "ORDER BY c2 BM25 OF 'aa' LIMIT 10", ColumnMetadata.Kind.CLUSTERING, "c2");
        if (supportsANN)
            assertThatPlanFor(query + "ORDER BY c3 ANN OF [0.1, 0.1] LIMIT 10", row1, row2, row3, row4).uses("clustering_ann");

        // query clustering key columns with included index
        assertThatPlanFor(query + "WHERE c1=10 WITH included_indexes={clustering_numeric}", row1, row3).uses("clustering_numeric");
        assertThatPlanFor(query + "WHERE c1!=10 WITH included_indexes={clustering_numeric}", row2, row4).uses("clustering_numeric");
        assertThatPlanFor(query + "WHERE c1>10 WITH included_indexes={clustering_numeric}", row2, row4).uses("clustering_numeric");
        assertThatPlanFor(query + "WHERE c1<11 WITH included_indexes={clustering_numeric}", row1, row3).uses("clustering_numeric");
        assertThatPlanFor(query + "WHERE c1>=10 WITH included_indexes={clustering_numeric}", row1, row2, row3, row4).uses("clustering_numeric");
        assertThatPlanFor(query + "WHERE c1<=11 WITH included_indexes={clustering_numeric}", row1, row2, row3, row4).uses("clustering_numeric");
        assertThatPlanFor(query + "WHERE c2='aa' WITH included_indexes={clustering_literal}", row1, row3).uses("clustering_literal");
        assertCannotBeRestrictedByClustering(query + "WHERE c2:'aa' WITH included_indexes={clustering_analyzed}", "c2");
        if (supportsBM25)
            assertBM25OnNonRegularColumnIsRejected(query + "ORDER BY c2 BM25 OF 'aa' LIMIT 10 WITH included_indexes={clustering_analyzed}",
                                                   ColumnMetadata.Kind.CLUSTERING, "c2");
        if (supportsANN)
            assertThatPlanFor(query + "ORDER BY c3 ANN OF [0.1, 0.1] LIMIT 10 WITH included_indexes={clustering_ann}", row1, row2, row3, row4).uses("clustering_ann");

        // query clustering key columns with excluded index
        assertThatPlanFor(query + "WHERE c1=10 ALLOW FILTERING WITH excluded_indexes={clustering_numeric}", row1, row3).usesNone();
        assertThatPlanFor(query + "WHERE c1!=10 ALLOW FILTERING WITH excluded_indexes={clustering_numeric}", row2, row4).usesNone();
        assertThatPlanFor(query + "WHERE c1>10 ALLOW FILTERING WITH excluded_indexes={clustering_numeric}", row2, row4).usesNone();
        assertThatPlanFor(query + "WHERE c1<11 ALLOW FILTERING WITH excluded_indexes={clustering_numeric}", row1, row3).usesNone();
        assertThatPlanFor(query + "WHERE c1>=10 ALLOW FILTERING WITH excluded_indexes={clustering_numeric}", row1, row2, row3, row4).usesNone();
        assertThatPlanFor(query + "WHERE c1<=11 ALLOW FILTERING WITH excluded_indexes={clustering_numeric}", row1, row2, row3, row4).usesNone();
        assertThatPlanFor(query + "WHERE c2='aa' ALLOW FILTERING WITH excluded_indexes={clustering_literal}", row1, row3).usesNone();
        assertCannotBeRestrictedByClustering(query + "WHERE c2:'aa' ALLOW FILTERING WITH excluded_indexes={clustering_analyzed}", "c2");
        if (supportsBM25)
            assertBM25RequiresAnAnalyzedIndex(query + "ORDER BY c2 BM25 OF 'aa' LIMIT 10 WITH excluded_indexes={clustering_analyzed}", "c2");
        if (supportsANN)
            assertANNOrderingNeedsIndex(query + "ORDER BY c3 ANN OF [0.1, 0.2] LIMIT 10 WITH excluded_indexes={clustering_ann}", "c3");

        // query static columns without hints
        assertThatPlanFor(query + "WHERE s1=100", row1, row2).uses("static_numeric");
        assertThatPlanFor(query + "WHERE s1!=100", row3, row4).uses("static_numeric");
        assertThatPlanFor(query + "WHERE s1>100", row3, row4).uses("static_numeric");
        assertThatPlanFor(query + "WHERE s1<200", row1, row2).uses("static_numeric");
        assertThatPlanFor(query + "WHERE s1>=100", row1, row2, row3, row4).uses("static_numeric");
        assertThatPlanFor(query + "WHERE s1<=200", row1, row2, row3, row4).uses("static_numeric");
        assertThatPlanFor(query + "WHERE s2='aaa'", row1, row2).uses("static_literal");
        assertThatPlanFor(query + "WHERE s2:'aaa'", row1, row2).uses("static_analyzed");
        if (supportsBM25)
            assertBM25OnNonRegularColumnIsRejected(query + "ORDER BY s2 BM25 OF 'aa' LIMIT 10", ColumnMetadata.Kind.STATIC, "s2");
        if (supportsANN)
            assertThatPlanFor(query + "ORDER BY s3 ANN OF [0.1, 0.1] LIMIT 10", row1, row2, row3, row4).uses("static_ann");

        // query static columns with included index
        assertThatPlanFor(query + "WHERE s1=100 WITH included_indexes={static_numeric}", row1, row2).uses("static_numeric");
        assertThatPlanFor(query + "WHERE s1!=100 WITH included_indexes={static_numeric}", row3, row4).uses("static_numeric");
        assertThatPlanFor(query + "WHERE s1>100 WITH included_indexes={static_numeric}", row3, row4).uses("static_numeric");
        assertThatPlanFor(query + "WHERE s1<200 WITH included_indexes={static_numeric}", row1, row2).uses("static_numeric");
        assertThatPlanFor(query + "WHERE s1>=100 WITH included_indexes={static_numeric}", row1, row2, row3, row4).uses("static_numeric");
        assertThatPlanFor(query + "WHERE s1<=200 WITH included_indexes={static_numeric}", row1, row2, row3, row4).uses("static_numeric");
        assertThatPlanFor(query + "WHERE s2='aaa' WITH included_indexes={static_literal}", row1, row2).uses("static_literal");
        assertThatPlanFor(query + "WHERE s2:'aaa' WITH included_indexes={static_analyzed}", row1, row2).uses("static_analyzed");
        if (supportsBM25)
            assertBM25OnNonRegularColumnIsRejected(query + "ORDER BY s2 BM25 OF 'aa' LIMIT 10 WITH included_indexes={static_analyzed}",
                                                   ColumnMetadata.Kind.STATIC, "s2");
        if (supportsANN)
            assertThatPlanFor(query + "ORDER BY s3 ANN OF [0.1, 0.1] LIMIT 10 WITH included_indexes={static_ann}", row1, row2, row3, row4).uses("static_ann");

        // query static columns with excluded index
        assertThatPlanFor(query + "WHERE s1=100 ALLOW FILTERING WITH excluded_indexes={static_numeric}", row1, row2).usesNone();
        assertThatPlanFor(query + "WHERE s1!=100 ALLOW FILTERING WITH excluded_indexes={static_numeric}", row3, row4).usesNone();
        assertThatPlanFor(query + "WHERE s1>100 ALLOW FILTERING WITH excluded_indexes={static_numeric}", row3, row4).usesNone();
        assertThatPlanFor(query + "WHERE s1<200 ALLOW FILTERING WITH excluded_indexes={static_numeric}", row1, row2).usesNone();
        assertThatPlanFor(query + "WHERE s1>=100 ALLOW FILTERING WITH excluded_indexes={static_numeric}", row1, row2, row3, row4).usesNone();
        assertThatPlanFor(query + "WHERE s1<=200 ALLOW FILTERING WITH excluded_indexes={static_numeric}", row1, row2, row3, row4).usesNone();
        assertThatPlanFor(query + "WHERE s2='aaa' ALLOW FILTERING WITH excluded_indexes={static_literal}", row1, row2).usesNone();
        assertIndexDoesNotSupportAnalyzerMatches(query + "WHERE s2:'aaa' ALLOW FILTERING WITH excluded_indexes={static_analyzed}", "s2");
        if (supportsBM25)
            assertBM25RequiresAnAnalyzedIndex(query + "ORDER BY s2 BM25 OF 'aa' LIMIT 10 WITH excluded_indexes={static_analyzed}", "s2");
        if (supportsANN)
            assertANNOrderingNeedsIndex(query + "ORDER BY s3 ANN OF [0.1, 0.1] LIMIT 10 WITH excluded_indexes={static_ann}", "s3");

        // query regular columns without hints
        assertThatPlanFor(query + "WHERE r1=1000", row1, row3).uses("regular_numeric");
        assertThatPlanFor(query + "WHERE r1!=1000", row2, row4).uses("regular_numeric");
        assertThatPlanFor(query + "WHERE r1>1000", row2, row4).uses("regular_numeric");
        assertThatPlanFor(query + "WHERE r1<1001", row1, row3).uses("regular_numeric");
        assertThatPlanFor(query + "WHERE r1>=1000", row1, row2, row3, row4).uses("regular_numeric");
        assertThatPlanFor(query + "WHERE r1<=1001", row1, row2, row3, row4).uses("regular_numeric");
        assertThatPlanFor(query + "WHERE r2='aaaa'", row1, row3).uses("regular_literal");
        assertThatPlanFor(query + "WHERE r2:'aaaa'", row1, row3).uses("regular_analyzed");
        if (supportsBM25)
            assertThatPlanFor(query + "ORDER BY r2 BM25 OF 'aaaa' LIMIT 10", row1, row3).uses("regular_analyzed");
        if (supportsANN)
            assertThatPlanFor(query + "ORDER BY r3 ANN OF [0.1, 0.1] LIMIT 10", row1, row2, row3, row4).uses("regular_ann");

        // query regular columns with included indexes
        assertThatPlanFor(query + "WHERE r1=1000 WITH included_indexes={regular_numeric}", row1, row3).uses("regular_numeric");
        assertThatPlanFor(query + "WHERE r1!=1000 WITH included_indexes={regular_numeric}", row2, row4).uses("regular_numeric");
        assertThatPlanFor(query + "WHERE r1>1000 WITH included_indexes={regular_numeric}", row2, row4).uses("regular_numeric");
        assertThatPlanFor(query + "WHERE r1<1001 WITH included_indexes={regular_numeric}", row1, row3).uses("regular_numeric");
        assertThatPlanFor(query + "WHERE r1>=1000 WITH included_indexes={regular_numeric}", row1, row2, row3, row4).uses("regular_numeric");
        assertThatPlanFor(query + "WHERE r1<=1001 WITH included_indexes={regular_numeric}", row1, row2, row3, row4).uses("regular_numeric");
        assertThatPlanFor(query + "WHERE r2='aaaa' WITH included_indexes={regular_literal}", row1, row3).uses("regular_literal");
        assertThatPlanFor(query + "WHERE r2:'aaaa' WITH included_indexes={regular_analyzed}", row1, row3).uses("regular_analyzed");
        if (supportsBM25)
            assertThatPlanFor(query + "ORDER BY r2 BM25 OF 'aaaa' LIMIT 10 WITH included_indexes={regular_analyzed}", row1, row3).uses("regular_analyzed");
        if (supportsANN)
            assertThatPlanFor(query + "ORDER BY r3 ANN OF [0.1, 0.1] LIMIT 10 WITH included_indexes={regular_ann}", row1, row2, row3, row4).uses("regular_ann");

        // query regular columns with excluded indexes
        assertThatPlanFor(query + "WHERE r1=1000 ALLOW FILTERING WITH excluded_indexes={regular_numeric}", row1, row3).usesNone();
        assertThatPlanFor(query + "WHERE r1!=1000 ALLOW FILTERING WITH excluded_indexes={regular_numeric}", row2, row4).usesNone();
        assertThatPlanFor(query + "WHERE r1>1000 ALLOW FILTERING WITH excluded_indexes={regular_numeric}", row2, row4).usesNone();
        assertThatPlanFor(query + "WHERE r1<1001 ALLOW FILTERING WITH excluded_indexes={regular_numeric}", row1, row3).usesNone();
        assertThatPlanFor(query + "WHERE r1>=1000 ALLOW FILTERING WITH excluded_indexes={regular_numeric}", row1, row2, row3, row4).usesNone();
        assertThatPlanFor(query + "WHERE r1<=1001 ALLOW FILTERING WITH excluded_indexes={regular_numeric}", row1, row2, row3, row4).usesNone();
        assertThatPlanFor(query + "WHERE r2='aaaa' ALLOW FILTERING WITH excluded_indexes={regular_literal}", row1, row3).usesNone();
        assertIndexDoesNotSupportAnalyzerMatches(query + "WHERE r2:'aaaa' ALLOW FILTERING WITH excluded_indexes={regular_analyzed}", "r2");
        if (supportsBM25)
            assertBM25RequiresAnAnalyzedIndex(query + "ORDER BY r2 BM25 OF 'aaaa' LIMIT 10 WITH excluded_indexes={regular_analyzed}", "r2");
        if (supportsANN)
            assertANNOrderingNeedsIndex(query + "ORDER BY r3 ANN OF [0.1, 0.1] LIMIT 10 WITH excluded_indexes={regular_ann}", "r3");
    }

    private void assertNeedsAllowFiltering(String query)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(StatementRestrictions.REQUIRES_ALLOW_FILTERING_MESSAGE);
    }

    public void assertNonIncludableIndexesError(String query, String... indexes)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(IndexHints.NON_INCLUDABLE_INDEXES_ERROR);
    }

    private void assertWildcardIncludedIndexesError(String query)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(IndexHints.WILDCARD_INCLUDED_INDEXES_ERROR);
    }

    private void assertConflictingHints(String query, String indexName)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessageContaining(IndexHints.CONFLICTING_INDEXES_ERROR + indexName);
    }

    private void assertANNOrderingNeedsIndex(String query, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(format(StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, column));
    }

    private void assertUnsupportVectorOperator(String query)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(StatementRestrictions.VECTOR_INDEXES_UNSUPPORTED_OP_MESSAGE);
    }

    private void assertMatchNeedsIndex(String query, String column, String value)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(format(StatementRestrictions.RESTRICTION_REQUIRES_INDEX_MESSAGE,
                                     ':',
                                     format("%s : '%s'", column, value)));
    }

    private void assertBM25RequiresAnAnalyzedIndex(String query, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(format(StatementRestrictions.BM25_ORDERING_REQUIRES_ANALYZED_INDEX_MESSAGE, column));
    }

    private void assertIndexDoesNotSupportAnalyzerMatches(String query, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(format(StatementRestrictions.INDEX_DOES_NOT_SUPPORT_ANALYZER_MATCHES_MESSAGE, column));
    }

    private void assertCannotBeRestrictedByClustering(String query, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(format(SingleColumnRestriction.AnalyzerMatchesRestriction.CANNOT_BE_RESTRICTED_BY_CLUSTERING_ERROR, column));
    }

    private void assertBM25OnNonRegularColumnIsRejected(String query, ColumnMetadata.Kind kind, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(format(StatementRestrictions.BM25_ORDERING_REQUIRES_REGULAR_COLUMN_MESSAGE,
                                     kind.name().toLowerCase().replace("_", " "), column));
    }
}
