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

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.restrictions.SingleColumnRestriction;
import org.apache.cassandra.cql3.restrictions.StatementRestrictions;
import org.assertj.core.api.Assertions;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;
import org.apache.cassandra.index.sai.disk.v1.SegmentBuilder;
import org.apache.cassandra.index.sai.plan.QueryController;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.lang.String.format;
import static org.apache.cassandra.index.sai.analyzer.AnalyzerEqOperatorSupport.EQ_AMBIGUOUS_ERROR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class BM25Test extends SAITester
{
    @Parameterized.Parameter
    public Version testVersion;

    @Parameterized.Parameters(name = "version={0}")
    public static List<Object> data()
    {
        return Version.ALL.stream().filter(v -> v.onOrAfter(Version.BM25_EARLIEST))
                                   .map(v -> new Object[]{ v })
                                   .collect(Collectors.toList());
    }

    // Pattern that treats apostrophes within words as part of the word
    public static final Pattern PATTERN = Pattern.compile("[^\\w']+|'(?=\\s)|(?<=\\s)'");
    public static final int DATASET_BODY_COLUMN = 3;

    @Before
    public void setup() throws Throwable
    {
        SAIUtil.setCurrentVersion(testVersion);
    }

    @Test
    public void testTwoIndexes()
    {
        // create un-analyzed index
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");

        // BM25 should fail with only an equality index
        assertInvalidMessage("BM25 ordering on column v requires an analyzed index",
                             "SELECT k FROM %s WHERE v : 'apple' ORDER BY v BM25 OF 'apple' LIMIT 3");

        createAnalyzedIndex();
        // BM25 query should work now
        var result = execute("SELECT k FROM %s WHERE v : 'apple' ORDER BY v BM25 OF 'apple' LIMIT 3");
        assertRows(result, row(1));
    }

    @Test
    public void testDeletedRow() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createAnalyzedIndex();
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple juice')");
        var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3");
        assertThat(result).hasSize(2);
        execute("DELETE FROM %s WHERE k=2");
        String select = "SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3";
        beforeAndAfterFlush(() -> assertRows(execute(select), row(1)));
    }

    @Test
    public void testDeletedColumn() throws Throwable
    {
        String select = "SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3";

        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createAnalyzedIndex();
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("DELETE v FROM %s WHERE k = 1");
        assertRows(execute(select));
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple juice')");
        assertRows(execute(select), row(1), row(2));
        execute("DELETE v FROM %s WHERE k = 2");
        beforeAndAfterFlush(() -> assertRows(execute(select), row(1)));
    }

    @Test
    public void testDeletedRowWithPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, n int)");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        createAnalyzedIndex();
        execute("INSERT INTO %s (k, v, n) VALUES (1, 'apple', 0)");
        execute("INSERT INTO %s (k, v, n) VALUES (2, 'apple juice', 0)");
        String select = "SELECT k FROM %s WHERE n = 0 ORDER BY v BM25 OF 'apple' LIMIT 3";
        assertRows(execute(select), row(1), row(2));
        execute("DELETE FROM %s WHERE k=2");
        beforeAndAfterFlush(() -> assertRows(execute(select), row(1)));
    }

    @Test
    public void testRangeRestrictedBM25OnlyQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, n int)");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        createAnalyzedIndex();
        execute("INSERT INTO %s (k, v, n) VALUES (1, 'apple', 0)");
        execute("INSERT INTO %s (k, v, n) VALUES (2, 'apple juice', 0)");
        String select = "SELECT k FROM %s WHERE token(k) > token(1) AND token(k) < token(3) ORDER BY v BM25 OF 'apple' LIMIT 3";
        beforeAndAfterFlush(() -> assertRows(execute(select), row(2)));
    }

    @Test
    public void testRangeRestrictedHybridQuery() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, n int)");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        createAnalyzedIndex();
        execute("INSERT INTO %s (k, v, n) VALUES (1, 'apple', 0)");
        execute("INSERT INTO %s (k, v, n) VALUES (2, 'apple juice', 0)");
        // Insert many unrelated rows so we do search-then-sort
        for (int i = 3; i < 100; i++)
            execute("INSERT INTO %s (k, v, n) VALUES (?, 'apple juice', 1)", i);
        String select = "SELECT k FROM %s WHERE token(k) > token(1) AND token(k) < token(3) " +
                        "AND n = 0 ORDER BY v BM25 OF 'apple' LIMIT 3";
        beforeAndAfterFlush(() -> assertRows(execute(select), row(2)));
    }

    @Test
    public void testSearchThenSortWithPartitionTombstoneInSeparateSSTable()
    {
        // A search-then-sort BM25 query reads each candidate row straight from every sstable. A partition that is
        // present in an sstable only as a partition-level tombstone (deleted after the sstable holding its row was
        // flushed) must be skipped, not fail the query with NoSuchElementException.
        createTable("CREATE TABLE %s (k int, c int, query_lexical_value text, array_contains set<text>, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(array_contains) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        createAnalyzedIndex("query_lexical_value");
        execute("INSERT INTO %s (k, c, query_lexical_value, array_contains) VALUES (1, 1, 'apple', {'target'})");
        execute("INSERT INTO %s (k, c, query_lexical_value, array_contains) VALUES (2, 1, 'apple juice', {'target'}) USING TIMESTAMP 1");
        // Insert many unrelated rows so we do search-then-sort
        for (int i = 3; i < 100; i++)
            execute("INSERT INTO %s (k, c, query_lexical_value, array_contains) VALUES (?, 1, 'apple juice', {'other'})", i);
        flush();

        // The tombstone for k = 2 lands in a second sstable. Live rows on either side of c = 1 keep its lexical
        // index segment spanning the deleted candidate, while the first sstable's collection index still emits (2, 1).
        execute("DELETE FROM %s USING TIMESTAMP 2 WHERE k = 2");
        execute("INSERT INTO %s (k, c, query_lexical_value, array_contains) VALUES (2, 0, 'pear', {'other'}) USING TIMESTAMP 3");
        execute("INSERT INTO %s (k, c, query_lexical_value, array_contains) VALUES (2, 2, 'pear', {'other'}) USING TIMESTAMP 3");
        flush();
        String select = "SELECT k, c FROM %s WHERE array_contains CONTAINS ? " +
                        "ORDER BY query_lexical_value BM25 OF ? LIMIT 3";
        assertEmpty(execute(select, "target", "term-no-document-contains"));
        assertRows(execute(select, "target", "apple"), row(1, 1));

        // Re-inserting the deleted key in a third sstable must not fail either, and the row must come back exactly once
        execute("INSERT INTO %s (k, c, query_lexical_value, array_contains) VALUES (2, 1, 'apple juice', {'target'}) USING TIMESTAMP 4");
        flush();
        assertRows(execute(select, "target", "apple"), row(1, 1), row(2, 1));
        compact();
        assertRows(execute(select, "target", "apple"), row(1, 1), row(2, 1));
    }

    @Test
    public void testSearchThenSortWithRowReinsertedWithoutOrderedColumn() throws Throwable
    {
        // The live row has no value for the BM25 column (deleted and re-inserted without it) while an older sstable
        // still holds and scores the previous value: the stale score must be discarded, not NPE during validation.
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text, n int)");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        createAnalyzedIndex();
        execute("INSERT INTO %s (k, v, n) VALUES (1, 'apple', 0)");
        execute("INSERT INTO %s (k, v, n) VALUES (2, 'apple juice', 0)");
        // Insert many unrelated rows so we do search-then-sort
        for (int i = 3; i < 100; i++)
            execute("INSERT INTO %s (k, v, n) VALUES (?, 'apple juice', 1)", i);
        flush();

        execute("DELETE FROM %s WHERE k = 2");
        execute("INSERT INTO %s (k, n) VALUES (2, 0)");
        String select = "SELECT k FROM %s WHERE n = 0 ORDER BY v BM25 OF 'apple' LIMIT 3";
        beforeAndAfterFlush(() -> assertRows(execute(select), row(1)));
    }

    @Test
    public void testSearchThenSortWithRangeTombstoneBeforeLiveRow()
    {
        createTable("CREATE TABLE %s (k int, c int, v text, n int, PRIMARY KEY (k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        createAnalyzedIndex();
        execute("INSERT INTO %s (k, c, v, n) VALUES (1, 1, 'apple', 0)");
        execute("INSERT INTO %s (k, c, v, n) VALUES (2, 1, 'apple juice', 0)");
        for (int i = 3; i < 100; i++)
            execute("INSERT INTO %s (k, c, v, n) VALUES (?, 1, 'apple juice', 1)", i);

        // The range tombstone is older than the live row, so both are preserved in the sstable.
        execute("DELETE FROM %s USING TIMESTAMP 1 WHERE k = 2 AND c >= 0 AND c <= 2");
        flush();

        String select = "SELECT k, c FROM %s WHERE n = 0 ORDER BY v BM25 OF 'apple' LIMIT 3";
        assertRows(execute(select), row(1, 1), row(2, 1));
    }

    @Test
    public void testTwoIndexesAmbiguousPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");

        createAnalyzedIndex();
        // Create  un-analyzed indexes
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple juice')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'orange juice')");

        // equality predicate is ambiguous (both analyzed and un-analyzed indexes could support it) so it should
        // be rejected
        beforeAndAfterFlush(() -> {
            // Single predicate
            assertInvalidMessage(String.format(EQ_AMBIGUOUS_ERROR, 'v', getIndex(0), getIndex(1), getIndex(0)),
                                 "SELECT k FROM %s WHERE v = 'apple'");

            // AND
            assertInvalidMessage(String.format(EQ_AMBIGUOUS_ERROR, 'v', getIndex(0), getIndex(1), getIndex(0)),
                                 "SELECT k FROM %s WHERE v = 'apple' AND v : 'juice'");

            // OR
            assertInvalidMessage(String.format(EQ_AMBIGUOUS_ERROR, 'v', getIndex(0), getIndex(1), getIndex(0)),
                                 "SELECT k FROM %s WHERE v = 'apple' OR v : 'juice'");
        });
    }

    @Test
    public void testTwoIndexesWithEqualsUnsupported() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        // analyzed index with equals_behavior:unsupported option
        createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                    "WITH OPTIONS = { 'equals_behaviour_when_analyzed': 'unsupported', " +
                    "'index_analyzer':'{\"tokenizer\":{\"name\":\"standard\"},\"filters\":[{\"name\":\"porterstem\"}]}' }");

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple juice')");

        beforeAndAfterFlush(() -> {
            // combining two EQ predicates is not allowed
            assertInvalid("SELECT k FROM %s WHERE v = 'apple' AND v = 'juice'");

            // combining EQ and MATCH predicates is also not allowed (when we're not converting EQ to MATCH)
            assertInvalid("SELECT k FROM %s WHERE v = 'apple' AND v : 'apple'");

            // combining two MATCH predicates is fine
            assertRows(execute("SELECT k FROM %s WHERE v : 'apple' AND v : 'juice'"),
                       row(2));

            // = operator should use un-analyzed index since equals is unsupported in analyzed index
            assertRows(execute("SELECT k FROM %s WHERE v = 'apple'"),
                       row(1));

            // : operator should use analyzed index
            assertRows(execute("SELECT k FROM %s WHERE v : 'apple'"),
                       row(1), row(2));
        });
    }

    @Test
    public void testComplexQueriesWithMultipleIndexes() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v1 text, v2 text, v3 int)");

        // Create mix of analyzed, unanalyzed, and non-text indexes
        createIndex("CREATE CUSTOM INDEX ON %s(v1) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");
        createAnalyzedIndex("v2");
        createIndex("CREATE CUSTOM INDEX ON %s(v3) USING 'org.apache.cassandra.index.sai.StorageAttachedIndex'");

        execute("INSERT INTO %s (k, v1, v2, v3) VALUES (1, 'apple', 'orange juice', 5)");
        execute("INSERT INTO %s (k, v1, v2, v3) VALUES (2, 'apple juice', 'apple', 10)");
        execute("INSERT INTO %s (k, v1, v2, v3) VALUES (3, 'banana', 'grape juice', 5)");

        beforeAndAfterFlush(() -> {
            // Complex query mixing different types of indexes and operators
            assertRows(execute("SELECT k FROM %s WHERE v1 = 'apple' AND v2 : 'juice' AND v3 = 5"),
                       row(1));

            // Mix of AND and OR conditions across different index types
            assertRows(execute("SELECT k FROM %s WHERE v3 = 5 AND (v1 = 'apple' OR v2 : 'apple')"),
                       row(1));

            // Multi-term analyzed query
            assertRows(execute("SELECT k FROM %s WHERE v2 : 'orange juice'"),
                       row(1));

            // Range query with text match
            assertRows(execute("SELECT k FROM %s WHERE v3 >= 5 AND v2 : 'juice'"),
                       row(1), row(3));
        });
    }

    @Test
    public void testMatchingAllowed() throws Throwable
    {
        // match operator should be allowed with BM25 on the same column
        // (seems obvious but exercises a corner case in the internal RestrictionSet processing)
        createSimpleTable();

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");

        beforeAndAfterFlush(() ->
        {
            var result = execute("SELECT k FROM %s WHERE v : 'apple' ORDER BY v BM25 OF 'apple' LIMIT 3");
            assertRows(result, row(1));
        });
    }

    @Test
    public void testUnknownQueryTerm() throws Throwable
    {
        createSimpleTable();

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");

        beforeAndAfterFlush(() ->
                            {
                                var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'orange' LIMIT 1");
                                assertEmpty(result);
                            });
    }

    @Test
    public void testDuplicateQueryTerm() throws Throwable
    {
        createSimpleTable();

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");

        beforeAndAfterFlush(() ->
                            {
                                var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'apple apple' LIMIT 1");
                                assertRows(result, row(1));
                            });
    }

    @Test
    public void testEmptyQuery() throws Throwable
    {
        createSimpleTable();

        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");

        beforeAndAfterFlush(() ->
                            assertInvalidMessage("BM25 query must contain at least one term (perhaps your analyzer is discarding tokens you didn't expect)",
                                                 "SELECT k FROM %s ORDER BY v BM25 OF '+' LIMIT 1"));
    }

    @Test
    public void testTermFrequencyOrdering() throws Throwable
    {
        createSimpleTable();

        // Insert documents with varying frequencies of the term "apple"
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple apple')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'apple apple apple')");

        beforeAndAfterFlush(() ->
        {
            // Results should be ordered by term frequency (highest to lowest)
            var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3");
            assertRows(result,
                       row(3),  // 3 occurrences
                       row(2),  // 2 occurrences
                       row(1)); // 1 occurrence
        });
    }

    @Test
    public void testTermFrequenciesWithOverwrites() throws Throwable
    {
        createSimpleTable();

        // Insert documents with varying frequencies of the term "apple", but overwrite the first term
        // This exercises the code that is supposed to reset frequency counts for overwrites
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple apple')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'apple apple apple')");

        beforeAndAfterFlush(() ->
                            {
                                // Results should be ordered by term frequency (highest to lowest)
                                var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3");
                                assertRows(result,
                                           row(3),  // 3 occurrences
                                           row(2),  // 2 occurrences
                                           row(1)); // 1 occurrence
                            });
    }

    @Test
    public void testDocumentLength() throws Throwable
    {
        createSimpleTable();
        // Create documents with same term frequency but different lengths
        execute("INSERT INTO %s (k, v) VALUES (1, 'test test')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'test test other words here to make it longer')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'test test extremely long document with many additional words to significantly increase the document length while maintaining the same term frequency for our target term')");

        beforeAndAfterFlush(() ->
        {
            // Documents with same term frequency should be ordered by length (shorter first)
            var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'test' LIMIT 3");
            assertRows(result,
                       row(1),
                       row(2),
                       row(3));
        });
    }

    @Test
    public void testMultiTermQueryScoring() throws Throwable
    {
        createSimpleTable();
        // Two terms, but "apple" appears in fewer documents
        execute("INSERT INTO %s (k, v) VALUES (1, 'apple banana')");
        execute("INSERT INTO %s (k, v) VALUES (2, 'apple apple banana')");
        execute("INSERT INTO %s (k, v) VALUES (3, 'apple banana banana')");
        execute("INSERT INTO %s (k, v) VALUES (4, 'apple apple banana banana')");
        execute("INSERT INTO %s (k, v) VALUES (5, 'banana banana')");

        beforeAndAfterFlush(() ->
        {
            var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'apple banana' LIMIT 4");
            assertRows(result,
                       row(2),  // Highest frequency of most important term
                       row(4),  // More mentions of both terms
                       row(1),  // One of each term
                       row(3)); // Low frequency of most important term
        });
    }

    @Test
    public void testIrrelevantRowsScoring() throws Throwable
    {
        createSimpleTable();
        // Insert pizza reviews with varying relevance to "crispy crust"
        execute("INSERT INTO %s (k, v) VALUES (1, 'The pizza had a crispy crust and was delicious')"); // Basic mention
        execute("INSERT INTO %s (k, v) VALUES (2, 'Very crispy crispy crust, perfectly cooked')"); // Emphasized crispy
        execute("INSERT INTO %s (k, v) VALUES (3, 'The crust crust crust was okay, nothing special')"); // Only crust mentions
        execute("INSERT INTO %s (k, v) VALUES (4, 'Super crispy crispy crust crust, best pizza ever!')");  // Most mentions of both
        execute("INSERT INTO %s (k, v) VALUES (5, 'The toppings were good but the pizza was soggy')"); // Irrelevant review

        beforeAndAfterFlush(this::assertIrrelevantRowsCorrect);
    }

    private void assertIrrelevantRowsCorrect()
    {
        var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'crispy crust' LIMIT 5");
        assertRows(result,
                   row(4), // Highest frequency of both terms
                   row(2), // High frequency of 'crispy', one 'crust'
                   row(1)); // One mention of each term
        // Rows 3 and 5 do not contain all terms
    }

    @Test
    public void testIrrelevantRowsWithCompaction()
    {
        // same dataset as testIrrelevantRowsScoring, but split across two sstables
        createSimpleTable();
        disableCompaction();

        execute("INSERT INTO %s (k, v) VALUES (1, 'The pizza had a crispy crust and was delicious')"); // Basic mention
        execute("INSERT INTO %s (k, v) VALUES (2, 'Very crispy crispy crust, perfectly cooked')"); // Emphasized crispy
        flush();

        execute("INSERT INTO %s (k, v) VALUES (3, 'The crust crust crust was okay, nothing special')"); // Only crust mentions
        execute("INSERT INTO %s (k, v) VALUES (4, 'Super crispy crispy crust crust, best pizza ever!')");  // Most mentions of both
        execute("INSERT INTO %s (k, v) VALUES (5, 'The toppings were good but the pizza was soggy')"); // Irrelevant review
        flush();

        assertIrrelevantRowsCorrect();

        compact();
        assertIrrelevantRowsCorrect();

        // Force segmentation and requery
        long original = SegmentBuilder.updateLastValidSegmentRowId(2);
        compact();
        assertIrrelevantRowsCorrect();

        SegmentBuilder.updateLastValidSegmentRowId(original);
    }

    private void createSimpleTable()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createAnalyzedIndex();
    }

    private String createAnalyzedIndex()
    {
        return createAnalyzedIndex("v");
    }

    private String createAnalyzedIndex(String column)
    {
        return createAnalyzedIndex(column, false);
    }

    private String createAnalyzedIndex(String column, boolean lowercase)
    {
        return createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") " +
                           "USING 'org.apache.cassandra.index.sai.StorageAttachedIndex' " +
                           "WITH OPTIONS = {" +
                           "'index_analyzer': '{" +
                           "\"tokenizer\" : {\"name\" : \"standard\"}, " +
                           "\"filters\" : [{\"name\" : \"porterstem\"}" +
                           (lowercase ? ", {\"name\" : \"lowercase\"}]" : "]")
                           + "}'}"
        );
    }

    @Test
    public void testWithPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, p int, v text)");
        createAnalyzedIndex();
        createIndex("CREATE CUSTOM INDEX ON %s(p) USING 'StorageAttachedIndex'");

        // Insert documents with varying frequencies of the term "apple"
        execute("INSERT INTO %s (k, p, v) VALUES (1, 5, 'apple')");
        execute("INSERT INTO %s (k, p, v) VALUES (2, 5, 'apple apple')");
        execute("INSERT INTO %s (k, p, v) VALUES (3, 5, 'apple apple apple')");
        execute("INSERT INTO %s (k, p, v) VALUES (4, 6, 'apple apple apple')");
        execute("INSERT INTO %s (k, p, v) VALUES (5, 7, 'apple apple apple')");

        beforeAndAfterFlush(() ->
        {
            // Results should be ordered by term frequency (highest to lowest)
            var result = execute("SELECT k FROM %s WHERE p = 5 ORDER BY v BM25 OF 'apple' LIMIT 3");
            assertRows(result,
                       row(3),  // 3 occurrences
                       row(2),  // 2 occurrences
                       row(1)); // 1 occurrence
        });
    }

    @Test
    public void testWidePartition() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v text, PRIMARY KEY (k1, k2))");
        createAnalyzedIndex();

        // Insert documents with varying frequencies of the term "apple"
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 1, 'apple')");
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 2, 'apple apple')");
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 3, 'apple apple apple')");

        beforeAndAfterFlush(() ->
        {
            // Results should be ordered by term frequency (highest to lowest)
            var result = execute("SELECT k2 FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3");
            assertRows(result,
                       row(3),  // 3 occurrences
                       row(2),  // 2 occurrences
                       row(1)); // 1 occurrence
        });
    }

    @Test
    public void testWidePartitionWithPkPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, v text, PRIMARY KEY (k1, k2))");
        createAnalyzedIndex();

        // Insert documents with varying frequencies of the term "apple"
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 1, 'apple')");
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 2, 'apple apple')");
        execute("INSERT INTO %s (k1, k2, v) VALUES (0, 3, 'apple apple apple')");
        execute("INSERT INTO %s (k1, k2, v) VALUES (1, 3, 'apple apple apple')");
        execute("INSERT INTO %s (k1, k2, v) VALUES (2, 3, 'apple apple apple')");

        beforeAndAfterFlush(() ->
        {
            // Results should be ordered by term frequency (highest to lowest)
            var result = execute("SELECT k2 FROM %s WHERE k1 = 0 ORDER BY v BM25 OF 'apple' LIMIT 3");
            assertRows(result,
                       row(3),  // 3 occurrences
                       row(2),  // 2 occurrences
                       row(1)); // 1 occurrence
        });
    }

    @Test
    public void testWidePartitionWithPredicate() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, p int, v text, PRIMARY KEY (k1, k2))");
        createAnalyzedIndex();
        createIndex("CREATE CUSTOM INDEX ON %s(p) USING 'StorageAttachedIndex'");

        // Insert documents with varying frequencies of the term "apple"
        execute("INSERT INTO %s (k1, k2, p, v) VALUES (0, 1, 5, 'apple')");
        execute("INSERT INTO %s (k1, k2, p, v) VALUES (0, 2, 5, 'apple apple')");
        execute("INSERT INTO %s (k1, k2, p, v) VALUES (0, 3, 5, 'apple apple apple')");
        execute("INSERT INTO %s (k1, k2, p, v) VALUES (0, 4, 6, 'apple apple apple')");
        execute("INSERT INTO %s (k1, k2, p, v) VALUES (0, 5, 7, 'apple apple apple')");

        beforeAndAfterFlush(() ->
        {
            // Results should be ordered by term frequency (highest to lowest)
            var result = execute("SELECT k2 FROM %s WHERE p = 5 ORDER BY v BM25 OF 'apple' LIMIT 3");
            assertRows(result,
                       row(3),  // 3 occurrences
                       row(2),  // 2 occurrences
                       row(1)); // 1 occurrence
        });
    }

    @Test
    public void testWithPredicateSearchThenOrder() throws Throwable
    {
        QueryController.QUERY_OPT_LEVEL = 0;
        testWithPredicate();
    }

    @Test
    public void testWidePartitionWithPredicateOrderThenSearch() throws Throwable
    {
        QueryController.QUERY_OPT_LEVEL = 1;
        testWidePartitionWithPredicate();
    }

    @Test
    public void testQueryWithNulls() throws Throwable
    {
        createSimpleTable();

        execute("INSERT INTO %s (k, v) VALUES (0, null)");
        execute("INSERT INTO %s (k, v) VALUES (1, 'test document')");
        beforeAndAfterFlush(() ->
        {
            var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'test' LIMIT 1");
            assertRows(result, row(1));
        });
    }

    @Test
    public void testQueryEmptyTable()
    {
        createSimpleTable();
        var result = execute("SELECT k FROM %s ORDER BY v BM25 OF 'test' LIMIT 1");
        assertThat(result).hasSize(0);
    }

    @Test
    public void testBM25RaceConditionConcurrentQueriesInInvertedIndexSearcher() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, v text, PRIMARY KEY (pk))");
        createAnalyzedIndex();

        // Create 3 docs that have the same BM25 score and will be our top docs
        execute("INSERT INTO %s (pk, v) VALUES (1, 'apple apple apple')");
        execute("INSERT INTO %s (pk, v) VALUES (2, 'apple apple apple')");
        execute("INSERT INTO %s (pk, v) VALUES (3, 'apple apple apple')");

        // Now insert a lot of docs that will hit the query, but will be lower in frequency and therefore in score
        for (int i = 4; i < 10000; i++)
            execute("INSERT INTO %s (pk, v) VALUES (?, 'apple apple')", i);

        // Bug only present in sstable
        flush();

        // Trigger many concurrent queries
        final ExecutorService executor = Executors.newFixedThreadPool(10);
        String select = "SELECT pk FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3";
        var futures = new ArrayList<Future<UntypedResultSet>>();
        for (int i = 0; i < 1000; i++)
            futures.add(executor.submit(() -> execute(select)));

        // The top results are always the same rows
        for (Future<UntypedResultSet> future : futures)
            assertRowsIgnoringOrder(future.get(), row(1), row(2), row(3));

        // Shutdown executor
        assertEquals(0, executor.shutdownNow().size());
    }

    @Test
    public void testWildcardSelection()
    {
        createTable("CREATE TABLE %s (k int, c int, v text, PRIMARY KEY (k, c))");
        createAnalyzedIndex();
        execute("INSERT INTO %s (k, c, v) VALUES (1, 1, 'apple')");

        var result = execute("SELECT * FROM %s ORDER BY v BM25 OF 'apple' LIMIT 3");
        assertThat(result).hasSize(1);
    }

    @Test
    public void cannotHaveAggregationOnBM25Query()
    {
        createSimpleTable();

        execute("INSERT INTO %s (k, v) VALUES (1, '4')");
        execute("INSERT INTO %s (k, v) VALUES (2, '3')");
        execute("INSERT INTO %s (k, v) VALUES (3, '2')");
        execute("INSERT INTO %s (k, v) VALUES (4, '1')");

        assertThatThrownBy(() -> execute("SELECT max(v) FROM %s ORDER BY v BM25 OF 'apple' LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);

        assertThatThrownBy(() -> execute("SELECT max(v) FROM %s WHERE k = 1 ORDER BY v BM25 OF 'apple' LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);

        assertThatThrownBy(() -> execute("SELECT * FROM %s GROUP BY k ORDER BY v BM25 OF 'apple' LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);

        assertThatThrownBy(() -> execute("SELECT count(*) FROM %s ORDER BY v BM25 OF 'apple' LIMIT 4"))
                .isInstanceOf(InvalidRequestException.class)
                .hasMessage(SelectStatement.TOPK_AGGREGATION_ERROR);
    }

    @Test
    public void testBM25andFilterz() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, category text, score int, title text, body text)");
        createAnalyzedIndex("body");
        createIndex("CREATE CUSTOM INDEX ON %s (score) USING 'StorageAttachedIndex'");
        insertPrimitiveData();
        beforeAndAfterFlush(
                () -> {
                    // 10 docs have score 3 and 3 of those have "health"
                    var result = execute("SELECT * FROM %s WHERE score = 3 ORDER BY body BM25 OF ? LIMIT 10",
                                         "health");
                    assertThat(result).hasSize(3);

                    // 4 docs have score 2 and one of those has "discussed"
                    result = execute("SELECT * FROM %s WHERE score = 2 ORDER BY body BM25 OF ? LIMIT 10",
                                         "discussed");
                    assertThat(result).hasSize(1);
                });
    }

    @Test
    public void testOrderingSeveralSSTablesWithMapPredicate() throws Throwable
    {
        // Force search-then-sort
        QueryController.QUERY_OPT_LEVEL = 0;
        createTable("CREATE TABLE %s (id int PRIMARY KEY, category text, map_category map<int, int>)");
        createAnalyzedIndex("category", true);
        createIndex("CREATE CUSTOM INDEX ON %s (entries(map_category)) USING 'StorageAttachedIndex'");
        // We don't want compaction to merge the two sstables since they are key to testing this code path.
        disableCompaction();

        // Insert documents so that they all have the same bm25 score and are easy to query across sstables
        for (int i = 0; i < 10; i++)
        {
            execute("INSERT INTO %s (id, category, map_category) VALUES (?, ?, ?)",
                    i, "Health", map(0, i));
            if (i == 4)
                flush();
        }

        // Confirm that the memtable/sstable and sstable/sstable pairings work as expected.
        beforeAndAfterFlush(() -> {
            // Submit a query that will fetch keys from 2 overlapping sstables. The key is that they are overlapping
            // because we have optimizations that will skip keys that are out of the sstable's range. In this case,
            // the actual bm25 data doesn't matter because we are covering the edge case of mapping PrK back to
            // its value here.
            assertRowsIgnoringOrder(execute("SELECT id FROM %s WHERE map_category[0] >= 4 AND map_category[0] <= 6 ORDER BY category BM25 OF 'health' LIMIT 10"),
                                    row(4), row(5), row(6));
        });
    }

    @Test
    public void testErrorMessages()
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, category text, score int, " +
                    "title text, body text, bodyset set<text>, " +
                    "map_category map<int, text>, map_body map<text, text>)");
        createAnalyzedIndex("body", true);
        createAnalyzedIndex("bodyset", true);
        createAnalyzedIndex("map_body", true);

        // Improve message issue CNDB-13514
        assertInvalidMessage("BM25 ordering on column bodyset requires an analyzed index",
                             "SELECT * FROM %s ORDER BY bodyset BM25 OF ? LIMIT 10");

        // Discussion of message incosistency CNDB-13526
        assertInvalidMessage("Ordering on non-clustering column requires each restricted column to be indexed except for fully-specified partition keys",
                             "SELECT * FROM %s WHERE map_body CONTAINS KEY 'Climate' ORDER BY body BM25 OF ? LIMIT 10");
    }

    @Test
    public void testWithLowercase() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, body text)");
        createAnalyzedIndex("body", true);
        execute("INSERT INTO %s (id, body) VALUES (?, ?)", 1, "Hi hi");
        execute("INSERT INTO %s (id, body) VALUES (?, ?)", 2, "hi hi longer");
        executeQuery(Arrays.asList(1, 2), "SELECT * FROM %s ORDER BY body BM25 OF 'hi' LIMIT 4");
    }

    // ID 10: total words = 12, climate occurrences = 4
    // ID 18: total words = 13, climate occurrences = 4
    // ID 0: total words = 16, climate occurrences = 3
    // ID 15: total words = 11, climate occurrences = 2
    // ID 5: total words = 13, climate occurrences = 2
    // ID 11: total words = 12, climate occurrences = 1
    // ID 17: total words = 14, climate occurrences = 1
    private static final List<Integer> CLIMATE_QUERY_RESULTS = Arrays.asList(10, 18, 0, 15, 5, 11, 17);
    private static final List<Integer> CLIMATE_QUERY_SCORE_5_RESULTS = Arrays.asList(10, 18, 0);

    @Test
    public void testCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, category text, score int, " +
                    "title text, body text, bodyset set<text>, " +
                    "map_category map<int, text>, map_body map<text, text>)");
        createAnalyzedIndex("body", true);
        createAnalyzedIndex("bodyset", true);
        createAnalyzedIndex("map_body", true);
        createIndex("CREATE CUSTOM INDEX ON %s (score) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s (category) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s (map_category) USING 'StorageAttachedIndex'");
        createIndex("CREATE CUSTOM INDEX ON %s (KEYS(map_body)) USING 'StorageAttachedIndex'");
        insertCollectionData(this);
        analyzeDataset("climate");
        analyzeDataset("health");

        runThenFlushThenCompact(
        () -> {
            executeQuery(CLIMATE_QUERY_RESULTS, "SELECT * FROM %s  ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(CLIMATE_QUERY_SCORE_5_RESULTS, "SELECT * FROM %s WHERE score = 5 ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(CLIMATE_QUERY_RESULTS, "SELECT * FROM %s WHERE bodyset CONTAINS 'climate' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(Arrays.asList(15, 5, 11, 17), "SELECT * FROM %s WHERE bodyset CONTAINS 'health' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(CLIMATE_QUERY_RESULTS, "SELECT * FROM %s WHERE map_category CONTAINS 'Climate' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(Arrays.asList(18, 15, 5, 11, 17), "SELECT * FROM %s WHERE map_category CONTAINS 'Health' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(CLIMATE_QUERY_RESULTS, "SELECT * FROM %s WHERE map_body CONTAINS 'Climate' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(Arrays.asList(10, 18, 15, 5, 11, 17), "SELECT * FROM %s WHERE map_body CONTAINS 'health' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");
            executeQuery(Arrays.asList(10, 18, 15, 5, 11, 17), "SELECT * FROM %s WHERE map_body CONTAINS KEY 'Health' ORDER BY body BM25 OF ? LIMIT 10",
                         "climate");

            // ID 3: total words = 15, health occurrences = 3
            // ID 11: total words = 12, health occurrences = 2
            // ID 5: total words = 13, health occurrences = 2
            // ID 8: total words = 13, health occurrences = 2
            // ID 17: total words = 14, health occurrences = 2
            // ID 13: total words = 11, health occurrences = 1
            // ID 15: total words = 11, health occurrences = 1
            executeQuery(Arrays.asList(5, 15), "SELECT * FROM %s WHERE score > 3 ORDER BY body BM25 OF ? LIMIT 10",
                         "health");
            executeQuery(Arrays.asList(3, 11, 8, 17, 13), "SELECT * FROM %s WHERE category = 'Health' " +
                                                          "ORDER BY body BM25 OF ? LIMIT 10",
                         "Health");
            executeQuery(Arrays.asList(3, 11, 8, 17, 13), "SELECT * FROM %s WHERE score <= 3 AND category = 'Health' " +
                                                          "ORDER BY body BM25 OF ? LIMIT 10",
                         "health");
        });
    }

    @Test
    public void testOrderingSeveralSegments() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, category text, score int," +
                    "title text, body text)");
        createAnalyzedIndex("body", true);
        createIndex("CREATE CUSTOM INDEX ON %s (score) USING 'StorageAttachedIndex'");
        executeQuery(Collections.emptyList(), "SELECT * FROM %s  ORDER BY body BM25 OF ? LIMIT 10",
                     "climate");
        insertPrimitiveData(0, 10);
        flush();
        insertPrimitiveData(10, 20);

        // The same result as in testCollections above
        executeQuery(CLIMATE_QUERY_RESULTS,
                     Arrays.asList(0, 10, 5, 18, 15, 11, 17),
                     "SELECT * FROM %s  ORDER BY body BM25 OF ? LIMIT 10",
                     "climate");
        executeQuery(CLIMATE_QUERY_SCORE_5_RESULTS,
                     Arrays.asList(0, 10, 18),
                     "SELECT * FROM %s WHERE score = 5 ORDER BY body BM25 OF ? LIMIT 10",
                     "climate");

        flush();
        executeQuery(CLIMATE_QUERY_RESULTS,
                     Arrays.asList(10, 18, 0, 5, 15, 11, 17),
                     "SELECT * FROM %s  ORDER BY body BM25 OF ? LIMIT 10",
                     "climate");
        executeQuery(CLIMATE_QUERY_SCORE_5_RESULTS, "SELECT * FROM %s WHERE score = 5 ORDER BY body BM25 OF ? LIMIT 10",
                     "climate");

        compact();
        executeQuery(CLIMATE_QUERY_RESULTS, "SELECT * FROM %s  ORDER BY body BM25 OF ? LIMIT 10",
                     "climate");
        executeQuery(CLIMATE_QUERY_SCORE_5_RESULTS, "SELECT * FROM %s WHERE score = 5 ORDER BY body BM25 OF ? LIMIT 10",
                     "climate");
    }

    @Test
    public void testOrderByPartitionKey()
    {
        createTable("CREATE TABLE %s (k text PRIMARY KEY)");
        assertInvalidMessage("Cannot create secondary index on the only partition key column k",
                             "CREATE CUSTOM INDEX ON %s(k) USING 'StorageAttachedIndex'");
    }

    @Test
    public void testOrderByPartitionKeyComponent()
    {
        createTable("CREATE TABLE %s (k1 int, k2 text, PRIMARY KEY((k1, k2)))");
        createIndex("CREATE CUSTOM INDEX ON %s(k2) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k1, k2) VALUES (?, ?)";
        Object[] row1 = row(1, "orange");
        Object[] row2 = row(2, "orange apple");
        Object[] row3 = row(3, "orange orange");
        Object[] row4 = row(4, "banana apple");
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        assertRows(execute("SELECT * FROM %s WHERE k2:'orange' LIMIT 10"), row2, row1, row3);
        assertRejectsBM25OnNonRegularColumn("SELECT * FROM %s ORDER BY k2 BM25 OF 'orange' LIMIT 10", "partition key", "k2");
    }

    @Test
    public void testOrderByClusteringKey()
    {
        createTable("CREATE TABLE %s (k int, c text, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(c) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, c) VALUES (?, ?)";
        Object[] row1 = row(1, "orange");
        Object[] row2 = row(2, "orange apple");
        Object[] row3 = row(3, "orange orange");
        Object[] row4 = row(4, "banana apple");
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        assertCannotBeRestrictedByClustering("SELECT * FROM %s WHERE c:'orange' LIMIT 10", "c");
        assertRejectsBM25OnNonRegularColumn("SELECT * FROM %s ORDER BY c BM25 OF 'orange' LIMIT 10", "clustering", "c");
    }

    @Test
    public void testOrderByClusteringKeyComponent()
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 text, PRIMARY KEY(k, c1, c2))");
        createIndex("CREATE CUSTOM INDEX ON %s(c2) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, c1, c2) VALUES (?, ?, ?)";
        Object[] row1 = row(1, 1, "orange");
        Object[] row2 = row(1, 2, "orange apple");
        Object[] row3 = row(2, 1, "orange orange");
        Object[] row4 = row(2, 2, "banana apple");
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        assertCannotBeRestrictedByClustering("SELECT * FROM %s WHERE c2:'orange' LIMIT 10", "c2");
        assertRejectsBM25OnNonRegularColumn("SELECT * FROM %s ORDER BY c2 BM25 OF 'orange' LIMIT 10", "clustering", "c2");
    }

    @Test
    public void testOrderByStaticColumn()
    {
        createTable("CREATE TABLE %s (k int, c int, s text static, PRIMARY KEY(k, c))");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, c, s) VALUES (?, ?, ?)";
        Object[] row1 = row(1, 0, "orange");
        Object[] row2 = row(2, 0, "orange apple");
        Object[] row3 = row(3, 0, "orange orange");
        Object[] row4 = row(4, 0, "banana apple");
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        assertRows(execute("SELECT k, c, s FROM %s WHERE s:'orange' LIMIT 10"), row1, row2, row3);
        assertRejectsBM25OnNonRegularColumn("SELECT k, c, s FROM %s ORDER BY s BM25 OF 'orange' LIMIT 10", "static", "s");
    }

    @Test
    public void testOrderByListColumn()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<text>)");
        createIndex("CREATE CUSTOM INDEX ON %s(l) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, l) VALUES (?, ?)";
        Object[] row1 = row(1, list("orange"));
        Object[] row2 = row(2, list("orange apple"));
        Object[] row3 = row(3, list("orange orange"));
        Object[] row4 = row(4, list("banana apple"));
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        assertRows(execute("SELECT * FROM %s WHERE l CONTAINS 'orange' LIMIT 10"), row1, row2, row3);
        assertInvalidMessage("Invalid STRING constant (orange) for \"l\" of type list<text>",
                             "SELECT * FROM %s ORDER BY l BM25 OF 'orange' LIMIT 10");
    }

    @Test
    public void testOrderByFrozenListColumn()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l frozen<list<text>>)");
        assertInvalidMessage("Cannot use an analyzer on full(l) because it's a frozen collection.",
                             "CREATE CUSTOM INDEX ON %s(FULL(l)) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
    }

    @Test
    public void testOrderBySetColumn()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s set<text>)");
        createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, s) VALUES (?, ?)";
        Object[] row1 = row(1, set("orange"));
        Object[] row2 = row(2, set("orange apple"));
        Object[] row3 = row(3, set("orange orange"));
        Object[] row4 = row(4, set("banana apple"));
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        assertRows(execute("SELECT * FROM %s WHERE s CONTAINS 'orange' LIMIT 10"), row1, row2, row3);
        assertInvalidMessage("Invalid STRING constant (orange) for \"s\" of type set<text>",
                             "SELECT * FROM %s ORDER BY s BM25 OF 'orange' LIMIT 10");
    }

    @Test
    public void testOrderByFrozenSetColumn()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, s frozen<set<text>>)");
        assertInvalidMessage("Cannot use an analyzer on full(s) because it's a frozen collection.",
                             "CREATE CUSTOM INDEX ON %s(FULL(s)) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
    }

    @Test
    public void testOrderByMapColumn()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m map<text,text>)");
        createIndex("CREATE CUSTOM INDEX ON %s(KEYS(m)) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        createIndex("CREATE CUSTOM INDEX ON %s(VALUES(m)) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");

        String insert = "INSERT INTO %s (k, m) VALUES (?, ?)";
        Object[] row1 = row(1, map("orange", "banana apple"));
        Object[] row2 = row(2, map("orange apple", "orange orange"));
        Object[] row3 = row(3, map("orange orange", "orange apple"));
        Object[] row4 = row(4, map("banana apple", "orange"));
        execute(insert, row1);
        execute(insert, row2);
        execute(insert, row3);
        execute(insert, row4);

        assertRows(execute("SELECT * FROM %s WHERE m CONTAINS KEY 'orange' LIMIT 10"), row1, row2, row3);
        assertRows(execute("SELECT * FROM %s WHERE m CONTAINS 'orange' LIMIT 10"), row2, row4, row3);
        assertInvalidMessage("Invalid STRING constant (orange) for \"m\" of type map<text, text>",
                             "SELECT * FROM %s ORDER BY m BM25 OF 'orange' LIMIT 10");
    }

    @Test
    public void testOrderByFrozenMapColumn()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, m frozen<map<text, text>>)");
        assertInvalidMessage("Cannot use an analyzer on full(m) because it's a frozen collection.",
                             "CREATE CUSTOM INDEX ON %s(FULL(m)) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
    }

    private void assertCannotBeRestrictedByClustering(String query, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(format(SingleColumnRestriction.AnalyzerMatchesRestriction.CANNOT_BE_RESTRICTED_BY_CLUSTERING_ERROR, column));
    }

    private void assertRejectsBM25OnNonRegularColumn(String query, String columnType, String column)
    {
        Assertions.assertThatThrownBy(() -> execute(query))
                  .isInstanceOf(InvalidRequestException.class)
                  .hasMessage(format(StatementRestrictions.BM25_ORDERING_REQUIRES_REGULAR_COLUMN_MESSAGE, columnType, column));
    }

    public final static Object[][] DATASET =
    {
    { 0, "Climate", 5, "Climate change is a pressing issue. Climate patterns are shifting globally. Scientists study climate data daily." },
    { 1, "Technology", 3, "Technology is advancing. New technology in AI and robotics is groundbreaking." },
    { 2, "Economy", 4, "The economy is recovering. Economy experts are optimistic. However, the global economy still faces risks." },
    { 3, "Health", 3, "Health is wealth. Health policies need to be improved to ensure better public health outcomes." },
    { 4, "Education", 2, "Education is the foundation of success. Online education is booming." },
    { 5, "Climate", 4, "Climate and health are closely linked. Climate affects air quality and health outcomes." },
    { 6, "Education", 3, "Technology and education go hand in hand. EdTech is revolutionizing education through technology." },
    { 7, "Economy", 3, "The global economy is influenced by technology. Fintech is a key part of the economy today." },
    { 8, "Health", 3, "Education and health programs must be prioritized. Health education is vital in schools." },
    { 9, "Mixed", 3, "Technology, economy, and education are pillars of development." },
    { 10, "Climate", 5, "Climate climate climate. It's everywhere. Climate drives political and economic decisions." },
    { 11, "Health", 2, "Health concerns rise with climate issues. Health organizations are sounding the alarm." },
    { 12, "Economy", 3, "The economy is fluctuating. Uncertainty looms over the economy." },
    { 13, "Health", 3, "Cutting-edge technology is transforming healthcare. Healthtech merges health and technology." },
    { 14, "Education", 2, "Education reforms are underway. Education experts suggest holistic changes." },
    { 15, "Climate", 4, "Climate affects the economy and health. Climate events cost billions annually." },
    { 16, "Technology", 3, "Technology is the backbone of the modern economy. Without technology, economic growth stagnates." },
    { 17, "Health", 2, "Health is discussed less than economy or climate or technology, but health matters deeply." },
    { 18, "Climate", 5, "Climate change, climate policies, climate research—climate is the buzzword of our time." },
    { 19, "Mixed", 3, "Investments in education and technology will shape the future of the global economy." }
    };

    private void analyzeDataset(String term)
    {
        for (Object[] row : DATASET)
        {
            String body = (String) row[DATASET_BODY_COLUMN];
            String[] words = PATTERN.split(body.toLowerCase());

            long totalWords = words.length;
            long termCount = Arrays.stream(words)
                                   .filter(word -> word.equals(term))
                                   .count();

            if (termCount > 0)
                System.out.printf("            // ID %d: total words = %d, %s occurrences = %d%n",
                                  (Integer) row[0], totalWords, term, termCount);
        }
    }

    private void insertPrimitiveData()
    {
        insertPrimitiveData(0, DATASET.length);
    }

    private void insertPrimitiveData(int start, int end)
    {
        for (int i = start; i < end; i++)
        {
            Object[] row = DATASET[i];
            execute(
            "INSERT INTO %s (id, category, score, body) VALUES (?, ?, ?, ?)",
            row[0],
            row[1],
            row[2],
            row[3]
            );
        }
    }

    public static void insertCollectionData(SAITester tester)
    {
        for (int row = 0; row < DATASET.length; row++)
            tester.execute(
            "INSERT INTO %s (id, category, score, body, bodyset, map_category, map_body) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            DATASET[row][0],
            DATASET[row][1],
            DATASET[row][2],
            DATASET[row][3],
            generateSetColumnValue(row),
            generateMapValueIntKey(row),
            generateMapValueTextKey(row)
            );
    }

    private static HashSet<String> generateSetColumnValue(int row)
    {
        HashSet<String> setColumnValue = new HashSet<>();
        for (int i = 0; i < row % 3 + 1; i++)
            setColumnValue.add((String) DATASET[row - i][3]);
        return setColumnValue;
    }

    private static HashMap<Integer, String> generateMapValueIntKey(int row)
    {
        HashMap<Integer, String> mapIntKey = new HashMap<>();
        for (int j = 0; j <= row && j < 3; j++)
            mapIntKey.putIfAbsent((Integer) DATASET[row - j][2], (String) DATASET[row - j][1]);
        return mapIntKey;
    }

    private static HashMap<String, String> generateMapValueTextKey(int row)
    {
        HashMap<String, String> mapTextKey = new HashMap<>();
        for (int j = 0; j <= row && j < 3; j++)
            mapTextKey.putIfAbsent((String) DATASET[row - j][1], (String) DATASET[row - j][3]);
        return mapTextKey;
    }

    private void executeQuery(List<Integer> expected, String query, Object... values) throws Throwable
    {
        assertResult(execute(query, values), expected);
        prepare(query);
        assertResult(execute(query, values), expected);
    }

    private void executeQuery(List<Integer> expected, List<Integer> expectedEC, String query, Object... values) throws Throwable
    {
        if (testVersion == Version.EC)
            executeQuery(expectedEC, query, values);
        else
            executeQuery(expected, query, values);
    }

    private void assertResult(UntypedResultSet result, List<Integer> expected)
    {
        Assertions.assertThat(result).hasSize(expected.size());
        var ids = result.stream()
                        .map(row -> row.getInt("id"))
                        .collect(Collectors.toList());
        Assertions.assertThat(ids).isEqualTo(expected);
    }
}
