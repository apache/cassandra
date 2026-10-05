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

import java.math.BigDecimal;
import java.util.Collection;
import java.util.stream.Collectors;

import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.index.sai.SAITester;
import org.apache.cassandra.index.sai.SAIUtil;
import org.apache.cassandra.index.sai.disk.format.Version;

/**
 * Tests that range queries that cannot match anything, i.e. ranges with the lower bound above the upper bound and
 * ranges with equal bounds and at least one exclusive side, return no rows regardless of whether the data is in the
 * memtable, on disk, or both.
 */
@RunWith(Parameterized.class)
public class EmptyRangeQueryTest extends SAITester
{
    @Parameterized.Parameter
    public Version version;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Version.ALL.stream().map(v -> new Object[]{ v }).collect(Collectors.toList());
    }

    @Before
    public void setup() throws Throwable
    {
        SAIUtil.setCurrentVersion(version);
    }

    @After
    public void teardown() throws Throwable
    {
        SAIUtil.resetCurrentVersion();
    }

    @Test
    public void testInvertedRange() throws Throwable
    {
        // The original reproduction: a single row and an inverted range.
        createTable("CREATE TABLE %s (pk bigint, ck bigint, v bigint, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 4)");

        beforeAndAfterFlush(() -> assertEmpty(execute("SELECT * FROM %s WHERE v > 5 AND v < 3")));
    }

    @Test
    public void testRegularColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v bigint, w int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX ON %s(w) USING 'sai'");
        disableCompaction();

        execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, 1, 3, 0)");
        execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, 2, 5, 0)");
        execute("INSERT INTO %s (pk, ck, v, w) VALUES (2, 1, 5, 1)");

        // data in memtable only, then on disk only
        beforeAndAfterFlush(() -> {
            assertEmptyRanges("v", 5L, 3L, "w = 0", "w = 1");
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v >= 5 AND v <= 5"), row(1, 2), row(2, 1));
            assertRows(execute("SELECT pk, ck FROM %s WHERE v >= 5 AND v <= 5 AND w = 0"), row(1, 2));
            assertRows(execute("SELECT pk, ck FROM %s WHERE pk = 1 AND v >= 5 AND v <= 5"), row(1, 2));
        });

        // data in memtable and on disk
        execute("INSERT INTO %s (pk, ck, v, w) VALUES (1, 3, 4, 0)");
        execute("INSERT INTO %s (pk, ck, v, w) VALUES (2, 2, 5, 0)");
        assertEmptyRanges("v", 5L, 3L, "w = 0", "w = 1");
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v >= 5 AND v <= 5"), row(1, 2), row(2, 1), row(2, 2));
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v >= 5 AND v <= 5 AND w = 0"), row(1, 2), row(2, 2));
        assertRows(execute("SELECT pk, ck FROM %s WHERE pk = 1 AND v >= 5 AND v <= 5"), row(1, 2));
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v > 3 AND v < 5"), row(1, 3));
    }

    @Test
    public void testStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, s bigint static, w int, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(s) USING 'sai'");
        createIndex("CREATE INDEX ON %s(w) USING 'sai'");
        disableCompaction();

        execute("INSERT INTO %s (pk, ck, s, w) VALUES (1, 1, 5, 0)");
        execute("INSERT INTO %s (pk, ck, w) VALUES (1, 2, 1)");
        execute("INSERT INTO %s (pk, ck, s, w) VALUES (2, 1, 3, 0)");

        beforeAndAfterFlush(() -> {
            assertEmptyRanges("s", 5L, 3L, "w = 0", "w = 1");
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE s >= 5 AND s <= 5"), row(1, 1), row(1, 2));
            assertRows(execute("SELECT pk, ck FROM %s WHERE s >= 5 AND s <= 5 AND w = 1"), row(1, 2));
        });

        execute("INSERT INTO %s (pk, ck, s, w) VALUES (3, 1, 5, 1)");
        assertEmptyRanges("s", 5L, 3L, "w = 0", "w = 1");
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE s >= 5 AND s <= 5"), row(1, 1), row(1, 2), row(3, 1));
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE s >= 5 AND s <= 5 AND w = 1"), row(1, 2), row(3, 1));
    }

    @Test
    public void testClusteringColumn() throws Throwable
    {
        testClusteringColumn("ASC");
    }

    @Test
    public void testReversedClusteringColumn() throws Throwable
    {
        testClusteringColumn("DESC");
    }

    private void testClusteringColumn(String order) throws Throwable
    {
        // The static column makes the query reach the index even though the clustering slices are empty.
        createTable("CREATE TABLE %s (pk int, ck bigint, s int static, w int, PRIMARY KEY (pk, ck)) WITH CLUSTERING ORDER BY (ck " + order + ')');
        createIndex("CREATE INDEX ON %s(ck) USING 'sai'");
        createIndex("CREATE INDEX ON %s(w) USING 'sai'");
        disableCompaction();

        execute("INSERT INTO %s (pk, ck, s, w) VALUES (1, 3, 0, 0)");
        execute("INSERT INTO %s (pk, ck, w) VALUES (1, 5, 0)");
        execute("INSERT INTO %s (pk, ck, w) VALUES (2, 5, 1)");

        beforeAndAfterFlush(() -> {
            assertEmptyRanges("ck", 5L, 3L, "w = 0", "w = 1");
            assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE ck >= 5 AND ck <= 5"), row(1, 5L), row(2, 5L));
            assertRows(execute("SELECT pk, ck FROM %s WHERE ck >= 5 AND ck <= 5 AND w = 1"), row(2, 5L));
            assertRows(execute("SELECT pk, ck FROM %s WHERE pk = 1 AND ck >= 5 AND ck <= 5"), row(1, 5L));
        });

        execute("INSERT INTO %s (pk, ck, w) VALUES (1, 4, 1)");
        execute("INSERT INTO %s (pk, ck, w) VALUES (3, 5, 1)");
        assertEmptyRanges("ck", 5L, 3L, "w = 0", "w = 1");
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE ck >= 5 AND ck <= 5"), row(1, 5L), row(2, 5L), row(3, 5L));
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE ck >= 5 AND ck <= 5 AND w = 1"), row(2, 5L), row(3, 5L));
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE ck > 3 AND ck < 5"), row(1, 4L));
    }

    @Test
    public void testDecimalColumn() throws Throwable
    {
        // Decimal index terms are truncated and their range bounds are always inclusive in the index, so these exercise
        // a different bound encoding.
        createTable("CREATE TABLE %s (pk int, ck int, d decimal, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(d) USING 'sai'");
        disableCompaction();

        BigDecimal five = new BigDecimal("5.5");
        BigDecimal three = new BigDecimal("3.5");
        execute("INSERT INTO %s (pk, ck, d) VALUES (1, 1, ?)", three);
        execute("INSERT INTO %s (pk, ck, d) VALUES (1, 2, ?)", five);

        beforeAndAfterFlush(() -> {
            assertEmptyRanges("d", five, three);
            assertRows(execute("SELECT pk, ck FROM %s WHERE d >= ? AND d <= ?", five, five), row(1, 2));
        });

        execute("INSERT INTO %s (pk, ck, d) VALUES (2, 1, ?)", five);
        assertEmptyRanges("d", five, three);
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE d >= ? AND d <= ?", five, five), row(1, 2), row(2, 1));
    }

    @Test
    public void testDecimalBoundsEqualAfterTruncation() throws Throwable
    {
        // Guards against a false empty rather than an assertion: ranges whose encoded bounds are equal and inclusive
        // must still match.
        createTable("CREATE TABLE %s (pk int, ck int, d decimal, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(d) USING 'sai'");
        disableCompaction();

        // These only differ beyond the precision of the index terms, so their encoded bounds are equal.
        String prefix = "1." + "0".repeat(80);
        BigDecimal low = new BigDecimal(prefix + '1');
        BigDecimal mid = new BigDecimal(prefix + '2');
        BigDecimal high = new BigDecimal(prefix + '3');
        execute("INSERT INTO %s (pk, ck, d) VALUES (1, 1, ?)", mid);

        beforeAndAfterFlush(() -> {
            assertEmptyRanges("d", high, low);
            assertEmptyRanges("d", mid, low);
            assertRows(execute("SELECT pk, ck FROM %s WHERE d > ? AND d < ?", low, high), row(1, 1));
            assertRows(execute("SELECT pk, ck FROM %s WHERE d >= ? AND d <= ?", mid, mid), row(1, 1));
        });

        execute("INSERT INTO %s (pk, ck, d) VALUES (2, 1, ?)", mid);
        assertEmptyRanges("d", high, low);
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE d > ? AND d < ?", low, high), row(1, 1), row(2, 1));
    }

    @Test
    public void testMapEntries() throws Throwable
    {
        // Each map entry bound becomes its own expression, so this guards the map entries path against regressions
        // and does not reach the empty range check.
        createTable("CREATE TABLE %s (pk int PRIMARY KEY, m map<text, int>)");
        createIndex("CREATE INDEX ON %s(entries(m)) USING 'sai'");
        disableCompaction();

        execute("INSERT INTO %s (pk, m) VALUES (1, {'a': 3})");
        execute("INSERT INTO %s (pk, m) VALUES (2, {'a': 5})");

        beforeAndAfterFlush(() -> {
            assertEmptyRanges("m['a']", 5, 3);
            assertRows(execute("SELECT pk FROM %s WHERE m['a'] >= 5 AND m['a'] <= 5"), row(2));
        });

        execute("INSERT INTO %s (pk, m) VALUES (3, {'a': 5})");
        assertEmptyRanges("m['a']", 5, 3);
        assertRowsIgnoringOrder(execute("SELECT pk FROM %s WHERE m['a'] >= 5 AND m['a'] <= 5"), row(2), row(3));
    }

    @Test
    public void testOrderBy() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v bigint, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        disableCompaction();

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 3)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 5)");

        beforeAndAfterFlush(() -> {
            assertEmptyRangesOrderedBy("v", 5L, 3L);
            assertRows(execute("SELECT pk, ck FROM %s WHERE v >= 5 AND v <= 5 ORDER BY v LIMIT 10"), row(1, 2));
        });

        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 4)");
        assertEmptyRangesOrderedBy("v", 5L, 3L);
        assertRows(execute("SELECT pk, ck FROM %s WHERE v > 3 AND v < 5 ORDER BY v LIMIT 10"), row(2, 1));
    }

    @Test
    public void testOr() throws Throwable
    {
        createTable("CREATE TABLE %s (pk int, ck int, v bigint, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        disableCompaction();

        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 1, 3)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 2, 4)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (1, 3, 5)");
        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 1, 9)");

        beforeAndAfterFlush(() -> {
            assertRows(execute("SELECT pk, ck FROM %s WHERE (v > 5 AND v < 3) OR v = 4"), row(1, 2));
            assertRows(execute("SELECT pk, ck FROM %s WHERE (v > 5 AND v < 5) OR v = 4"), row(1, 2));
            assertRows(execute("SELECT pk, ck FROM %s WHERE v > 8 OR (v > 5 AND v < 3)"), row(2, 1));
        });

        execute("INSERT INTO %s (pk, ck, v) VALUES (2, 2, 4)");
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE (v > 5 AND v < 3) OR v = 4"), row(1, 2), row(2, 2));
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE (v > 5 AND v < 5) OR v = 4"), row(1, 2), row(2, 2));
        assertRows(execute("SELECT pk, ck FROM %s WHERE v > 8 OR (v > 5 AND v < 3)"), row(2, 1));
    }

    @Test
    public void testAnn() throws Throwable
    {
        // The ANN query only reaches the memtable index's empty range check on index versions EB and later; on the
        // earlier vector versions (CA, DB, DC) it passes without the check, so there this is only a correctness check.
        Assume.assumeTrue(version.onOrAfter(Version.JVECTOR_EARLIEST));

        createTable("CREATE TABLE %s (pk int, ck int, v bigint, vec vector<float, 2>, PRIMARY KEY (pk, ck))");
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");
        createIndex("CREATE INDEX ON %s(vec) USING 'sai'");
        disableCompaction();

        execute("INSERT INTO %s (pk, ck, v, vec) VALUES (1, 1, 3, [1.0, 2.0])");
        execute("INSERT INTO %s (pk, ck, v, vec) VALUES (1, 2, 5, [2.0, 3.0])");

        String ann = " ORDER BY vec ANN OF [1.0, 2.0] LIMIT 10";
        beforeAndAfterFlush(() -> {
            assertEmptyRanges("", "v", 5L, 3L, ann);
            assertRows(execute("SELECT pk, ck FROM %s WHERE v >= 5 AND v <= 5" + ann), row(1, 2));
        });

        execute("INSERT INTO %s (pk, ck, v, vec) VALUES (2, 1, 5, [3.0, 4.0])");
        assertEmptyRanges("", "v", 5L, 3L, ann);
        assertRowsIgnoringOrder(execute("SELECT pk, ck FROM %s WHERE v >= 5 AND v <= 5" + ann), row(1, 2), row(2, 1));
    }

    /**
     * Asserts that the ranges on the given column that cannot match anything return no rows, on their own, combined
     * with a partition key restriction and combined with each of the given restrictions on other columns.
     *
     * @param value a value present in the column
     * @param smallerValue a value smaller than {@code value}
     */
    private void assertEmptyRanges(String column, Object value, Object smallerValue, String... otherRestrictions)
    {
        assertEmptyRanges("", column, value, smallerValue, "");
        assertEmptyRanges("pk = 1 AND ", column, value, smallerValue, "");
        for (String restriction : otherRestrictions)
            assertEmptyRanges(restriction + " AND ", column, value, smallerValue, "");
    }

    private void assertEmptyRangesOrderedBy(String column, Object value, Object smallerValue)
    {
        assertEmptyRanges("", column, value, smallerValue, " ORDER BY " + column + " LIMIT 10");
        assertEmptyRanges("", column, value, smallerValue, " ORDER BY " + column + " DESC LIMIT 10");
    }

    private void assertEmptyRanges(String restriction, String column, Object value, Object smallerValue, String suffix)
    {
        String select = "SELECT * FROM %s WHERE " + restriction;
        assertEmpty(execute(select + column + " > ? AND " + column + " < ?" + suffix, value, smallerValue));
        assertEmpty(execute(select + column + " >= ? AND " + column + " <= ?" + suffix, value, smallerValue));
        assertEmpty(execute(select + column + " > ? AND " + column + " < ?" + suffix, value, value));
        assertEmpty(execute(select + column + " >= ? AND " + column + " < ?" + suffix, value, value));
        assertEmpty(execute(select + column + " > ? AND " + column + " <= ?" + suffix, value, value));
    }
}
