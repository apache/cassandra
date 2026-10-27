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

package org.apache.cassandra.db.filter;

import java.util.Arrays;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.Ordering;
import org.apache.cassandra.db.ReadQuery;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.Redaction;
import org.apache.cassandra.db.virtual.AbstractVirtualTable;
import org.apache.cassandra.db.virtual.SimpleDataSet;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.db.virtual.VirtualTable;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.schema.TableMetadata;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.cql3.restrictions.StatementRestrictions.BM25_ORDERING_REQUIRES_ANALYZED_INDEX_MESSAGE;
import static org.apache.cassandra.cql3.restrictions.StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE;
import static org.apache.cassandra.cql3.restrictions.StatementRestrictions.NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE;

/// Tests for the optimization of row filters built from restrictions that:
/// * Replaces conjunctions with always-false branches by an optimized always-false filter element.
/// * Removes always-false branches from disjunctions.
/// * Removes unnecessary conjunction/disjunction nesting.
@RunWith(Parameterized.class)
public class RowFilterEmptyRangesTest extends CQLTester
{
    /// Whether to index the queried columns.
    @Parameterized.Parameter
    public boolean index;

    /// Whether to flush the table data before querying.
    @Parameterized.Parameter(1)
    public boolean flush;

    @Parameterized.Parameters(name = "index={0} flush={1}")
    public static Iterable<Object[]> parameters()
    {
        return Arrays.asList(new Object[]{ false, false },
                             new Object[]{ false, true },
                             new Object[]{ true, false },
                             new Object[]{ true, true });
    }

    @Test
    public void testEmptyRanges()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a int, b int, c int)");
        maybeIndex("a", "b", "c");

        for (int i = -5; i < 5; i++)
        {
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, i, i);
        }
        maybeFlush();

        // without any row filter (nothing to optimize)
        assertIsNotEmptyQuery("SELECT * FROM %s");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE k = 0");

        // with a single-restriction row filter (nothing to optimize)
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 0");

        // with multiple expressions in the same column that don't need optimization
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 0 AND a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 1 AND a < 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 1 AND a <= 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 1 AND a < 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 1 AND a <= 2");

        // with multiple expressions in the same column that do need optimization
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= 0 AND a < 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > 0 AND a <= 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a < 1 AND a > 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > 2 AND a < 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a < 1 AND a >= 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > 2 AND a <= 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a <= 1 AND a > 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= 2 AND a < 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a <= 1 AND a >= 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= 2 AND a <= 1");

        // unsatisfiable conjunctions should be removed from disjunctions, even though the entire query is not empty
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 OR (b < 1 AND b > 2)", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 OR (b < 1 AND b >= 2)", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 OR (b <= 1 AND b >= 2)", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 OR (b <= 1 AND b >= 2)", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 0 OR (b < 1 AND b > 2)", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 0 OR (b < 1 AND b >= 2)", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 0 OR (b <= 1 AND b >= 2)", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 0 OR (b <= 1 AND b >= 2)", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a < 0 OR (b < 1 AND b > 2)", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a < 0 OR (b < 1 AND b >= 2)", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a < 0 OR (b <= 1 AND b >= 2)", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a < 0 OR (b <= 1 AND b >= 2)", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 0 OR (b < 1 AND b > 2)", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 0 OR (b < 1 AND b >= 2)", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 0 OR (b <= 1 AND b >= 2)", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a >= 0 OR (b <= 1 AND b >= 2)", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 0 OR (b < 1 AND b > 2)", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 0 OR (b < 1 AND b >= 2)", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 0 OR (b <= 1 AND b >= 2)", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 0 OR (b <= 1 AND b >= 2)", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b > 2) OR a = 0", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b >= 2) OR a = 0", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a = 0", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a = 0", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b > 2) OR a > 0", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b >= 2) OR a > 0", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a > 0", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a > 0", "a > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b > 2) OR a < 0", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b >= 2) OR a < 0", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a < 0", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a < 0", "a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b > 2) OR a >= 0", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b >= 2) OR a >= 0", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a >= 0", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a >= 0", "a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b > 2) OR a <= 0", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b >= 2) OR a <= 0", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a <= 0", "a <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b <= 1 AND b >= 2) OR a <= 0", "a <= 0");

        // Fragmented intervals
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a > 0 AND a < 2) OR (a > 4 AND a < 6)");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a > 0 OR a < 2) AND (a > 4 OR a < 6)");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a < 0 AND a > 2) OR (a > 4 AND a < 6)", "a > 4 AND a < 6");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a > 0 AND a < 2) OR (a < 4 AND a > 6)", "a > 0 AND a < 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE (a < 0 AND a > 2) OR (a < 4 AND a > 6)");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a < 0 OR a > 2) AND (a < 4 OR a > 6)");

        // some more convoluted cases
        assertIsEmptyQuery("SELECT * FROM %s WHERE (a < 1 AND b > 1) AND (a > 2 AND b < 2)");
        assertIsEmptyQuery("SELECT * FROM %s WHERE (a > 2 AND b < 2) AND (a < 1 AND b > 1)");
        assertIsEmptyQuery("SELECT * FROM %s WHERE (b > 2 AND b < 1) OR (a > 2 AND a < 1)");
        assertIsEmptyQuery("SELECT * FROM %s WHERE (a > 2 AND a < 1) OR (b > 2 AND b < 1)");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b < 1 AND b > 2) OR (a = 0 AND b = 0)", "a = 0 AND b = 0", "b = 0 AND a = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a = 0 AND b = 0) OR (b < 1 AND b > 2)", "a = 0 AND b = 0", "b = 0 AND a = 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a <= 0 AND a >= 0 AND a != 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a <= 0 AND a >= 0 AND a != 0 AND a != 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a <= 0 AND a >= 0 AND a != 1 AND a != 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a != 1 AND a != 0 AND a <= 0 AND a >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 0 AND a >= 0 AND a != 1");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 0 AND a >= 0 AND a != 1 AND a != 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a <= 1 AND a >= 0 AND a != 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a < 1 AND a >= 0 AND a != 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a != 0 AND a != 1");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 AND (b = 0 OR (c < 1 AND c > 2))", "a = 0 AND (b = 0)");

        // Similar cases that don't need simplification because they are forbidden by pre-binding validation
        assertInvalidMessage("a cannot be restricted by more than one relation if it includes an Equal",
                             "SELECT * FROM %s WHERE a = 0 AND a != 0");
        assertInvalidMessage("Column \"a\" cannot be restricted by both an equality and an inequality relation",
                             "SELECT * FROM %s WHERE a NOT IN (0, 1) AND a IN (1, 2)");
        assertInvalidMessage("Column \"a\" cannot be restricted by both an equality and an inequality relation",
                             "SELECT * FROM %s WHERE a > 0 AND a IN (1, 2)");
        assertInvalidMessage("More than one restriction was found for the start bound on a",
                             "SELECT * FROM %s WHERE a > 0 AND a < 2 AND a > 4 AND a < 6");
        assertInvalidMessage("More than one restriction was found for the start bound on a",
                             "SELECT * FROM %s WHERE (a > 0 AND a < 2) AND (a > 4 AND a < 6)");
    }

    @Test
    public void testEmptyRangesPartitionKey()
    {
        createTable("CREATE TABLE %s (k1 int, k2 int, c int, a int, PRIMARY KEY ((k1, k2), c))");
        maybeIndex("k1");

        for (int i = -5; i < 5; i++)
        {
            execute("INSERT INTO %s (k1, k2, c, a) VALUES (?, ?, ?, ?)", i, i, i, i);
        }
        maybeFlush();

        assertIsNotEmptyQuery("SELECT * FROM %s WHERE k1 > 1 AND k1 < 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE k1 >= 1 AND k1 <= 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE k1 < 1 AND k1 > 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE k1 >= 0 AND k1 < 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE k1 > 0 AND k1 <= 0");
    }

    @Test
    public void testEmptyRangesClusteringKey()
    {
        testEmptyRangesClusteringKey(Ordering.Direction.ASC);
        testEmptyRangesClusteringKey(Ordering.Direction.DESC);
    }

    private void testEmptyRangesClusteringKey(Ordering.Direction direction)
    {
        createTable("CREATE TABLE %s (k int, c int, a int, PRIMARY KEY (k, c)) WITH CLUSTERING ORDER BY (c " + direction + ')');
        maybeIndex("c");

        for (int i = -5; i < 5; i++)
        {
            execute("INSERT INTO %s (k, c, a) VALUES (?, ?, ?)", i, i, i);
        }
        maybeFlush();

        assertIsNotEmptyQuery("SELECT * FROM %s WHERE c > 1 AND c < 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE c >= 1 AND c <= 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE c < 1 AND c > 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE c >= 0 AND c < 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE c > 0 AND c <= 0");
    }

    @Test
    public void testEmptyRangesBigint()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a bigint, b bigint)");
        maybeIndex("a", "b");

        for (long i = -5; i < 5; i++)
        {
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", (int) i, i, i);
        }
        maybeFlush();

        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 1 AND a < 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > 2 AND a < 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= 0 AND a < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0 OR (b < 1 AND b > 2)", "a = 0");
    }

    @Test
    public void testEmptyRangesFloat()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a float, b float)");
        maybeIndex("a", "b");

        for (int i = -5; i < 5; i++)
        {
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i * 2, (float) i, (float) i);
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i * 2 + 1, i + 0.5f, i + 0.5f);
        }
        maybeFlush();

        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > 1.0 AND a < 2.0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > 2.0 AND a < 1.0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= 0.0 AND a < 0.0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = 0.0 OR (b < 1.0 AND b > 2.0)", "a = 0.0");
    }

    @Test
    public void testEmptyRangesText()
    {
        Assume.assumeFalse("SAI doesn't support ranges on text columns", index);
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a text, b text)");

        for (int i = -5; i < 5; i++)
        {
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, String.valueOf(i), String.valueOf(i));
        }
        maybeFlush();

        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > '1' AND a < '2'");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > '2' AND a < '1'");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= '0' AND a < '0'");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = '0' OR (b < '1' AND b > '2')", "a = '0'");
    }

    @Test
    public void testEmptyRangesDate()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a date, b date)");
        maybeIndex("a", "b");

        execute("INSERT INTO %s (k, a, b) VALUES (1, '2000-01-01', '2000-01-01')");
        execute("INSERT INTO %s (k, a, b) VALUES (2, '2025-01-01', '2025-01-01')");
        execute("INSERT INTO %s (k, a, b) VALUES (3, '2099-12-31', '2099-12-31')");
        maybeFlush();

        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a > '2000-01-01' AND a < '2099-12-31'");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > '2099-12-31' AND a < '2000-01-01'");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a >= '2000-01-01' AND a < '2000-01-01'");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a = '2025-01-01' OR (b < '2000-01-01' AND b > '2099-12-31')", "a = '2025-...");
    }

    @Test
    public void testEmptyRangesInet()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a inet, b int)");
        maybeIndex("a", "b");

        execute("INSERT INTO %s (k, a, b) VALUES (1, '10.0.0.1', 0)");
        execute("INSERT INTO %s (k, a, b) VALUES (2, '200.0.0.1', 0)");
        execute("INSERT INTO %s (k, a, b) VALUES (3, '201.0.0.1', 0)");
        execute("INSERT INTO %s (k, a, b) VALUES (4, '1::1', 0)");
        execute("INSERT INTO %s (k, a, b) VALUES (5, '2001::1', 0)");
        execute("INSERT INTO %s (k, a, b) VALUES (6, '3000::1', 0)");
        maybeFlush();

        // Empty ranges on inet columns are not detected when building the row filter,
        // because SAI orders inet values differently from CQL (see CNDB-12978).
        // Therefore, ranges that are empty for CQL can be not empty for SAI, and they shouldn't be discarded.
        // Without an index the rows are filtered with the CQL order, so nothing is returned.
        // With an index the rows are found with the SAI order, so the rows between both bounds are returned.
        assertIsNotEmptyQueryIfIndexed("SELECT * FROM %s WHERE a > '200.0.0.1' AND a < '2001::1'");
        assertIsNotEmptyQueryIfIndexed("SELECT * FROM %s WHERE a >= '200.0.0.1' AND a < '2001::1'");
        assertIsNotEmptyQueryIfIndexed("SELECT * FROM %s WHERE a > '200.0.0.1' AND a <= '2001::1'");
        assertIsNotEmptyQueryIfIndexed("SELECT * FROM %s WHERE a >= '200.0.0.1' AND a <= '2001::1'");

        // Ranges that are empty with both orderings are not detected either, since inet columns are skipped entirely.
        // In that case SAI has to deal with those empty ranges on its own.
        assertIsNotEmptyQueryIfIndexed("SELECT * FROM %s WHERE a > '10.0.0.2' AND a < '10.0.0.1'");
        assertIsNotEmptyQueryIfIndexed("SELECT k FROM %s WHERE a >= '10.0.0.1' AND a < '10.0.0.1'");
        assertIsNotEmptyQueryIfIndexed("SELECT k FROM %s WHERE a > '10.0.0.1' AND a <= '10.0.0.1'");
        assertIsNotEmptyQueryIfIndexed("SELECT k FROM %s WHERE a > '10.0.0.1' AND a < '10.0.0.1'");

        // Equal bounds with both sides inclusive are not empty, with or without index.
        assertIsNotEmptyQuery("SELECT k FROM %s WHERE a >= '10.0.0.1' AND a <= '10.0.0.1'");

        // Skipping the optimization for inet shouldn't prevent the optimization for other columns.
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > '200.0.0.1' AND a < '2001::1' AND b < 1 AND b > 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a > '10.0.0.2' AND a < '10.0.0.1' AND b < 1 AND b > 2");
        assertIsNotEmptyQueryIfIndexed("SELECT * FROM %s WHERE (a > '200.0.0.1' AND a < '2001::1') OR (b < 1 AND b > 2)", "a > '200.0... AND a < '2001:...");
        assertIsNotEmptyQueryIfIndexed("SELECT * FROM %s WHERE (a > '10.0.0.2' AND a < '10.0.0.1') OR (b < 1 AND b > 2)", "a > '10.0.... AND a < '10.0....");
    }

    @Test
    public void testEmptyRangesMap()
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, a map<text, int>, b map<text, int>)");
        maybeIndex("entries(a)", "entries(b)");

        for (int i = -5; i < 5; i++)
        {
            execute("INSERT INTO %s (k, a, b) VALUES (?, ?, ?)", i, map("k1", i, "k2", i), map("k1", i, "k2", i));
        }
        maybeFlush();

        // without any row filter (nothing to optimize)
        assertIsNotEmptyQuery("SELECT * FROM %s");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE k = 0");

        // with a single-restriction row filter (nothing to optimize)
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] <= 0");

        // with multiple expressions in the same map element that don't need optimization
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] >= 0 AND a['k1'] <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] > 1 AND a['k1'] < 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] > 1 AND a['k1'] <= 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] >= 1 AND a['k1'] < 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] >= 1 AND a['k1'] <= 2");

        // with multiple expressions on different keys of the same map that don't need optimization
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] < 1 AND a['k2'] > 2");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] > 2 AND a['k2'] < 1");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] >= 0 AND a['k2'] < 0");

        // with multiple expressions in the same map element that do need optimization
        assertIsEmptyQuery("SELECT * FROM %s WHERE a['k1'] >= 0 AND a['k1'] < 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a['k1'] > 0 AND a['k1'] <= 0");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a['k1'] < 1 AND a['k1'] > 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a['k1'] > 2 AND a['k1'] < 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a['k1'] < 1 AND a['k1'] >= 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a['k1'] > 2 AND a['k1'] <= 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a['k1'] <= 1 AND a['k1'] > 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a['k1'] >= 2 AND a['k1'] < 1");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a['k1'] <= 1 AND a['k1'] >= 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE a['k1'] >= 2 AND a['k1'] <= 1");

        // unsatisfiable conjunctions should be removed from disjunctions, even though the entire query is not empty
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] = 0 OR (b['k1'] < 1 AND b['k1'] > 2)", "a['k1'] = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] > 0 OR (b['k1'] < 1 AND b['k1'] > 2)", "a['k1'] > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] < 0 OR (b['k1'] < 1 AND b['k1'] > 2)", "a['k1'] < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] >= 0 OR (b['k1'] < 1 AND b['k1'] > 2)", "a['k1'] >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE a['k1'] <= 0 OR (b['k1'] < 1 AND b['k1'] > 2)", "a['k1'] <= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b['k1'] < 1 AND b['k1'] > 2) OR a['k1'] = 0", "a['k1'] = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b['k1'] < 1 AND b['k1'] > 2) OR a['k1'] > 0", "a['k1'] > 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b['k1'] < 1 AND b['k1'] > 2) OR a['k1'] < 0", "a['k1'] < 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b['k1'] < 1 AND b['k1'] > 2) OR a['k1'] >= 0", "a['k1'] >= 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b['k1'] < 1 AND b['k1'] > 2) OR a['k1'] <= 0", "a['k1'] <= 0");

        // Fragmented intervals
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a['k1'] > 0 AND a['k1'] < 2) OR (a['k1'] > 4 AND a['k1'] < 6)");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a['k1'] > 0 OR a['k1'] < 2) AND (a['k1'] > 4 OR a['k1'] < 6)");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a['k1'] < 0 AND a['k1'] > 2) OR (a['k1'] > 4 AND a['k1'] < 6)", "a['k1'] > 4 AND a['k1'] < 6");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a['k1'] > 0 AND a['k1'] < 2) OR (a['k1'] < 4 AND a['k1'] > 6)", "a['k1'] > 0 AND a['k1'] < 2");
        assertIsEmptyQuery("SELECT * FROM %s WHERE (a['k1'] < 0 AND a['k1'] > 2) OR (a['k1'] < 4 AND a['k1'] > 6)");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a['k1'] < 0 OR a['k1'] > 2) AND (a['k1'] < 4 OR a['k1'] > 6)");

        // some more convoluted cases
        assertIsEmptyQuery("SELECT * FROM %s WHERE (a['k1'] < 1 AND b['k1'] > 1) AND (a['k1'] > 2 AND b['k1'] < 2)");
        assertIsEmptyQuery("SELECT * FROM %s WHERE (a['k1'] > 2 AND b['k1'] < 2) AND (a['k1'] < 1 AND b['k1'] > 1)");
        assertIsEmptyQuery("SELECT * FROM %s WHERE (b['k1'] > 2 AND b['k1'] < 1) OR (a['k1'] > 2 AND a['k1'] < 1)");
        assertIsEmptyQuery("SELECT * FROM %s WHERE (a['k1'] > 2 AND a['k1'] < 1) OR (b['k1'] > 2 AND b['k1'] < 1)");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (b['k1'] < 1 AND b['k1'] > 2) OR (a['k1'] = 0 AND b['k1'] = 0)", "a['k1'] = 0 AND b['k1'] = 0", "b['k1'] = 0 AND a['k1'] = 0");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE (a['k1'] = 0 AND b['k1'] = 0) OR (b['k1'] < 1 AND b['k1'] > 2)", "a['k1'] = 0 AND b['k1'] = 0", "b['k1'] = 0 AND a['k1'] = 0");
        // Similar cases that don't need simplification because they are forbidden by pre-binding validation
        assertInvalidMessage("Column \"a\" cannot be restricted by both an inequality relation and \"CONTAINS(values=[], keys=[], entryKeys=[], entryValues=[])\"",
                             "SELECT * FROM %s WHERE a['k1'] <= 0 AND a['k1'] >= 0 AND a['k1'] != 0");
        assertInvalidMessage("More than one restriction was found for the start bound on a",
                             "SELECT * FROM %s WHERE a['k1'] > 0 AND a['k1'] < 2 AND a['k1'] > 4 AND a['k1'] < 6 ALLOW FILTERING");
        assertInvalidMessage("More than one restriction was found for the start bound on a",
                             "SELECT * FROM %s WHERE (a['k1'] > 0 AND a['k1'] < 2) AND (a['k1'] > 4 AND a['k1'] < 6) ALLOW FILTERING");
    }

    @Test
    public void testVirtualTable()
    {
        Assume.assumeFalse(index); // virtual tables don't support indexes
        Assume.assumeFalse(flush); // virtual tables don't support flushing

        TableMetadata metadata = TableMetadata.builder(KEYSPACE, "vt")
                                              .kind(TableMetadata.Kind.VIRTUAL)
                                              .addPartitionKeyColumn("k", Int32Type.instance)
                                              .addRegularColumn("a", Int32Type.instance)
                                              .addRegularColumn("b", Int32Type.instance)
                                              .addRegularColumn("c", InetAddressType.instance)
                                              .build();

        SimpleDataSet data = new SimpleDataSet(metadata);
        data.row(0).column("a", 0).column("b", 0).column("c", InetAddresses.forString("10.0.0.1"))
            .row(1).column("a", 1).column("b", 1).column("c", InetAddresses.forString("200.0.0.1"))
            .row(2).column("a", 2).column("b", 2).column("c", InetAddresses.forString("2001::1"));

        VirtualTable table = new AbstractVirtualTable(metadata)
        {
            @Override
            public DataSet data()
            {
                return data;
            }
        };
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace("vk", ImmutableList.of(table)));

        assertIsEmptyQuery("SELECT * FROM vk.vt WHERE a < 1 AND a > 1");
        assertIsEmptyQuery("SELECT * FROM vk.vt WHERE a <= 1 AND a > 1");
        assertIsEmptyQuery("SELECT * FROM vk.vt WHERE a < 1 AND a >= 1");
        assertIsNotEmptyQuery("SELECT * FROM vk.vt WHERE a <= 1 AND a >= 1");
        assertIsNotEmptyQuery("SELECT * FROM vk.vt WHERE a = 0 OR (b < 1 AND b > 1)", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM vk.vt WHERE a = 0 OR (b < 1 AND b >= 1)", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM vk.vt WHERE a = 0 OR (b <= 1 AND b > 1)", "a = 0");
        assertIsNotEmptyQuery("SELECT * FROM vk.vt WHERE a = 0 OR (b <= 1 AND b >= 1)");

        assertIsEmptyQuery("SELECT * FROM vk.vt WHERE c < '10.0.0.1' AND c > '10.0.0.2'");
        assertIsNotEmptyQuery("SELECT * FROM vk.vt WHERE a = 0 OR (c < '10.0.0.1' AND c > '10.0.0.2')", "a = 0");
    }

    @Test
    public void testTopK()
    {
        Assume.assumeTrue(index);

        createTable("CREATE TABLE %s (k int PRIMARY KEY, n int, s text, v vector<float, 2>)");
        String nIdx = createIndex("CREATE CUSTOM INDEX ON %s(n) USING 'StorageAttachedIndex'");
        String sIdx = createIndex("CREATE CUSTOM INDEX ON %s(s) USING 'StorageAttachedIndex' WITH OPTIONS = { 'index_analyzer': 'standard' }");
        String vIdx = createIndex("CREATE CUSTOM INDEX ON %s(v) USING 'StorageAttachedIndex'");

        for (int i = 1; i <= 10; i++)
        {
            execute("INSERT INTO %s (k, n, s, v) VALUES (?, ?, ?, ?)",
                    i, i, String.valueOf(i), vector((float) i, (float) i));
        }
        maybeFlush();

        // generic ORDER BY
        assertIsNotEmptyQuery("SELECT * FROM %s ORDER BY n LIMIT 10");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE n > 0 ORDER BY n LIMIT 10");
        String unsatisfiableQuery = "SELECT * FROM %s WHERE n < 0 AND n > 1 ORDER BY n LIMIT 10";
        assertIsEmptyQuery(unsatisfiableQuery);
        assertInvalidMessage("Top-K queries cannot be run with an offset", unsatisfiableQuery + " OFFSET 1");
        assertInvalidMessage(IndexHints.NON_INCLUDABLE_INDEXES_ERROR,
                             unsatisfiableQuery + " WITH included_indexes={" + nIdx + '}');
        assertInvalidMessage(IndexHints.NON_INCLUDABLE_INDEXES_ERROR,
                             unsatisfiableQuery + " WITH included_indexes={" + sIdx + '}');
        assertInvalidMessage(IndexHints.NON_INCLUDABLE_INDEXES_ERROR,
                             unsatisfiableQuery + " WITH included_indexes={" + vIdx + '}');
        assertInvalidMessage(String.format(NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 'n'),
                             unsatisfiableQuery + " WITH excluded_indexes={" + nIdx + '}');
        assertIsEmptyQuery(unsatisfiableQuery + " WITH excluded_indexes={" + sIdx + '}');
        assertIsEmptyQuery(unsatisfiableQuery + " WITH excluded_indexes={" + vIdx + '}');
        assertInvalidMessage(String.format(NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 'n'),
                             unsatisfiableQuery + " WITH excluded_indexes={*}");

        // ANN
        assertIsNotEmptyQuery("SELECT * FROM %s ORDER BY v ANN OF [1, 1] LIMIT 10");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE n > 0 ORDER BY v ANN OF [1, 1] LIMIT 10");
        unsatisfiableQuery = "SELECT * FROM %s WHERE n < 0 AND n > 1 ORDER BY v ANN OF [1, 1] LIMIT 10";
        assertIsEmptyQuery(unsatisfiableQuery);
        assertInvalidMessage("Top-K queries cannot be run with an offset",
                             unsatisfiableQuery + " OFFSET 1");
        assertIsEmptyQuery(unsatisfiableQuery + " WITH ann_options = {'rerank_k': 0}");
        assertInvalidMessage("Invalid rerank_k value 5 greater than 0 and less than limit 10",
                             unsatisfiableQuery + " WITH ann_options = {'rerank_k': 5}");
        assertInvalidMessage("Invalid value 'bad' for option 'hybrid_sort_order'",
                             unsatisfiableQuery + " WITH optimizer_options = {'hybrid_sort_order': 'bad'}");
        assertInvalidMessage(IndexHints.NON_INCLUDABLE_INDEXES_ERROR,
                             unsatisfiableQuery + " WITH included_indexes={" + vIdx + '}');
        assertInvalidMessage(IndexHints.NON_INCLUDABLE_INDEXES_ERROR,
                             unsatisfiableQuery + " WITH included_indexes={" + nIdx + '}');
        assertInvalidMessage(IndexHints.NON_INCLUDABLE_INDEXES_ERROR,
                             unsatisfiableQuery + " WITH included_indexes={" + sIdx + '}');
        assertInvalidMessage(String.format(NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 'v'),
                             unsatisfiableQuery + " WITH excluded_indexes={" + vIdx + '}');
        assertInvalidMessage(NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             unsatisfiableQuery + " WITH excluded_indexes={" + nIdx + '}');
        assertIsEmptyQuery(unsatisfiableQuery + " WITH excluded_indexes={" + sIdx + '}');
        assertInvalidMessage(String.format(NON_CLUSTER_ORDERING_REQUIRES_INDEX_MESSAGE, 'v'),
                             unsatisfiableQuery + " WITH excluded_indexes={*}");

        // BM25
        assertIsNotEmptyQuery("SELECT * FROM %s ORDER BY s BM25 OF '0' LIMIT 10");
        assertIsNotEmptyQuery("SELECT * FROM %s WHERE n > 0 ORDER BY s BM25 OF '0' LIMIT 10");
        unsatisfiableQuery = "SELECT * FROM %s WHERE n < 0 AND n > 1 ORDER BY s BM25 OF '0' LIMIT 10";
        assertIsEmptyQuery(unsatisfiableQuery);
        assertInvalidMessage("Top-K queries cannot be run with an offset",
                             unsatisfiableQuery + " OFFSET 1");
        assertInvalidMessage(IndexHints.NON_INCLUDABLE_INDEXES_ERROR,
                             unsatisfiableQuery + " WITH included_indexes={" + nIdx + '}');
        assertInvalidMessage(IndexHints.NON_INCLUDABLE_INDEXES_ERROR,
                             unsatisfiableQuery + " WITH included_indexes={" + sIdx + '}');
        assertInvalidMessage(IndexHints.NON_INCLUDABLE_INDEXES_ERROR,
                             unsatisfiableQuery + " WITH included_indexes={" + vIdx + '}');
        assertInvalidMessage(NON_CLUSTER_ORDERING_REQUIRES_ALL_RESTRICTED_NON_PARTITION_KEY_COLUMNS_INDEXED_MESSAGE,
                             unsatisfiableQuery + " WITH excluded_indexes={" + nIdx + '}');
        assertInvalidMessage(String.format(BM25_ORDERING_REQUIRES_ANALYZED_INDEX_MESSAGE, 's'),
                             unsatisfiableQuery + " WITH excluded_indexes={" + sIdx + '}');
        assertIsEmptyQuery(unsatisfiableQuery + " WITH excluded_indexes={" + vIdx + '}');
        assertInvalidMessage(String.format(BM25_ORDERING_REQUIRES_ANALYZED_INDEX_MESSAGE, 's'),
                             unsatisfiableQuery + " WITH excluded_indexes={*}");
    }

    private void maybeIndex(String... columns)
    {
        if (!index)
            return;

        for (String column : columns)
            createIndex("CREATE CUSTOM INDEX ON %s(" + column + ") USING 'StorageAttachedIndex'");
    }

    private void maybeFlush()
    {
        if (flush)
            flush();
    }

    /// Verifies that the specified query produces an empty row filter.
    ///
    /// @param query A CQL query.
    private void assertIsEmptyQuery(String query)
    {
        if (index)
        {
            doAssertIsEmptyQuery(query);

            if (!query.toUpperCase().contains("ORDER BY"))
                doAssertIsEmptyQuery(query + " ALLOW FILTERING WITH excluded_indexes = {*}");
        }
        else
        {
            doAssertIsEmptyQuery(query + " ALLOW FILTERING");
        }
    }

    private void doAssertIsEmptyQuery(String query)
    {
        assertEmpty(execute(query));

        ReadQuery readQuery = parseReadQuery(query);
        Assertions.assertThat(readQuery.isEmpty()).isTrue();

        RowFilter rowFilter = readQuery.rowFilter();
        Assertions.assertThat(rowFilter)
                  .isEqualTo(RowFilter.REJECT_ALL)
                  .matches(RowFilter::isEmpty);

        // the unsatisfiable row filter shouldn't be sent to replicas; the serializer should reject it.
        Assertions.assertThatThrownBy(() -> RowFilter.serializer.serialize(rowFilter, new DataOutputBuffer(), MessagingService.current_version))
                  .isInstanceOf(AssertionError.class)
                  .hasMessageContaining("unsatisfiable row filter");
    }

    /// Verifies that the specified query produces a not empty row filter,
    /// with one of the specified CQL representations.
    ///
    /// @param query A CQL query.
    /// @param optimizedFilters The acceptable CQL representations of the row optimized filter.
    /// There can be more than one because the order of the restrictions is not deterministic.
    private void assertIsNotEmptyQuery(String query, @Nullable String... optimizedFilters)
    {
        if (index)
        {
            doAssertIsNotEmptyQuery(query, optimizedFilters);

            if (!query.toUpperCase().contains("ORDER BY"))
                doAssertIsNotEmptyQuery(query + " ALLOW FILTERING WITH excluded_indexes = {*}", optimizedFilters);
        }
        else
        {
            doAssertIsNotEmptyQuery(query + " ALLOW FILTERING", optimizedFilters);
        }
    }

    private void doAssertIsNotEmptyQuery(String query, @Nullable String... optimizedFilters)
    {
        execute(query);

        ReadQuery readQuery = parseReadQuery(query);
        Assertions.assertThat(readQuery.isEmpty()).isFalse();

        RowFilter rowFilter = readQuery.rowFilter();
        Assertions.assertThat(rowFilter)
                  .matches(f -> f != RowFilter.REJECT_ALL);

        if (optimizedFilters != null && optimizedFilters.length > 0)
            Assertions.assertThat(rowFilter.toCQLString(Redaction.NONE)).isIn((Object[]) optimizedFilters);
    }

    /// Verifies that the specified query produces a not empty row filter when there is an index,
    /// with one of the specified CQL representations, or an empty row filter if there is no index.
    ///
    /// @param query A CQL query.
    /// @param optimizedFilters The acceptable CQL representations of the row optimized filter.
    /// There can be more than one because the order of the restrictions is not deterministic.
    private void assertIsNotEmptyQueryIfIndexed(String query, @Nullable String... optimizedFilters)
    {
        if (index)
        {
            doAssertIsNotEmptyQuery(query, optimizedFilters);
            doAssertIsEmptyQuery(query + " ALLOW FILTERING WITH excluded_indexes = {*}");
        }
        else
        {
            doAssertIsEmptyQuery(query + " ALLOW FILTERING");
        }
    }
}
