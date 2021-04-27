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

package org.apache.cassandra.guardrails;


import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.lang.String.format;

/**
 * Tests the guardrail for the max number of items in collections.
 */
public class GuardrailItemsPerCollectionTest extends GuardrailWarningOnSSTableWriteTester
{
    private static final int THRESHOLD = 4;
    private static final String SSTABLE_WRITE_WARN_MESSAGE = String.format(
    "Detected collection <redacted> with %d items", THRESHOLD + 1);

    private long defaultItemsPerCollection;
    private boolean defaultReadBeforeWriteListOperationsEnabled;

    @Before
    public void before()
    {
        defaultItemsPerCollection = config().items_per_collection_warn_threshold;
        config().items_per_collection_warn_threshold = (long) THRESHOLD;

        defaultReadBeforeWriteListOperationsEnabled = config().read_before_write_list_operations_enabled;
        config().read_before_write_list_operations_enabled = true;
    }

    @After
    public void after()
    {
        config().items_per_collection_warn_threshold = defaultItemsPerCollection;
        config().read_before_write_list_operations_enabled = defaultReadBeforeWriteListOperationsEnabled;
    }

    @Test
    public void testConfigValidation()
    {
        testValidationOfStrictlyPositiveProperty((c, v) -> c.items_per_collection_warn_threshold = v,
                                                 "items_per_collection_warn_threshold");
    }

    @Test
    public void testSetSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<text>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set(0));
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", set(1));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", set(THRESHOLD));
        assertNotWarnedOnFlush();

        assertWarnedOnClient("INSERT INTO %s (k, v) VALUES (5, ?)", set(THRESHOLD + 1));
        assertWarnedOnFlush();
    }

    @Test
    public void testFrozenSetSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<set<text>>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set(0));
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", set(1));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", set(THRESHOLD));
        assertWarnedOnClient("INSERT INTO %s (k, v) VALUES (5, ?)", set(THRESHOLD + 1));

        // frozen collections size is not checked during sstable write
        assertNotWarnedOnFlush();
    }

    @Test
    public void testSetSizeWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<text>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", set(1));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", set(1, THRESHOLD));
        assertNotWarnedOnFlush();

        assertWarnedOnClient("INSERT INTO %s (k, v) VALUES (0, ?)", set(THRESHOLD + 1));
        assertValid("UPDATE %s SET v = v - ? WHERE k = 0", set(1));
        assertNotWarnedOnFlush();

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set(1));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 1", set(1, THRESHOLD + 1));
        assertWarnedOnFlush();
    }

    @Test
    public void testSetSizeAfterCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<text>)");
        disableCompaction();

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", set(1));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", set(1, THRESHOLD));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list(THRESHOLD));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v - ? WHERE k = 1", set(1));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set(THRESHOLD));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 1", set(1, THRESHOLD + 1));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact();

        assertValid("DELETE v FROM %s WHERE k = 1");
        assertNotWarnedOnCompact();
    }

    @Test
    public void testListSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<text>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", list(1));
        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", list(THRESHOLD - 1));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", list(THRESHOLD));
        assertNotWarnedOnFlush();

        assertWarnedOnClient("INSERT INTO %s (k, v) VALUES (5, ?)", list(THRESHOLD + 1));
        assertWarnedOnFlush();
    }

    @Test
    public void testFrozenListSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<list<text>>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", list(1));
        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", list(THRESHOLD - 1));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", list(THRESHOLD));
        assertWarnedOnClient("INSERT INTO %s (k, v) VALUES (5, ?)", list(THRESHOLD + 1));

        // frozen collections size is not checked during sstable write
        assertNotWarnedOnFlush();
    }

    @Test
    public void testListSizeWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<text>)");
        disableCompaction();

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", list(1));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", list(1, THRESHOLD));
        assertNotWarnedOnFlush();

        assertWarnedOnClient("INSERT INTO %s (k, v) VALUES (0, ?)", set(THRESHOLD + 1));
        assertValid("UPDATE %s SET v[?] = null WHERE k = 0", THRESHOLD);
        assertNotWarnedOnFlush();

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list(1));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 1", list(1, THRESHOLD + 1));
        assertWarnedOnFlush();

        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", list(1));
        assertValid("UPDATE %s SET v = ? + v WHERE k = 2", list(1, THRESHOLD + 1));
        assertWarnedOnFlush();
    }

    @Test
    public void testListSizeAfterCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<text>)");
        disableCompaction();

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", list(1));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", list(1, THRESHOLD));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list(THRESHOLD));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v[?] = null WHERE k = 1", 0);
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", list(1));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 2", list(1, THRESHOLD + 1));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact();

        assertValid("DELETE v[1] FROM %s WHERE k = 2");
        assertNotWarnedOnCompact();

        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", list(1));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = ? + v WHERE k = 3", list(1, THRESHOLD + 1));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact();
    }

    @Test
    public void testMapSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<text, text>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", map());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(1));
        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", map(THRESHOLD));
        assertNotWarnedOnFlush();

        assertWarnedOnClient("INSERT INTO %s (k, v) VALUES (4, ?)", map(THRESHOLD + 1));
        assertWarnedOnFlush();
    }

    @Test
    public void testFrozenMapSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<map<text, text>>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", map());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(1));
        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", map(THRESHOLD));
        assertWarnedOnClient("INSERT INTO %s (k, v) VALUES (4, ?)", map(THRESHOLD + 1));

        // frozen collections size is not checked during sstable write
        assertNotWarnedOnFlush();
    }

    @Test
    public void testMapSizeWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<text, text>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", map(1));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", map(1, THRESHOLD));
        assertNotWarnedOnFlush();

        assertWarnedOnClient("INSERT INTO %s (k, v) VALUES (1, ?)", map(THRESHOLD + 1));
        assertValid("UPDATE %s SET v = v - ? WHERE k = 1", set(1));
        assertNotWarnedOnFlush();

        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(1));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 2", map(1, THRESHOLD + 1));
        assertWarnedOnFlush();
    }

    @Test
    public void testMapSizeAfterCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<text, text>)");
        disableCompaction();

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", map(1));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", map(1, THRESHOLD));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", map(THRESHOLD));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v - ? WHERE k = 1", set(1));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(1));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 2", map(1, THRESHOLD + 1));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact();

        assertValid("DELETE v FROM %s WHERE k = 2");
        assertNotWarnedOnCompact();
    }

    @Test
    public void testMultipleCollections() throws Throwable
    {
        createTable("CREATE TABLE %s (" +
                    "   k int PRIMARY KEY, " +
                    "   s set<text>," +
                    "   l list<text>," +
                    "   m map<text, text>," +
                    "   fs frozen<set<text>>," +
                    "   fl frozen<list<text>>," +
                    "   fm frozen<map<text, text>>" +
                    ")");

        // the guardrail won't be triggered when the combined size of all the collections in a row is over the threshold
        assertValid("INSERT INTO %s (k, s, l, m, fs, fl, fm) VALUES (0, ?, ?, ?, ?, ?, ?)",
                    set(THRESHOLD), list(THRESHOLD), map(THRESHOLD),
                    set(THRESHOLD), list(THRESHOLD), map(THRESHOLD));
        assertNotWarnedOnFlush();

        // the guardrail will produce a log message for each column exceeding the threshold, not just for the first one
        assertWarns(Arrays.asList(format("Detected collection s with %d items", THRESHOLD + 1),
                                  format("Detected collection l with %d items", THRESHOLD + 2),
                                  format("Detected collection m with %d items", THRESHOLD + 3),
                                  format("Detected collection fs with %d items", THRESHOLD + 4),
                                  format("Detected collection fl with %d items", THRESHOLD + 5),
                                  format("Detected collection fm with %d items", THRESHOLD + 6)),
                    "INSERT INTO %s (k, s, l, m, fs, fl, fm) VALUES (1, ?, ?, ?, ?, ?, ?)",
                    set(THRESHOLD + 1), list(THRESHOLD + 2), map(THRESHOLD + 3),
                    set(THRESHOLD + 4), list(THRESHOLD + 5), map(THRESHOLD + 6));

        // only the non frozen collections will produce a warning during sstable write
        assertWarnedOnSSTableWrite(false,
                                   format("Detected collection <redacted> with %d items", THRESHOLD + 1),
                                   format("Detected collection <redacted> with %d items", THRESHOLD + 2),
                                   format("Detected collection <redacted> with %d items", THRESHOLD + 3));
    }

    @Test
    public void testCompositePartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 text, v set<text>, PRIMARY KEY((k1, k2)))");

        assertValid("INSERT INTO %s (k1, k2, v) VALUES (0, 'a', ?)", set(1));
        assertNotWarnedOnFlush();

        assertValid("INSERT INTO %s (k1, k2, v) VALUES (1, 'a', ?)", set(THRESHOLD));
        assertNotWarnedOnFlush();

        assertWarnedOnClient("INSERT INTO %s (k1, k2, v) VALUES (2, 'c', ?)", set(THRESHOLD + 1));
        assertWarnedOnFlush();
    }

    @Test
    public void testCompositeClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 text, v set<text>, PRIMARY KEY(k, c1, c2))");

        assertValid("INSERT INTO %s (k, c1, c2, v) VALUES (1, 10, 'a', ?)", set(1));
        assertNotWarnedOnFlush();

        assertValid("INSERT INTO %s (k, c1, c2, v) VALUES (2, 20, 'b', ?)", set(THRESHOLD));
        assertNotWarnedOnFlush();

        assertWarnedOnClient("INSERT INTO %s (k, c1, c2, v) VALUES (3, 30, 'c', ?)", set(THRESHOLD + 1));
        assertWarnedOnFlush();
    }

    @Test
    public void testSuperUser() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<text>)");

        // regular user should be warned
        assertWarnedOnClient("INSERT INTO %s (k, v) VALUES (1, ?)", set(THRESHOLD + 1));

        // super user shouldn't be warned
        useSuperUser();
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", set(THRESHOLD + 1));

        // sstable should produces warnings because the keyspace is not internal, regardless of the user
        assertWarnedOnSSTableWrite(false,
                                   format("Detected collection <redacted> with %d items", THRESHOLD + 1),
                                   format("Detected collection <redacted> with %d items", THRESHOLD + 1));
    }

    private static Set<Integer> set(int numElements)
    {
        return set(0, numElements);
    }

    private static Set<Integer> set(int startInclusive, int endExclusive)
    {
        return collection(startInclusive, endExclusive, Collectors.toSet());
    }

    private static List<Integer> list(int numElements)
    {
        return list(0, numElements);
    }

    private static List<Integer> list(int startInclusive, int endExclusive)
    {
        return collection(startInclusive, endExclusive, Collectors.toList());
    }

    private static Map<Integer, Integer> map(int numElements)
    {
        return map(0, numElements);
    }

    private static Map<Integer, Integer> map(int startInclusive, int endExclusive)
    {
        return collection(startInclusive, endExclusive, Collectors.toMap(x -> x, x -> x));
    }

    private static <R, A> R collection(int startInclusive, int endExclusive, Collector<Integer, A, R> collector)
    {
        return IntStream.range(startInclusive, endExclusive).boxed().collect(collector);
    }

    private void assertWarnedOnClient(String query, Object... args) throws Throwable
    {
        String warning = String.format("Detected collection v with %d items", THRESHOLD + 1);
        assertWarns(Collections.singletonList(warning), query, args);
    }

    private void assertWarnedOnFlush()
    {
        assertWarnedOnFlush(SSTABLE_WRITE_WARN_MESSAGE);
    }

    private void assertWarnedOnCompact()
    {
        assertWarnedOnCompact(SSTABLE_WRITE_WARN_MESSAGE);
    }
}
