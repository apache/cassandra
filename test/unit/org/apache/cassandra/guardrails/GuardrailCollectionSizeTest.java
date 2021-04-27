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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static java.nio.ByteBuffer.allocate;

/**
 * Tests the guardrail for the max size of collections.
 */
public class GuardrailCollectionSizeTest extends GuardrailWarningOnSSTableWriteTester
{
    private static final int THRESHOLD_IN_KB = 1;
    private static final int THRESHOLD_IN_BYTES = THRESHOLD_IN_KB * 1024;
    private static final String SSTABLE_WRITE_WARN_MESSAGE = "Detected collection <redacted> of size";

    private long defaultCollectionSize;

    @Before
    public void before()
    {
        defaultCollectionSize = config().collection_size_warn_threshold_in_kb;
        config().collection_size_warn_threshold_in_kb = (long) THRESHOLD_IN_KB;
    }

    @After
    public void after()
    {
        config().collection_size_warn_threshold_in_kb = defaultCollectionSize;
    }

    @Test
    public void testConfigValidation()
    {
        testValidationOfStrictlyPositiveProperty((c, v) -> c.collection_size_warn_threshold_in_kb = v,
                                                 "collection_size_warn_threshold_in_kb");
    }

    @Test
    public void testSetSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<text>)");
        disableCompaction();

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", set(allocate(1)));
        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", set(allocate(THRESHOLD_IN_BYTES / 2)));
        assertNotWarnedOnFlush();

        assertWarns("INSERT INTO %s (k, v) VALUES (4, ?)", set(allocate(THRESHOLD_IN_BYTES)));
        assertWarnedOnFlush();

        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)",
                    set(allocate(THRESHOLD_IN_BYTES / 2), allocate(THRESHOLD_IN_BYTES / 2 + 1)));
        assertWarnedOnFlush();
    }

    @Test
    public void testFrozenSetSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<set<text>>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", set((allocate(1))));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", set(allocate(THRESHOLD_IN_BYTES / 2)));
        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", set(allocate(THRESHOLD_IN_BYTES)));

        // frozen collections size is not checked during sstable write
        assertNotWarnedOnFlush();
    }

    @Test
    public void testSetSizeWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<text>)");
        disableCompaction();

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", set(allocate(1)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", set(allocate(1)));
        assertNotWarnedOnFlush();

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set(allocate(THRESHOLD_IN_BYTES / 2)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 1", set(allocate(THRESHOLD_IN_BYTES / 2 + 1)));
        assertWarnedOnFlush();
    }

    @Test
    public void testSetSizeAfterCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<text>)");
        disableCompaction();

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", set(allocate(1)));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", set(allocate(1)));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set(allocate(THRESHOLD_IN_BYTES / 2)));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 1", set(allocate(THRESHOLD_IN_BYTES / 2 + 1)));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact();

        assertValid("DELETE v FROM %s WHERE k = 1");
        assertNotWarnedOnCompact();
    }

    @Test
    public void testListSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<text>)");
        disableCompaction();

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", list(allocate(1)));
        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", list(allocate(THRESHOLD_IN_BYTES / 2)));
        assertNotWarnedOnFlush();

        assertWarns("INSERT INTO %s (k, v) VALUES (4, ?)", list(allocate(THRESHOLD_IN_BYTES)));
        assertWarnedOnFlush();

        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)",
                    list(allocate(THRESHOLD_IN_BYTES / 2), allocate(THRESHOLD_IN_BYTES / 2)));
        assertWarnedOnFlush();
    }

    @Test
    public void testFrozenListSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<list<text>>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", set((allocate(1))));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", set(allocate(THRESHOLD_IN_BYTES / 2)));
        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", set(allocate(THRESHOLD_IN_BYTES)));

        // frozen collections size is not checked during sstable write
        assertNotWarnedOnFlush();
    }

    @Test
    public void testListSizeWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<text>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", list(allocate(1)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", list(allocate(1)));
        assertNotWarnedOnFlush();

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list(allocate(THRESHOLD_IN_BYTES / 2)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 1", list(allocate(THRESHOLD_IN_BYTES / 2)));
        assertWarnedOnFlush();

        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", list(allocate(THRESHOLD_IN_BYTES / 2)));
        assertValid("UPDATE %s SET v = ? + v WHERE k = 2", list(allocate(THRESHOLD_IN_BYTES / 2)));
        assertWarnedOnFlush();
    }

    @Test
    public void testListSizeAfterCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<text>)");
        disableCompaction();

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", list(allocate(1)));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", list(allocate(1)));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list(allocate(THRESHOLD_IN_BYTES / 2)));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 1", list(allocate(THRESHOLD_IN_BYTES / 2)));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact();

        assertValid("DELETE v[1] FROM %s WHERE k = 1");
        assertNotWarnedOnCompact();

        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", list(allocate(THRESHOLD_IN_BYTES / 2)));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = ? + v WHERE k = 2", list(allocate(THRESHOLD_IN_BYTES / 2)));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact();
    }

    @Test
    public void testMapSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<text, text>)");
        disableCompaction();

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", map());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(allocate(1), allocate(1)));
        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", map(allocate(THRESHOLD_IN_BYTES / 2), allocate(1)));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", map(allocate(1), allocate(THRESHOLD_IN_BYTES / 2)));
        assertNotWarnedOnFlush();

        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)",
                    map(allocate(THRESHOLD_IN_BYTES / 2), allocate(THRESHOLD_IN_BYTES / 2)));
        assertWarnedOnFlush();

        assertWarns("INSERT INTO %s (k, v) VALUES (6, ?)",
                    map(allocate(1), allocate(THRESHOLD_IN_BYTES / 2),
                        allocate(2), allocate(THRESHOLD_IN_BYTES / 2)));
        assertWarnedOnFlush();

        assertWarns("INSERT INTO %s (k, v) VALUES (7, ?)",
                    map(allocate(THRESHOLD_IN_BYTES / 2), allocate(1),
                        allocate(THRESHOLD_IN_BYTES / 2 + 1), allocate(1)));
        assertWarnedOnFlush();

        assertWarns("INSERT INTO %s (k, v) VALUES (8, ?)", map(allocate(1), allocate(THRESHOLD_IN_BYTES)));
        assertWarnedOnFlush();

        assertWarns("INSERT INTO %s (k, v) VALUES (9, ?)", map(allocate(THRESHOLD_IN_BYTES), allocate(1)));
        assertWarnedOnFlush();
    }

    @Test
    public void testFrozenMapSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<map<text, text>>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", map());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(allocate(1), allocate(1)));
        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", map(allocate(THRESHOLD_IN_BYTES / 2), allocate(1)));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", map(allocate(1), allocate(THRESHOLD_IN_BYTES / 2)));
        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)",
                    map(allocate(THRESHOLD_IN_BYTES / 2), allocate(THRESHOLD_IN_BYTES / 2)));
        assertWarns("INSERT INTO %s (k, v) VALUES (6, ?)",
                    map(allocate(1), allocate(THRESHOLD_IN_BYTES / 2),
                        allocate(2), allocate(THRESHOLD_IN_BYTES / 2)));
        assertWarns("INSERT INTO %s (k, v) VALUES (7, ?)",
                    map(allocate(THRESHOLD_IN_BYTES / 2), allocate(1),
                        allocate(THRESHOLD_IN_BYTES / 2 + 1), allocate(1)));
        assertWarns("INSERT INTO %s (k, v) VALUES (8, ?)", map(allocate(1), allocate(THRESHOLD_IN_BYTES)));
        assertWarns("INSERT INTO %s (k, v) VALUES (9, ?)", map(allocate(THRESHOLD_IN_BYTES), allocate(1)));

        // frozen collections size is not checked during sstable write
        assertNotWarnedOnFlush();
    }

    @Test
    public void testMapSizeWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<text, text>)");
        disableCompaction();

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", map(allocate(1), allocate(1)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", map(allocate(1), allocate(1)));
        assertNotWarnedOnFlush();

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", map(allocate(1), allocate(THRESHOLD_IN_BYTES / 2)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 1", map(allocate(2), allocate(THRESHOLD_IN_BYTES / 2)));
        assertWarnedOnFlush();

        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(allocate(THRESHOLD_IN_BYTES / 2), allocate(1)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 2", map(allocate(THRESHOLD_IN_BYTES / 2 + 1), allocate(1)));
        assertWarnedOnFlush();

        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", map(allocate(1), allocate(THRESHOLD_IN_BYTES / 2)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 3", map(allocate(THRESHOLD_IN_BYTES / 2), allocate(1)));
        assertWarnedOnFlush();

        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", map(allocate(THRESHOLD_IN_BYTES / 2), allocate(1)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 4", map(allocate(1), allocate(THRESHOLD_IN_BYTES / 2)));
        assertWarnedOnFlush();
    }

    @Test
    public void testMapSizeAfterCompaction() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<text, text>)");
        disableCompaction();

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", map(allocate(1), allocate(1)));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", map(allocate(1), allocate(1)));
        assertNotWarnedOnFlush();
        assertNotWarnedOnCompact();

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", map(allocate(1), allocate(THRESHOLD_IN_BYTES / 2)));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 1", map(allocate(2), allocate(THRESHOLD_IN_BYTES / 2)));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact();

        truncate();

        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(allocate(THRESHOLD_IN_BYTES / 2), allocate(1)));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 2", map(allocate(THRESHOLD_IN_BYTES / 2 + 1), allocate(1)));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact();

        truncate();

        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", map(allocate(1), allocate(THRESHOLD_IN_BYTES / 2)));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 3", map(allocate(THRESHOLD_IN_BYTES / 2), allocate(1)));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact();

        truncate();

        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", map(allocate(THRESHOLD_IN_BYTES / 2), allocate(1)));
        assertNotWarnedOnFlush();
        assertValid("UPDATE %s SET v = v + ? WHERE k = 4", map(allocate(1), allocate(THRESHOLD_IN_BYTES / 2 + 1)));
        assertNotWarnedOnFlush();
        assertWarnedOnCompact();
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
        assertValid("INSERT INTO %s (k, s, fs, l, fl, m, fm) VALUES (0, ?, ?, ?, ?, ?, ?)",
                    set(allocate(THRESHOLD_IN_BYTES / 2)),
                    set(allocate(THRESHOLD_IN_BYTES / 2)),
                    list(allocate(THRESHOLD_IN_BYTES / 2)),
                    list(allocate(THRESHOLD_IN_BYTES / 2)),
                    map(allocate(THRESHOLD_IN_BYTES / 4),
                        allocate(THRESHOLD_IN_BYTES / 4)),
                    map(allocate(THRESHOLD_IN_BYTES / 4),
                        allocate(THRESHOLD_IN_BYTES / 4)));
        assertNotWarnedOnFlush();

        // the guardrail will produce a log message for each column exceeding the threshold, not just for the first one
        assertWarns(Arrays.asList("Detected collection s of size",
                                  "Detected collection fs of size",
                                  "Detected collection l of size",
                                  "Detected collection fl of size",
                                  "Detected collection m of size",
                                  "Detected collection fm of size"),
                    "INSERT INTO %s (k, s, fs, l, fl, m, fm) VALUES (0, ?, ?, ?, ?, ?, ?)",
                    set(allocate(THRESHOLD_IN_BYTES)),
                    set(allocate(THRESHOLD_IN_BYTES)),
                    list(allocate(THRESHOLD_IN_BYTES)),
                    list(allocate(THRESHOLD_IN_BYTES)),
                    map(allocate(THRESHOLD_IN_BYTES),
                        allocate(THRESHOLD_IN_BYTES)),
                    map(allocate(THRESHOLD_IN_BYTES),
                        allocate(THRESHOLD_IN_BYTES)));

        // only the non frozen collections will produce a warning during sstable write
        assertWarnedOnSSTableWrite(false,
                                   SSTABLE_WRITE_WARN_MESSAGE,
                                   SSTABLE_WRITE_WARN_MESSAGE,
                                   SSTABLE_WRITE_WARN_MESSAGE);
    }

    @Test
    public void testCompositePartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 text, v set<text>, PRIMARY KEY((k1, k2)))");

        assertValid("INSERT INTO %s (k1, k2, v) VALUES (0, 'a', ?)", set(allocate(1)));
        assertNotWarnedOnFlush();

        assertWarns("INSERT INTO %s (k1, k2, v) VALUES (1, 'b', ?)", set(allocate(THRESHOLD_IN_BYTES)));
        assertWarnedOnFlush();
    }

    @Test
    public void testCompositeClusteringKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 int, c2 text, v set<text>, PRIMARY KEY(k, c1, c2))");

        assertValid("INSERT INTO %s (k, c1, c2, v) VALUES (1, 10, 'a', ?)", set(allocate(1)));
        assertNotWarnedOnFlush();

        assertWarns("INSERT INTO %s (k, c1, c2, v) VALUES (2, 20, 'b', ?)", set(allocate(THRESHOLD_IN_BYTES)));
        assertWarnedOnFlush();

        assertWarns("INSERT INTO %s (k, c1, c2, v) VALUES (3, 30, 'c', ?)", set(allocate(THRESHOLD_IN_BYTES)));
        assertWarnedOnFlush();
    }

    @Test
    public void testSuperUser() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<text>)");

        // regular user should be warned
        assertWarns("INSERT INTO %s (k, v) VALUES (1, ?)", set(allocate(THRESHOLD_IN_BYTES)));

        // super user shouldn't be warned
        useSuperUser();
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", set(allocate(THRESHOLD_IN_BYTES)));

        // sstable should produces warnings because the keyspace is not internal, regardless of the user
        assertWarnedOnSSTableWrite(false, SSTABLE_WRITE_WARN_MESSAGE, SSTABLE_WRITE_WARN_MESSAGE);
    }

    private void truncate() throws Throwable
    {
        execute("TRUNCATE %s");
    }

    private void assertWarns(String query, Object... args) throws Throwable
    {
        String warning = "Detected collection v of size";
        assertWarns(warning, query, args);
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
