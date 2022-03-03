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

package org.apache.cassandra.db.guardrails;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;

/**
 * Tests the guardrail for the number of items on a collection, {@link Guardrails#itemsPerCollection}.
 * <p>
 * This test doesn't include the activation of the guardrail during sstable writes, these cases are covered by the dtest
 * {@link org.apache.cassandra.distributed.test.guardrails.GuardrailItemsPerCollectionOnSSTableWriteTest}.
 */
public class GuardrailItemsPerCollectionTest extends ThresholdTester
{
    private static final int WARN_THRESHOLD = 10;
    private static final int FAIL_THRESHOLD = 20;

    public GuardrailItemsPerCollectionTest()
    {
        super(WARN_THRESHOLD,
              FAIL_THRESHOLD,
              Guardrails.itemsPerCollection,
              Guardrails::setItemsPerCollectionThreshold,
              Guardrails::getItemsPerCollectionWarnThreshold,
              Guardrails::getItemsPerCollectionFailThreshold);
    }

    @After
    public void after()
    {
        // immediately drop the created table so its async cleanup doesn't interfere with the next tests
        if (currentTable() != null)
            dropTable("DROP TABLE %s");
    }

    @Test
    public void testSetSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<int>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set(0));
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", set(1));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", set(WARN_THRESHOLD));
        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", set(WARN_THRESHOLD + 1), WARN_THRESHOLD + 1);
        assertWarns("INSERT INTO %s (k, v) VALUES (6, ?)", set(FAIL_THRESHOLD), FAIL_THRESHOLD);
        assertFails("INSERT INTO %s (k, v) VALUES (7, ?)", set(FAIL_THRESHOLD + 1), FAIL_THRESHOLD + 1);
        assertFails("INSERT INTO %s (k, v) VALUES (8, ?)", set(FAIL_THRESHOLD + 10), FAIL_THRESHOLD + 10);
    }

    @Test
    public void testSetSizeFrozen() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<set<int>>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set(0));
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", set(1));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", set(WARN_THRESHOLD));
        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", set(WARN_THRESHOLD + 1), WARN_THRESHOLD + 1);
        assertWarns("INSERT INTO %s (k, v) VALUES (6, ?)", set(FAIL_THRESHOLD), FAIL_THRESHOLD);
        assertFails("INSERT INTO %s (k, v) VALUES (7, ?)", set(FAIL_THRESHOLD + 1), FAIL_THRESHOLD + 1);
    }

    @Test
    public void testSetSizeWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<int>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", set(1));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", set(1, WARN_THRESHOLD));
        assertWarns("UPDATE %s SET v = v + ? WHERE k = 0", set(1, FAIL_THRESHOLD), FAIL_THRESHOLD - 1);

        assertWarns("INSERT INTO %s (k, v) VALUES (1, ?)", set(WARN_THRESHOLD + 1), WARN_THRESHOLD + 1);
        assertValid("UPDATE %s SET v = v - ? WHERE k = 1", set(1));

        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", set(1));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 2", set(1, WARN_THRESHOLD + 1));

        assertFails("INSERT INTO %s (k, v) VALUES (3, ?)", set(FAIL_THRESHOLD + 1), FAIL_THRESHOLD + 1);
        assertValid("UPDATE %s SET v = v - ? WHERE k = 3", set(1));

        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", set(1));
        assertWarns("UPDATE %s SET v = v + ? WHERE k = 4", set(1, FAIL_THRESHOLD + 1), FAIL_THRESHOLD);
    }

    @Test
    public void testListSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<int>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list(0));
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", list(1));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", list(WARN_THRESHOLD));
        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", list(WARN_THRESHOLD + 1), WARN_THRESHOLD + 1);
        assertWarns("INSERT INTO %s (k, v) VALUES (6, ?)", list(FAIL_THRESHOLD), FAIL_THRESHOLD);
        assertFails("INSERT INTO %s (k, v) VALUES (7, ?)", list(FAIL_THRESHOLD + 1), FAIL_THRESHOLD + 1);
        assertFails("INSERT INTO %s (k, v) VALUES (7, ?)", list(FAIL_THRESHOLD + 10), FAIL_THRESHOLD + 10);
    }

    @Test
    public void testListSizeFrozen() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<list<int>>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list(0));
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", list(1));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", list(WARN_THRESHOLD));
        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", list(WARN_THRESHOLD + 1), WARN_THRESHOLD + 1);
        assertWarns("INSERT INTO %s (k, v) VALUES (6, ?)", list(FAIL_THRESHOLD), FAIL_THRESHOLD);
        assertFails("INSERT INTO %s (k, v) VALUES (7, ?)", list(FAIL_THRESHOLD + 1), FAIL_THRESHOLD + 1);
    }

    @Test
    public void testListSizeWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<int>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", list(1));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", list(1, WARN_THRESHOLD));
        assertWarns("UPDATE %s SET v = v + ? WHERE k = 0", list(1, FAIL_THRESHOLD), FAIL_THRESHOLD - 1);

        assertWarns("INSERT INTO %s (k, v) VALUES (1, ?)", list(WARN_THRESHOLD + 1), WARN_THRESHOLD + 1);
        assertValid("UPDATE %s SET v = v - ? WHERE k = 1", list(1));

        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", list(1));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 2", list(1, WARN_THRESHOLD + 1));

        assertFails("INSERT INTO %s (k, v) VALUES (3, ?)", list(FAIL_THRESHOLD + 1), FAIL_THRESHOLD + 1);
        assertValid("UPDATE %s SET v = v - ? WHERE k = 3", list(1));

        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", list(1));
        assertWarns("UPDATE %s SET v = v + ? WHERE k = 4", list(1, FAIL_THRESHOLD + 1), FAIL_THRESHOLD);

        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", set(WARN_THRESHOLD + 1), WARN_THRESHOLD + 1);
        assertValid("UPDATE %s SET v[0] = null WHERE k = 5");
    }

    @Test
    public void testMapSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<int, int>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", map(0));
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(1));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", map(WARN_THRESHOLD));
        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", map(WARN_THRESHOLD + 1), WARN_THRESHOLD + 1);
        assertWarns("INSERT INTO %s (k, v) VALUES (6, ?)", map(FAIL_THRESHOLD), FAIL_THRESHOLD);
        assertFails("INSERT INTO %s (k, v) VALUES (7, ?)", map(FAIL_THRESHOLD + 1), FAIL_THRESHOLD + 1);
        assertFails("INSERT INTO %s (k, v) VALUES (7, ?)", map(FAIL_THRESHOLD + 10), FAIL_THRESHOLD + 10);
    }

    @Test
    public void testMapSizeFrozen() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<map<int, int>>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", map(0));
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(1));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", map(WARN_THRESHOLD));
        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", map(WARN_THRESHOLD + 1), WARN_THRESHOLD + 1);
        assertWarns("INSERT INTO %s (k, v) VALUES (6, ?)", map(FAIL_THRESHOLD), FAIL_THRESHOLD);
        assertFails("INSERT INTO %s (k, v) VALUES (7, ?)", map(FAIL_THRESHOLD + 1), FAIL_THRESHOLD + 1);
    }

    @Test
    public void testMapSizeWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<int, int>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", map(1));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", map(1, WARN_THRESHOLD));
        assertWarns("UPDATE %s SET v = v + ? WHERE k = 0", map(1, FAIL_THRESHOLD), FAIL_THRESHOLD - 1);

        assertWarns("INSERT INTO %s (k, v) VALUES (1, ?)", map(WARN_THRESHOLD + 1), WARN_THRESHOLD + 1);
        assertValid("UPDATE %s SET v = v - ? WHERE k = 1", set(1));

        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(1));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 2", map(1, WARN_THRESHOLD + 1));

        assertFails("INSERT INTO %s (k, v) VALUES (3, ?)", map(FAIL_THRESHOLD + 1), FAIL_THRESHOLD + 1);
        assertValid("UPDATE %s SET v = v - ? WHERE k = 3", set(1));

        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", map(1));
        assertWarns("UPDATE %s SET v = v + ? WHERE k = 4", map(1, FAIL_THRESHOLD + 1), FAIL_THRESHOLD);
    }

    @Override
    protected String createTable(String query)
    {
        String table = super.createTable(query);
        disableCompaction();
        return table;
    }

    private void assertValid(String query, ByteBuffer collection) throws Throwable
    {
        assertValid(execute(query, collection));
    }

    private void assertWarns(String query, ByteBuffer collection, int numItems) throws Throwable
    {
        assertWarns(execute(query, collection),
                    String.format("Detected collection v with %d items, this exceeds the warning threshold of %d.",
                                  numItems, WARN_THRESHOLD));
    }

    private void assertFails(String query, ByteBuffer collection, int numItems) throws Throwable
    {
        assertFails(execute(query, collection),
                    String.format("Detected collection v with %d items, this exceeds the failure threshold of %d.",
                                  numItems, FAIL_THRESHOLD));
    }

    private CheckedFunction execute(String query, ByteBuffer collection)
    {
        return () -> execute(userClientState, query, Collections.singletonList(collection));
    }

    private static ByteBuffer set(int numElements)
    {
        return set(0, numElements);
    }

    private static ByteBuffer set(int startInclusive, int endExclusive)
    {
        return SetType.getInstance(Int32Type.instance, true)
                      .decompose(collection(startInclusive, endExclusive, Collectors.toSet()));
    }

    private static ByteBuffer list(int numElements)
    {
        return list(0, numElements);
    }

    private static ByteBuffer list(int startInclusive, int endExclusive)
    {
        return ListType.getInstance(Int32Type.instance, false)
                       .decompose(collection(startInclusive, endExclusive, Collectors.toList()));
    }

    private static ByteBuffer map(int numElements)
    {
        return map(0, numElements);
    }

    private static ByteBuffer map(int startInclusive, int endExclusive)
    {
        return MapType.getInstance(Int32Type.instance, Int32Type.instance, true)
                      .decompose(collection(startInclusive, endExclusive, Collectors.toMap(x -> x, x -> x)));
    }

    private static <R, A> R collection(int startInclusive, int endExclusive, Collector<Integer, A, R> collector)
    {
        return IntStream.range(startInclusive, endExclusive).boxed().collect(collector);
    }
}
