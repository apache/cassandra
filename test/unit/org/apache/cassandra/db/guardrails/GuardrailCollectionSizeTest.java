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
import java.util.Arrays;
import java.util.Collections;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;

import static java.nio.ByteBuffer.allocate;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;

/**
 * Tests the guardrail for the size of collections, {@link Guardrails#collectionSize}.
 * <p>
 * This test doesn't include the activation of the guardrail during sstable writes, these cases are covered by the dtest
 * {@link org.apache.cassandra.distributed.test.guardrails.GuardrailCollectionSizeOnSSTableWriteTest}.
 */
public class GuardrailCollectionSizeTest extends ThresholdTester
{
    private static final int WARN_THRESHOLD = 1024; // bytes
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD * 4; // bytes

    public GuardrailCollectionSizeTest()
    {
        super(WARN_THRESHOLD + "B",
              FAIL_THRESHOLD + "B",
              Guardrails.collectionSize,
              Guardrails::setCollectionSizeThreshold,
              Guardrails::getCollectionSizeWarnThreshold,
              Guardrails::getCollectionSizeFailThreshold,
              bytes -> new DataStorageSpec.LongBytesBound(bytes, BYTES).toString(),
              size -> new DataStorageSpec.LongBytesBound(size).toBytes());
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
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<blob>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", set(allocate(1)));
        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", set(allocate(WARN_THRESHOLD / 2)));

        assertWarns("INSERT INTO %s (k, v) VALUES (4, ?)", set(allocate(WARN_THRESHOLD)));
        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", set(allocate(WARN_THRESHOLD / 4), allocate(WARN_THRESHOLD * 3 / 4)));

        assertFails("INSERT INTO %s (k, v) VALUES (6, ?)", set(allocate(FAIL_THRESHOLD)));
        assertFails("INSERT INTO %s (k, v) VALUES (7, ?)", set(allocate(FAIL_THRESHOLD / 4), allocate(FAIL_THRESHOLD * 3 / 4)));
    }

    @Test
    public void testSetSizeFrozen() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<set<blob>>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", set(allocate(1)));
        assertWarns("INSERT INTO %s (k, v) VALUES (4, ?)", set(allocate(WARN_THRESHOLD)));
        assertFails("INSERT INTO %s (k, v) VALUES (5, ?)", set(allocate(FAIL_THRESHOLD)));
    }

    @Test
    public void testSetSizeWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v set<blob>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", set(allocate(1)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", set(allocate(1)));

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", set(allocate(WARN_THRESHOLD / 4)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 1", set(allocate(WARN_THRESHOLD * 3 / 4)));

        assertWarns("INSERT INTO %s (k, v) VALUES (2, ?)", set(allocate(FAIL_THRESHOLD / 4)));
        assertWarns("UPDATE %s SET v = v + ? WHERE k = 2", set(allocate(FAIL_THRESHOLD * 3 / 4)));
    }

    @Test
    public void testListSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<blob>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", list(allocate(1)));
        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", list(allocate(WARN_THRESHOLD / 2)));

        assertWarns("INSERT INTO %s (k, v) VALUES (4, ?)", list(allocate(WARN_THRESHOLD)));
        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", list(allocate(WARN_THRESHOLD / 2), allocate(WARN_THRESHOLD / 2)));

        assertFails("INSERT INTO %s (k, v) VALUES (6, ?)", list(allocate(FAIL_THRESHOLD)));
        assertFails("INSERT INTO %s (k, v) VALUES (7, ?)", list(allocate(FAIL_THRESHOLD / 2), allocate(FAIL_THRESHOLD / 2)));
    }

    @Test
    public void testListSizeFrozen() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<list<blob>>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", list(allocate(1)));
        assertWarns("INSERT INTO %s (k, v) VALUES (4, ?)", list(allocate(WARN_THRESHOLD)));
        assertFails("INSERT INTO %s (k, v) VALUES (5, ?)", list(allocate(FAIL_THRESHOLD)));
    }

    @Test
    public void testListSizeWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v list<blob>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", list(allocate(1)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", list(allocate(1)));

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", list(allocate(WARN_THRESHOLD / 2)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 1", list(allocate(WARN_THRESHOLD / 2)));

        assertWarns("INSERT INTO %s (k, v) VALUES (2, ?)", list(allocate(FAIL_THRESHOLD / 2)));
        assertWarns("UPDATE %s SET v = v + ? WHERE k = 2", list(allocate(FAIL_THRESHOLD / 2)));
    }

    @Test
    public void testMapSize() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<blob, blob>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", map());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(allocate(1), allocate(1)));
        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", map(allocate(1), allocate(WARN_THRESHOLD / 2)));
        assertValid("INSERT INTO %s (k, v) VALUES (4, ?)", map(allocate(WARN_THRESHOLD / 2), allocate(1)));

        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", map(allocate(WARN_THRESHOLD), allocate(1)));
        assertWarns("INSERT INTO %s (k, v) VALUES (6, ?)", map(allocate(1), allocate(WARN_THRESHOLD)));
        assertWarns("INSERT INTO %s (k, v) VALUES (7, ?)", map(allocate(WARN_THRESHOLD), allocate(WARN_THRESHOLD)));

        assertFails("INSERT INTO %s (k, v) VALUES (8, ?)", map(allocate(FAIL_THRESHOLD), allocate(1)));
        assertFails("INSERT INTO %s (k, v) VALUES (9, ?)", map(allocate(1), allocate(FAIL_THRESHOLD)));
        assertFails("INSERT INTO %s (k, v) VALUES (10, ?)", map(allocate(FAIL_THRESHOLD), allocate(FAIL_THRESHOLD)));
    }

    @Test
    public void testMapSizeFrozen() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<map<blob, blob>>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, null)");
        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", map());
        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(allocate(1), allocate(1)));
        assertWarns("INSERT INTO %s (k, v) VALUES (4, ?)", map(allocate(1), allocate(WARN_THRESHOLD)));
        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", map(allocate(WARN_THRESHOLD), allocate(1)));
        assertWarns("INSERT INTO %s (k, v) VALUES (6, ?)", map(allocate(WARN_THRESHOLD), allocate(WARN_THRESHOLD)));
        assertFails("INSERT INTO %s (k, v) VALUES (7, ?)", map(allocate(1), allocate(FAIL_THRESHOLD)));
        assertFails("INSERT INTO %s (k, v) VALUES (8, ?)", map(allocate(FAIL_THRESHOLD), allocate(1)));
        assertFails("INSERT INTO %s (k, v) VALUES (9, ?)", map(allocate(FAIL_THRESHOLD), allocate(FAIL_THRESHOLD)));
    }

    @Test
    public void testMapSizeWithUpdates() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v map<blob, blob>)");

        assertValid("INSERT INTO %s (k, v) VALUES (0, ?)", map(allocate(1), allocate(1)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 0", map(allocate(1), allocate(1)));

        assertValid("INSERT INTO %s (k, v) VALUES (1, ?)", map(allocate(1), allocate(WARN_THRESHOLD / 2)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 1", map(allocate(2), allocate(WARN_THRESHOLD / 2)));

        assertValid("INSERT INTO %s (k, v) VALUES (2, ?)", map(allocate(WARN_THRESHOLD / 4), allocate(1)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 2", map(allocate(WARN_THRESHOLD * 3 / 4), allocate(1)));

        assertValid("INSERT INTO %s (k, v) VALUES (3, ?)", map(allocate(WARN_THRESHOLD / 4), allocate(WARN_THRESHOLD / 4)));
        assertValid("UPDATE %s SET v = v + ? WHERE k = 3", map(allocate(WARN_THRESHOLD / 4 + 1), allocate(WARN_THRESHOLD / 4)));

        assertWarns("INSERT INTO %s (k, v) VALUES (4, ?)", map(allocate(1), allocate(FAIL_THRESHOLD / 2)));
        assertWarns("UPDATE %s SET v = v + ? WHERE k = 4", map(allocate(2), allocate(FAIL_THRESHOLD / 2)));

        assertWarns("INSERT INTO %s (k, v) VALUES (5, ?)", map(allocate(FAIL_THRESHOLD / 4), allocate(1)));
        assertWarns("UPDATE %s SET v = v + ? WHERE k = 5", map(allocate(FAIL_THRESHOLD * 3 / 4), allocate(1)));

        assertWarns("INSERT INTO %s (k, v) VALUES (6, ?)", map(allocate(FAIL_THRESHOLD / 4), allocate(FAIL_THRESHOLD / 4)));
        assertWarns("UPDATE %s SET v = v + ? WHERE k = 6", map(allocate(FAIL_THRESHOLD / 4 + 1), allocate(FAIL_THRESHOLD / 4)));
    }

    @Override
    protected String createTable(String query)
    {
        String table = super.createTable(query);
        disableCompaction();
        return table;
    }

    private void assertValid(String query, ByteBuffer... values) throws Throwable
    {
        assertValid(execute(query, values));
    }

    private void assertWarns(String query, ByteBuffer... values) throws Throwable
    {
        assertWarns(execute(query, values), "Detected collection v");
    }

    private void assertFails(String query, ByteBuffer... values) throws Throwable
    {
        assertFails(execute(query, values), "Detected collection v");
    }

    private CheckedFunction execute(String query, ByteBuffer... values)
    {
        return () -> execute(userClientState, query, Arrays.asList(values));
    }

    private static ByteBuffer set(ByteBuffer... values)
    {
        return SetType.getInstance(BytesType.instance, true).decompose(ImmutableSet.copyOf(values));
    }

    private static ByteBuffer list(ByteBuffer... values)
    {
        return ListType.getInstance(BytesType.instance, true).decompose(ImmutableList.copyOf(values));
    }

    private ByteBuffer map()
    {
        return MapType.getInstance(BytesType.instance, BytesType.instance, true).decompose(Collections.emptyMap());
    }

    private ByteBuffer map(ByteBuffer key, ByteBuffer value)
    {
        return MapType.getInstance(BytesType.instance, BytesType.instance, true).decompose(ImmutableMap.of(key, value));
    }
}
