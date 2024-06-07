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

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.MapType;

import static java.nio.ByteBuffer.allocate;

/**
 * Tests the guardrail for the size of map collections, {@link Guardrails#collectionSize}.
 * <p>
 * This test doesn't include the activation of the guardrail during sstable writes, these cases are covered by the dtest
 * {@link org.apache.cassandra.distributed.test.guardrails.GuardrailCollectionSizeOnSSTableWriteTest}.
 */
public class GuardrailCollectionMapSizeTest extends GuardrailCollectionTypeSpecificSizeTester
{
    public GuardrailCollectionMapSizeTest()
    {
        super(Guardrails.collectionMapSize,
              Guardrails::setCollectionMapSizeThreshold,
              Guardrails::getCollectionMapSizeWarnThreshold,
              Guardrails::getCollectionMapSizeFailThreshold);
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

    private ByteBuffer map()
    {
        return MapType.getInstance(BytesType.instance, BytesType.instance, true).decompose(Collections.emptyMap());
    }

    private ByteBuffer map(ByteBuffer key, ByteBuffer value)
    {
        return MapType.getInstance(BytesType.instance, BytesType.instance, true).decompose(ImmutableMap.of(key, value));
    }
}
