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

import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.SetType;

import static java.nio.ByteBuffer.allocate;

/**
 * Tests the guardrail for the size of set collections, {@link Guardrails#collectionSize}.
 * <p>
 * This test doesn't include the activation of the guardrail during sstable writes, these cases are covered by the dtest
 * {@link org.apache.cassandra.distributed.test.guardrails.GuardrailCollectionSizeOnSSTableWriteTest}.
 */
public class GuardrailCollectionSetSizeTest extends GuardrailCollectionTypeSpecificSizeTester
{
    public GuardrailCollectionSetSizeTest()
    {
        super(Guardrails.collectionSetSize,
              Guardrails::setCollectionSetSizeThreshold,
              Guardrails::getCollectionSetSizeWarnThreshold,
              Guardrails::getCollectionSetSizeFailThreshold);
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

    private static ByteBuffer set(ByteBuffer... values)
    {
        return SetType.getInstance(BytesType.instance, true).decompose(ImmutableSet.copyOf(values));
    }
}
