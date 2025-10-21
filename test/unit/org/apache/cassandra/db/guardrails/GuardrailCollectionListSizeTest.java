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

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.ListType;

import static java.nio.ByteBuffer.allocate;

/**
 * Tests the guardrail for the size of list collections, {@link Guardrails#collectionSize}.
 * <p>
 * This test doesn't include the activation of the guardrail during sstable writes, these cases are covered by the dtest
 * {@link org.apache.cassandra.distributed.test.guardrails.GuardrailCollectionSizeOnSSTableWriteTest}.
 */
public class GuardrailCollectionListSizeTest extends GuardrailCollectionTypeSpecificSizeTester
{
    public GuardrailCollectionListSizeTest()
    {
        super(Guardrails.collectionListSize,
              Guardrails::setCollectionListSizeThreshold,
              Guardrails::getCollectionListSizeWarnThreshold,
              Guardrails::getCollectionListSizeFailThreshold);
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

    private static ByteBuffer list(ByteBuffer... values)
    {
        return ListType.getInstance(BytesType.instance, true).decompose(ImmutableList.copyOf(values));
    }
}
