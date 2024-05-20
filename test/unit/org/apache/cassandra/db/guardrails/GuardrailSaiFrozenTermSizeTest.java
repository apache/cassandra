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

import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.transport.messages.ResultMessage;

import static java.lang.String.format;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;
import static org.junit.Assert.assertEquals;

/**
 * Tests the guardrails around the size of SAI frozen terms
 *
 * @see Guardrails#saiFrozenTermSize
 */
public class GuardrailSaiFrozenTermSizeTest extends ValueThresholdTester
{
    private static final int WARN_THRESHOLD = 2048; // bytes
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD * 4; // bytes

    public GuardrailSaiFrozenTermSizeTest()
    {
        super(WARN_THRESHOLD + "B",
              FAIL_THRESHOLD + "B",
              Guardrails.saiFrozenTermSize,
              Guardrails::setSaiFrozenTermSizeThreshold,
              Guardrails::getSaiFrozenTermSizeWarnThreshold,
              Guardrails::getSaiFrozenTermSizeFailThreshold,
              bytes -> new DataStorageSpec.LongBytesBound(bytes, BYTES).toString(),
              size -> new DataStorageSpec.LongBytesBound(size).toBytes());
    }

    @Override
    protected int warnThreshold()
    {
        return WARN_THRESHOLD;
    }

    @Override
    protected int failThreshold()
    {
        return FAIL_THRESHOLD;
    }

    @Test
    public void testTuple() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t tuple<text, text>)");
        createIndex("CREATE INDEX ON %s (t) USING 'sai'");

        testThreshold2("t", "INSERT INTO %s (k, t) VALUES (0, (?, ?))", 8);
        testThreshold2("t", "UPDATE %s SET t = (?, ?) WHERE k = 0", 8);
    }

    @Test
    public void testFrozenUDT() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (a text, b text)");
        createTable(format("CREATE TABLE %%s (k int PRIMARY KEY, v frozen<%s>)", udt));
        createIndex("CREATE INDEX ON %s (v) USING 'sai'");

        testThreshold("v", "INSERT INTO %s (k, v) VALUES (0, {a: ?})", 8);
        testThreshold("v", "INSERT INTO %s (k, v) VALUES (0, {b: ?})", 8);
        testThreshold("v", "UPDATE %s SET v = {a: ?} WHERE k = 0", 8);
        testThreshold("v", "UPDATE %s SET v = {b: ?} WHERE k = 0", 8);
        testThreshold2("v", "INSERT INTO %s (k, v) VALUES (0, {a: ?, b: ?})", 8);
        testThreshold2("v", "UPDATE %s SET v = {a: ?, b: ?} WHERE k = 0", 8);
    }

    @Test
    public void testFrozenList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, fl frozen<list<text>>)");
        createIndex("CREATE INDEX ON %s (FULL(fl)) USING 'sai'");

        // the serialized size of a frozen list is the size of its serialized elements, plus a 32-bit integer prefix for
        // the number of elements, and another 32-bit integer for the size of each element

        for (String query : Arrays.asList("INSERT INTO %s (k, fl) VALUES (0, ?)",
                                          "UPDATE %s SET fl = ? WHERE k = 0"))
        {
            testFrozenCollection("fl", query, this::list);
        }
    }

    @Test
    public void testWarningTupleOnBuild()
    {
        ByteBuffer largeTuple = ByteBuffer.allocate(warnThreshold() + 1);
        ByteBuffer smallTuple = ByteBuffer.allocate(1);

        createTable(KEYSPACE, "CREATE TABLE %s (k int PRIMARY KEY, t tuple<text>)");
        execute("INSERT INTO %s (k, t) VALUES (0, (?))", largeTuple);
        execute("INSERT INTO %s (k, t) VALUES (1, (?))", smallTuple);
        createIndex("CREATE INDEX ON %s(t) USING 'sai'");

        // verify that the large tuple is written on initial index build
        assertEquals(((ResultMessage.Rows) execute("SELECT * FROM %s WHERE t = (?)", largeTuple)).result.size(), 1);
        assertEquals(((ResultMessage.Rows) execute("SELECT * FROM %s WHERE t = (?)", smallTuple)).result.size(), 1);
    }

    @Test
    public void testFailingTupleOnBuild()
    {
        ByteBuffer oversizedTuple = ByteBuffer.allocate(failThreshold() + 1);
        ByteBuffer smallTuple = ByteBuffer.allocate(1);

        createTable(KEYSPACE, "CREATE TABLE %s (k int PRIMARY KEY, t tuple<text>)");
        execute("INSERT INTO %s (k, t) VALUES (0, (?))", oversizedTuple);
        execute("INSERT INTO %s (k, t) VALUES (1, (?))", smallTuple);
        createIndex("CREATE INDEX ON %s(t) USING 'sai'");

        // verify that the oversized tuple isn't written on initial index build
        assertEquals(((ResultMessage.Rows) execute("SELECT * FROM %s WHERE t = (?)", oversizedTuple)).result.size(), 0);
        assertEquals(((ResultMessage.Rows) execute("SELECT * FROM %s WHERE t = (?)", smallTuple)).result.size(), 1);
    }
}
