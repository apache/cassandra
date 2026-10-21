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

import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.transport.messages.ResultMessage;

import static java.nio.ByteBuffer.allocate;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;
import static org.junit.Assert.assertEquals;

/**
 * Tests the guardrails around the size of SAI blob terms
 *
 * @see Guardrails#saiBlobTermSize
 */
public class GuardrailSaiBlobTermSizeTest extends ValueThresholdTester
{
    private static final int WARN_THRESHOLD = 1024; // bytes
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD * 4; // bytes

    public GuardrailSaiBlobTermSizeTest()
    {
        super(WARN_THRESHOLD + "B",
              FAIL_THRESHOLD + "B",
              Guardrails.saiBlobTermSize,
              Guardrails::setSaiBlobTermSizeThreshold,
              Guardrails::getSaiBlobTermSizeWarnThreshold,
              Guardrails::getSaiBlobTermSizeFailThreshold,
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
    public void testRegularColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v blob)");
        createIndex("CREATE INDEX ON %s (v) USING 'sai'");

        testThreshold("v", "INSERT INTO %s (k, v) VALUES (0, ?)");
        testThreshold("v", "UPDATE %s SET v = ? WHERE k = 0");
    }

    @Test
    public void testStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s blob STATIC, r int, PRIMARY KEY(k, c))");
        createIndex("CREATE INDEX ON %s (s) USING 'sai'");

        testThreshold("s", "INSERT INTO %s (k, s) VALUES (0, ?)");
        testThreshold("s", "INSERT INTO %s (k, c, s, r) VALUES (0, 0, ?, 0)");
        testThreshold("s", "UPDATE %s SET s = ? WHERE k = 0");
        testThreshold("s", "UPDATE %s SET s = ?, r = 0 WHERE k = 0 AND c = 0");
    }

    @Test
    public void testWarningTermOnBuild()
    {
        ByteBuffer largeTerm = allocate(warnThreshold() + 1);
        ByteBuffer smallTerm = allocate(1);

        createTable(KEYSPACE, "CREATE TABLE %s (k int PRIMARY KEY, v blob)");
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", largeTerm);
        execute("INSERT INTO %s (k, v) VALUES (1, ?)", smallTerm);
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        // verify that the large term is written on initial index build
        assertEquals(((ResultMessage.Rows) execute("SELECT * FROM %s WHERE v = ?", largeTerm)).result.size(), 1);
        assertEquals(((ResultMessage.Rows) execute("SELECT * FROM %s WHERE v = ?", smallTerm)).result.size(), 1);
    }

    @Test
    public void testFailingTermOnBuild()
    {
        ByteBuffer oversizedTerm = allocate(failThreshold() + 1);
        ByteBuffer smallTerm = allocate(1);

        createTable(KEYSPACE, "CREATE TABLE %s (k int PRIMARY KEY, v blob)");
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", oversizedTerm);
        execute("INSERT INTO %s (k, v) VALUES (1, ?)", smallTerm);
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        // verify that the oversized term isn't written on initial index build
        assertEquals(((ResultMessage.Rows) execute("SELECT * FROM %s WHERE v = ?", oversizedTerm)).result.size(), 0);
        assertEquals(((ResultMessage.Rows) execute("SELECT * FROM %s WHERE v = ?", smallTerm)).result.size(), 1);
    }
}
