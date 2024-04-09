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

import static java.nio.ByteBuffer.allocate;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;
import static org.junit.Assert.assertEquals;

/**
 * Tests the guardrails around the size of SAI string terms
 *
 * @see Guardrails#saiStringTermSize
 */
public class GuardrailSaiStringTermSizeTest extends ValueThresholdTester
{
    private static final int WARN_THRESHOLD = 1024; // bytes
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD * 4; // bytes

    public GuardrailSaiStringTermSizeTest()
    {
        super(WARN_THRESHOLD + "B",
              FAIL_THRESHOLD + "B",
              Guardrails.saiStringTermSize,
              Guardrails::setSaiStringTermSizeThreshold,
              Guardrails::getSaiStringTermSizeWarnThreshold,
              Guardrails::getSaiStringTermSizeFailThreshold,
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
    public void testCompositePartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 int, k2 text, v int, PRIMARY KEY((k1, k2)))");
        createIndex("CREATE INDEX ON %s (k2) USING 'sai'");

        testThreshold("k2", "INSERT INTO %s (k1, k2, v) VALUES (0, ?, 0)");
        testThreshold("k2", "UPDATE %s SET v = 1 WHERE k1 = 0 AND k2 = ?");
    }

    @Test
    public void testSimpleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c text, v int, PRIMARY KEY(k, c))");
        createIndex("CREATE INDEX ON %s (c) USING 'sai'");

        testThreshold("c", "INSERT INTO %s (k, c, v) VALUES (0, ?, 0)");
        testThreshold("c", "UPDATE %s SET v = 1 WHERE k = 0 AND c = ?");
    }

    @Test
    public void testRegularColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v text)");
        createIndex("CREATE INDEX ON %s (v) USING 'sai'");

        testThreshold("v", "INSERT INTO %s (k, v) VALUES (0, ?)");
        testThreshold("v", "UPDATE %s SET v = ? WHERE k = 0");
    }

    @Test
    public void testStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s text STATIC, r int, PRIMARY KEY(k, c))");
        createIndex("CREATE INDEX ON %s (s) USING 'sai'");

        testThreshold("s", "INSERT INTO %s (k, s) VALUES (0, ?)");
        testThreshold("s", "INSERT INTO %s (k, c, s, r) VALUES (0, 0, ?, 0)");
        testThreshold("s", "UPDATE %s SET s = ? WHERE k = 0");
        testThreshold("s", "UPDATE %s SET s = ?, r = 0 WHERE k = 0 AND c = 0");
    }

    @Test
    public void testList() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, l list<text>)");
        createIndex("CREATE INDEX ON %s (l) USING 'sai'");

        for (String query : Arrays.asList("INSERT INTO %s (k, l) VALUES (0, ?)",
                                          "UPDATE %s SET l = ? WHERE k = 0",
                                          "UPDATE %s SET l = l + ? WHERE k = 0"))
        {
            testCollection("l", query, this::list);
        }

        testThreshold("l", "UPDATE %s SET l[0] = ? WHERE k = 0");

        String query = "UPDATE %s SET l = l - ? WHERE k = 0";
        assertValid(query, this::list, allocate(1));
        assertValid(query, this::list, allocate(FAIL_THRESHOLD));
        assertValid(query, this::list, allocate(FAIL_THRESHOLD + 1)); // Doesn't write anything because we couldn't write
    }

    @Test
    public void testBatch() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, r text, s text STATIC, PRIMARY KEY(k, c))");
        createIndex("CREATE INDEX ON %s (s) USING 'sai'");
        createIndex("CREATE INDEX ON %s (r) USING 'sai'");

        // static column
        testThreshold("s", "BEGIN BATCH INSERT INTO %s (k, s) VALUES ('0', ?); APPLY BATCH;");
        testThreshold("s", "BEGIN BATCH INSERT INTO %s (k, s, c, r) VALUES ('0', ?, '0', '0'); APPLY BATCH;");
        testThreshold("s", "BEGIN BATCH UPDATE %s SET s = ? WHERE k = '0'; APPLY BATCH;");
        testThreshold("s", "BEGIN BATCH UPDATE %s SET s = ?, r = '0' WHERE k = '0' AND c = '0'; APPLY BATCH;");

        // regular column
        testThreshold("r", "BEGIN BATCH INSERT INTO %s (k, c, r) VALUES ('0', '0', ?); APPLY BATCH;");
        testThreshold("r", "BEGIN BATCH UPDATE %s SET r = ? WHERE k = '0' AND c = '0'; APPLY BATCH;");
    }

    @Test
    public void testCASWithIfNotExistsCondition() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, v text, s text STATIC, PRIMARY KEY(k, c))");
        createIndex("CREATE INDEX ON %s (s) USING 'sai'");
        createIndex("CREATE INDEX ON %s (v) USING 'sai'");

        // static column
        assertValid("INSERT INTO %s (k, s) VALUES ('1', ?) IF NOT EXISTS", allocate(1));
        assertValid("INSERT INTO %s (k, s) VALUES ('2', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD));
        assertValid("INSERT INTO %s (k, s) VALUES ('2', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD + 1)); // not applied
        assertWarns("s", "INSERT INTO %s (k, s) VALUES ('3', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD + 1));

        // regular column
        assertValid("INSERT INTO %s (k, c, v) VALUES ('4', '0', ?) IF NOT EXISTS", allocate(1));
        assertValid("INSERT INTO %s (k, c, v) VALUES ('5', '0', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD));
        assertValid("INSERT INTO %s (k, c, v) VALUES ('5', '0', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD + 1)); // not applied
        assertWarns("v", "INSERT INTO %s (k, c, v) VALUES ('6', '0', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD + 1));
    }

    @Test
    public void testSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, r text, s text STATIC, PRIMARY KEY(k, c))");
        createIndex("CREATE INDEX ON %s (c) USING 'sai'");
        createIndex("CREATE INDEX ON %s (r) USING 'sai'");
        createIndex("CREATE INDEX ON %s (s) USING 'sai'");

        // the guardail is only checked for writes; reads are excluded
        testNoThreshold("SELECT * FROM %s WHERE k = ?");
        testNoThreshold("SELECT * FROM %s WHERE k = '0' AND c = ?");
        testNoThreshold("SELECT * FROM %s WHERE c = ? ALLOW FILTERING");
        testNoThreshold("SELECT * FROM %s WHERE s = ? ALLOW FILTERING");
        testNoThreshold("SELECT * FROM %s WHERE r = ? ALLOW FILTERING");
    }

    @Test
    public void testWarningTermOnBuild()
    {
        ByteBuffer largeTerm = allocate(warnThreshold() + 1);
        ByteBuffer smallTerm = allocate(1);

        createTable(KEYSPACE, "CREATE TABLE %s (k int PRIMARY KEY, v text)");
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

        createTable(KEYSPACE, "CREATE TABLE %s (k int PRIMARY KEY, v text)");
        execute("INSERT INTO %s (k, v) VALUES (0, ?)", oversizedTerm);
        execute("INSERT INTO %s (k, v) VALUES (1, ?)", smallTerm);
        createIndex("CREATE INDEX ON %s(v) USING 'sai'");

        // verify that the oversized term isn't written on initial index build
        assertEquals(((ResultMessage.Rows) execute("SELECT * FROM %s WHERE v = ?", oversizedTerm)).result.size(), 0);
        assertEquals(((ResultMessage.Rows) execute("SELECT * FROM %s WHERE v = ?", smallTerm)).result.size(), 1);
    }
}
