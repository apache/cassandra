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

import org.junit.Test;

import org.apache.cassandra.config.DataStorageSpec;

import static java.nio.ByteBuffer.allocate;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.BYTES;

/**
 * Tests the guardrail for the size of blob column values, {@link Guardrails#columnBlobValueSize}.
 */
public class GuardrailColumnBlobValueSizeTest extends ColumnTypeSpecificValueThresholdTester
{
    private static final int WARN_THRESHOLD = 1024; // bytes
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD * 4; // bytes

    public GuardrailColumnBlobValueSizeTest()
    {
        super(WARN_THRESHOLD + "B",
              FAIL_THRESHOLD + "B",
              Guardrails.columnBlobValueSize,
              Guardrails::setColumnBlobValueSizeThreshold,
              Guardrails::getColumnBlobValueSizeWarnThreshold,
              Guardrails::getColumnBlobValueSizeFailThreshold,
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

    @Override
    protected String columnType()
    {
        return "blob";
    }

    @Override
    @Test
    public void testBatch() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c " + columnType() + ", r " + columnType() + ", s " + columnType() + " STATIC, PRIMARY KEY(k, c))");

        // partition key
        testNoThreshold("BEGIN BATCH INSERT INTO %s (k, c, r) VALUES (?, textAsBlob('0'), textAsBlob('0')); APPLY BATCH;");
        testNoThreshold("BEGIN BATCH UPDATE %s SET r = textAsBlob('0') WHERE k = ? AND c = textAsBlob('0'); APPLY BATCH;");

        // static column
        testThreshold("s", "BEGIN BATCH INSERT INTO %s (k, s) VALUES ('0', ?); APPLY BATCH;");
        testThreshold("s", "BEGIN BATCH INSERT INTO %s (k, s, c, r) VALUES ('0', ?, textAsBlob('0'), textAsBlob('0')); APPLY BATCH;");
        testThreshold("s", "BEGIN BATCH UPDATE %s SET s = ? WHERE k = '0'; APPLY BATCH;");
        testThreshold("s", "BEGIN BATCH UPDATE %s SET s = ?, r = textAsBlob('0') WHERE k = '0' AND c = textAsBlob('0'); APPLY BATCH;");

        // clustering key
        testNoThreshold("BEGIN BATCH INSERT INTO %s (k, c, r) VALUES ('0', ?, textAsBlob('0')); APPLY BATCH;");
        testNoThreshold("BEGIN BATCH UPDATE %s SET r = textAsBlob('0') WHERE k = '0' AND c = ?; APPLY BATCH;");
        testNoThreshold("BEGIN BATCH DELETE r FROM %s WHERE k = '0' AND c = ?; APPLY BATCH;");
        testNoThreshold("BEGIN BATCH DELETE FROM %s WHERE k = '0' AND c = ?; APPLY BATCH;");

        // regular column
        testThreshold("r", "BEGIN BATCH INSERT INTO %s (k, c, r) VALUES ('0', textAsBlob('0'), ?); APPLY BATCH;");
        testThreshold("r", "BEGIN BATCH UPDATE %s SET r = ? WHERE k = '0' AND c = textAsBlob('0'); APPLY BATCH;");
    }

    @Override
    @Test
    public void testCASWithIfNotExistsCondition() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c " + columnType() + ", v " + columnType() + ", s " + columnType() + " STATIC, PRIMARY KEY(k, c))");

        // partition key
        testNoThreshold("INSERT INTO %s (k, c, v) VALUES (?, textAsBlob('0'), textAsBlob('0')) IF NOT EXISTS");

        // clustering key
        testNoThreshold("INSERT INTO %s (k, c, v) VALUES ('0', ?, textAsBlob('0')) IF NOT EXISTS");

        // static column
        assertValid("INSERT INTO %s (k, s) VALUES ('1', ?) IF NOT EXISTS", allocate(1));
        assertValid("INSERT INTO %s (k, s) VALUES ('2', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD));
        assertValid("INSERT INTO %s (k, s) VALUES ('2', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD + 1)); // not applied
        assertWarns("s", "INSERT INTO %s (k, s) VALUES ('3', ?) IF NOT EXISTS", allocate(WARN_THRESHOLD + 1));

        // regular column
        assertValid("INSERT INTO %s (k, c, v) VALUES ('4', textAsBlob('0'), ?) IF NOT EXISTS", allocate(1));
        assertValid("INSERT INTO %s (k, c, v) VALUES ('5', textAsBlob('0'), ?) IF NOT EXISTS", allocate(WARN_THRESHOLD));
        assertValid("INSERT INTO %s (k, c, v) VALUES ('5', textAsBlob('0'), ?) IF NOT EXISTS", allocate(WARN_THRESHOLD + 1)); // not applied
        assertWarns("v", "INSERT INTO %s (k, c, v) VALUES ('6', textAsBlob('0'), ?) IF NOT EXISTS", allocate(WARN_THRESHOLD + 1));
    }

    @Override
    @Test
    public void testCASWithIfExistsCondition() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c " + columnType() + ", v " + columnType() + ", s " + columnType() + " STATIC, PRIMARY KEY(k, c))");

        // partition key, the CAS updates with values beyond the threshold are not applied, so they don't come to fail
        testNoThreshold("UPDATE %s SET v = textAsBlob('0') WHERE k = ? AND c = textAsBlob('0') IF EXISTS");

        // clustering key, the CAS updates with values beyond the threshold are not applied, so they don't come to fail
        testNoThreshold("UPDATE %s SET v = textAsBlob('0') WHERE k = '0' AND c = ? IF EXISTS");

        // static column, only the applied CAS updates can fire the guardrail
        assertValid("INSERT INTO %s (k, s) VALUES ('0', textAsBlob('0'))");
        testThreshold("s", "UPDATE %s SET s = ? WHERE k = '0' IF EXISTS");
        assertValid("DELETE FROM %s WHERE k = '0'");
        testNoThreshold("UPDATE %s SET s = ? WHERE k = '0' IF EXISTS");

        // regular column, only the applied CAS updates can fire the guardrail
        assertValid("INSERT INTO %s (k, c) VALUES ('0', textAsBlob('0'))");
        testThreshold("v", "UPDATE %s SET v = ? WHERE k = '0' AND c = textAsBlob('0') IF EXISTS");
        assertValid("DELETE FROM %s WHERE k = '0' AND c = textAsBlob('0')");
        testNoThreshold("UPDATE %s SET v = ? WHERE k = '0' AND c = textAsBlob('0') IF EXISTS");
    }

    @Test
    public void testCASWithColumnsCondition() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + columnType() + ")");

        // updates are always accepted for values lesser than the threshold, independently of whether they are applied
        assertValid("DELETE FROM %s WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = textAsBlob('0')", allocate(1));
        assertValid("UPDATE %s SET v = textAsBlob('0') WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = textAsBlob('0')", allocate(1));

        // updates are always accepted for values equals to the threshold, independently of whether they are applied
        assertValid("DELETE FROM %s WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = textAsBlob('0')", allocate(WARN_THRESHOLD));
        assertValid("UPDATE %s SET v = textAsBlob('0') WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = textAsBlob('0')", allocate(WARN_THRESHOLD));

        // updates beyond the threshold fail only if the update is applied
        assertValid("DELETE FROM %s WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = textAsBlob('0')", allocate(WARN_THRESHOLD + 1));
        assertValid("UPDATE %s SET v = textAsBlob('0') WHERE k = 0");
        assertWarns("v", "UPDATE %s SET v = ? WHERE k = 0 IF v = textAsBlob('0')", allocate(WARN_THRESHOLD + 1));
    }

    @Override
    @Test
    public void testSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (k " + columnType() + ", c " + columnType() + ", r " + columnType() + ", s " + columnType() + " STATIC, PRIMARY KEY(k, c))");

        // the guardail is only checked for writes; reads are excluded

        testNoThreshold("SELECT * FROM %s WHERE k = ?");
        testNoThreshold("SELECT * FROM %s WHERE c = ? ALLOW FILTERING");
        testNoThreshold("SELECT * FROM %s WHERE s = ? ALLOW FILTERING");
        testNoThreshold("SELECT * FROM %s WHERE r = ? ALLOW FILTERING");
    }
}
