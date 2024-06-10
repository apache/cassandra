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

import java.util.function.Function;
import java.util.function.ToLongFunction;

import org.junit.Test;

import static java.nio.ByteBuffer.allocate;


public abstract class ColumnTypeSpecificValueThresholdTester extends ValueThresholdTester
{

    private static final int WARN_THRESHOLD = 1024; // bytes
    private static final int FAIL_THRESHOLD = WARN_THRESHOLD * 4; // bytes

    protected ColumnTypeSpecificValueThresholdTester(String warnThreshold,
                                                     String failThreshold,
                                                     Threshold threshold,
                                                     TriConsumer<Guardrails, String, String> setter,
                                                     Function<Guardrails, String> warnGetter,
                                                     Function<Guardrails, String> failGetter,
                                                     Function<Long, String> stringFormatter,
                                                     ToLongFunction<String> stringParser)
    {
        super(warnThreshold,
              failThreshold,
              threshold,
              setter,
              warnGetter,
              failGetter,
              stringFormatter,
              stringParser);
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

    protected abstract String columnType();

    @Test
    public void testSimplePartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k " + columnType() + " PRIMARY KEY, v int)");

        // the size of primary key columns is not guarded because they already have a fixed limit of 65535B

        testNoThreshold("INSERT INTO %s (k, v) VALUES (?, 0)");
        testNoThreshold("UPDATE %s SET v = 1 WHERE k = ?");
        testNoThreshold("DELETE v FROM %s WHERE k = ?");
        testNoThreshold("DELETE FROM %s WHERE k = ?");
    }

    @Test
    public void testCompositePartitionKey() throws Throwable
    {
        createTable("CREATE TABLE %s (k1 " + columnType() + ", k2 " + columnType() + ", v int, PRIMARY KEY((k1, k2)))");

        // the size of primary key columns is not guarded because they already have a fixed limit of 65535B

        testNoThreshold2("INSERT INTO %s (k1, k2, v) VALUES (?, ?, 0)");
        testNoThreshold2("UPDATE %s SET v = 1 WHERE k1 = ? AND k2 = ?");
        testNoThreshold2("DELETE v FROM %s WHERE k1 = ? AND k2 = ?");
        testNoThreshold2("DELETE FROM %s WHERE k1 = ? AND k2 = ?");
    }

    @Test
    public void testSimpleClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c " + columnType() + ", v int, PRIMARY KEY(k, c))");

        // the size of primary key columns is not guarded because they already have a fixed limit of 65535B

        testNoThreshold("INSERT INTO %s (k, c, v) VALUES (0, ?, 0)");
        testNoThreshold("UPDATE %s SET v = 1 WHERE k = 0 AND c = ?");
        testNoThreshold("DELETE v FROM %s WHERE k = 0 AND c = ?");
        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c = ?");
    }

    @Test
    public void testCompositeClustering() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c1 " + columnType() + ", c2 " + columnType() + ", v int, PRIMARY KEY(k, c1, c2))");

        // the size of primary key columns is not guarded because they already have a fixed limit of 65535B

        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c1 = ?");
        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c1 > ?");
        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c1 < ?");
        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c1 >= ?");
        testNoThreshold("DELETE FROM %s WHERE k = 0 AND c1 <= ?");

        testNoThreshold2("INSERT INTO %s (k, c1, c2, v) VALUES (0, ?, ?, 0)");
        testNoThreshold2("UPDATE %s SET v = 1 WHERE k = 0 AND c1 = ? AND c2 = ?");
        testNoThreshold2("DELETE v FROM %s WHERE k = 0 AND c1 = ? AND c2 = ?");
        testNoThreshold2("DELETE FROM %s WHERE k = 0 AND c1 = ? AND c2 = ?");
        testNoThreshold2("DELETE FROM %s WHERE k = 0 AND c1 = ? AND c2 > ?");
        testNoThreshold2("DELETE FROM %s WHERE k = 0 AND c1 = ? AND c2 < ?");
        testNoThreshold2("DELETE FROM %s WHERE k = 0 AND c1 = ? AND c2 >= ?");
        testNoThreshold2("DELETE FROM %s WHERE k = 0 AND c1 = ? AND c2 <= ?");
    }

    @Test
    public void testRegularColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + columnType() + ")");

        testThreshold("v", "INSERT INTO %s (k, v) VALUES (0, ?)");
        testThreshold("v", "UPDATE %s SET v = ? WHERE k = 0");
    }

    @Test
    public void testStaticColumn() throws Throwable
    {
        createTable("CREATE TABLE %s (k int, c int, s " + columnType() + " STATIC, r int, PRIMARY KEY(k, c))");

        testThreshold("s", "INSERT INTO %s (k, s) VALUES (0, ?)");
        testThreshold("s", "INSERT INTO %s (k, c, s, r) VALUES (0, 0, ?, 0)");
        testThreshold("s", "UPDATE %s SET s = ? WHERE k = 0");
        testThreshold("s", "UPDATE %s SET s = ?, r = 0 WHERE k = 0 AND c = 0");
    }

    @Test
    public void testBatch() throws Throwable
    {
        createTable("CREATE TABLE %s (k " + columnType() + ", c " + columnType() + ", r " + columnType() + ", s " + columnType() + " STATIC, PRIMARY KEY(k, c))");

        // partition key
        testNoThreshold("BEGIN BATCH INSERT INTO %s (k, c, r) VALUES (?, '0', '0'); APPLY BATCH;");
        testNoThreshold("BEGIN BATCH UPDATE %s SET r = '0' WHERE k = ? AND c = '0'; APPLY BATCH;");
        testNoThreshold("BEGIN BATCH DELETE r FROM %s WHERE k = ? AND c = '0'; APPLY BATCH;");
        testNoThreshold("BEGIN BATCH DELETE FROM %s WHERE k = ?; APPLY BATCH;");

        // static column
        testThreshold("s", "BEGIN BATCH INSERT INTO %s (k, s) VALUES ('0', ?); APPLY BATCH;");
        testThreshold("s", "BEGIN BATCH INSERT INTO %s (k, s, c, r) VALUES ('0', ?, '0', '0'); APPLY BATCH;");
        testThreshold("s", "BEGIN BATCH UPDATE %s SET s = ? WHERE k = '0'; APPLY BATCH;");
        testThreshold("s", "BEGIN BATCH UPDATE %s SET s = ?, r = '0' WHERE k = '0' AND c = '0'; APPLY BATCH;");

        // clustering key
        testNoThreshold("BEGIN BATCH INSERT INTO %s (k, c, r) VALUES ('0', ?, '0'); APPLY BATCH;");
        testNoThreshold("BEGIN BATCH UPDATE %s SET r = '0' WHERE k = '0' AND c = ?; APPLY BATCH;");
        testNoThreshold("BEGIN BATCH DELETE r FROM %s WHERE k = '0' AND c = ?; APPLY BATCH;");
        testNoThreshold("BEGIN BATCH DELETE FROM %s WHERE k = '0' AND c = ?; APPLY BATCH;");

        // regular column
        testThreshold("r", "BEGIN BATCH INSERT INTO %s (k, c, r) VALUES ('0', '0', ?); APPLY BATCH;");
        testThreshold("r", "BEGIN BATCH UPDATE %s SET r = ? WHERE k = '0' AND c = '0'; APPLY BATCH;");
    }

    @Test
    public void testCASWithIfNotExistsCondition() throws Throwable
    {
        createTable("CREATE TABLE %s (k " + columnType() + ", c " + columnType() + ", v " + columnType() + ", s " + columnType() + " STATIC, PRIMARY KEY(k, c))");

        // partition key
        testNoThreshold("INSERT INTO %s (k, c, v) VALUES (?, '0', '0') IF NOT EXISTS");

        // clustering key
        testNoThreshold("INSERT INTO %s (k, c, v) VALUES ('0', ?, '0') IF NOT EXISTS");

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
    public void testCASWithIfExistsCondition() throws Throwable
    {
        createTable("CREATE TABLE %s (k " + columnType() + ", c " + columnType() + ", v " + columnType() + ", s " + columnType() + " STATIC, PRIMARY KEY(k, c))");

        // partition key, the CAS updates with values beyond the threshold are not applied, so they don't come to fail
        testNoThreshold("UPDATE %s SET v = '0' WHERE k = ? AND c = '0' IF EXISTS");

        // clustering key, the CAS updates with values beyond the threshold are not applied, so they don't come to fail
        testNoThreshold("UPDATE %s SET v = '0' WHERE k = '0' AND c = ? IF EXISTS");

        // static column, only the applied CAS updates can fire the guardrail
        assertValid("INSERT INTO %s (k, s) VALUES ('0', '0')");
        testThreshold("s", "UPDATE %s SET s = ? WHERE k = '0' IF EXISTS");
        assertValid("DELETE FROM %s WHERE k = '0'");
        testNoThreshold("UPDATE %s SET s = ? WHERE k = '0' IF EXISTS");

        // regular column, only the applied CAS updates can fire the guardrail
        assertValid("INSERT INTO %s (k, c) VALUES ('0', '0')");
        testThreshold("v", "UPDATE %s SET v = ? WHERE k = '0' AND c = '0' IF EXISTS");
        assertValid("DELETE FROM %s WHERE k = '0' AND c = '0'");
        testNoThreshold("UPDATE %s SET v = ? WHERE k = '0' AND c = '0' IF EXISTS");
    }

    @Test
    public void testCASWithColumnsCondition() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v " + columnType() + ")");

        // updates are always accepted for values lesser than the threshold, independently of whether they are applied
        assertValid("DELETE FROM %s WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(1));
        assertValid("UPDATE %s SET v = '0' WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(1));

        // updates are always accepted for values equals to the threshold, independently of whether they are applied
        assertValid("DELETE FROM %s WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(WARN_THRESHOLD));
        assertValid("UPDATE %s SET v = '0' WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(WARN_THRESHOLD));

        // updates beyond the threshold fail only if the update is applied
        assertValid("DELETE FROM %s WHERE k = 0");
        assertValid("UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(WARN_THRESHOLD + 1));
        assertValid("UPDATE %s SET v = '0' WHERE k = 0");
        assertWarns("v", "UPDATE %s SET v = ? WHERE k = 0 IF v = '0'", allocate(WARN_THRESHOLD + 1));
    }

    @Test
    public void testSelect() throws Throwable
    {
        createTable("CREATE TABLE %s (k " + columnType() + ", c " + columnType() + ", r " + columnType() + ", s " + columnType() + " STATIC, PRIMARY KEY(k, c))");

        // the guardail is only checked for writes; reads are excluded

        testNoThreshold("SELECT * FROM %s WHERE k = ?");
        testNoThreshold("SELECT * FROM %s WHERE k = '0' AND c = ?");
        testNoThreshold("SELECT * FROM %s WHERE c = ? ALLOW FILTERING");
        testNoThreshold("SELECT * FROM %s WHERE s = ? ALLOW FILTERING");
        testNoThreshold("SELECT * FROM %s WHERE r = ? ALLOW FILTERING");
    }
}
