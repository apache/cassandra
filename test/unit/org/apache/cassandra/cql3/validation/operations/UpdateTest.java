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

package org.apache.cassandra.cql3.validation.operations;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.utils.ByteBufferUtil;

public class UpdateTest extends CQLTester
{
    /**
     * Test altering the type of a column, including the one in the primary key (#4041)
     * migrated from cql_tests.py:TestCQL.update_type_test()
     */
    @Test
    public void testUpdateColumnType() throws Throwable
    {
        createTable("CREATE TABLE %s (k text, c text, s set <text>, v text, PRIMARY KEY(k, c))");

        // using utf8 character so that we can see the transition to BytesType
        execute("INSERT INTO %s (k, c, v, s) VALUES ('ɸ', 'ɸ', 'ɸ', {'ɸ'})");

        assertRows(execute("SELECT * FROM %s"),
                   row("ɸ", "ɸ", set("ɸ"), "ɸ"));

        execute("ALTER TABLE %s ALTER v TYPE blob");
        assertRows(execute("SELECT * FROM %s"),
                   row("ɸ", "ɸ", set("ɸ"), ByteBufferUtil.bytes("ɸ")));

        execute("ALTER TABLE %s ALTER k TYPE blob");
        assertRows(execute("SELECT * FROM %s"),
                   row(ByteBufferUtil.bytes("ɸ"), "ɸ", set("ɸ"), ByteBufferUtil.bytes("ɸ")));

        execute("ALTER TABLE %s ALTER c TYPE blob");
        assertRows(execute("SELECT * FROM %s"),
                   row(ByteBufferUtil.bytes("ɸ"), ByteBufferUtil.bytes("ɸ"), set("ɸ"), ByteBufferUtil.bytes("ɸ")));

        execute("ALTER TABLE %s ALTER s TYPE set<blob>");
        assertRows(execute("SELECT * FROM %s"),
                   row(ByteBufferUtil.bytes("ɸ"), ByteBufferUtil.bytes("ɸ"), set(ByteBufferUtil.bytes("ɸ")), ByteBufferUtil.bytes("ɸ")));
    }

    @Test
    public void testTypeCasts() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, t text, a ascii, d double, i int)");

        // The followings is fine
        execute("UPDATE %s SET t = 'foo' WHERE k = ?", 0);
        execute("UPDATE %s SET t = (ascii)'foo' WHERE k = ?", 0);
        execute("UPDATE %s SET t = (text)(ascii)'foo' WHERE k = ?", 0);
        execute("UPDATE %s SET a = 'foo' WHERE k = ?", 0);
        execute("UPDATE %s SET a = (ascii)'foo' WHERE k = ?", 0);

        // But trying to put some explicitely type-casted text into an ascii
        // column should be rejected (even though the text is actually ascci)
        assertInvalid("UPDATE %s SET a = (text)'foo' WHERE k = ?", 0);

        // This is also fine because integer constants works for both integer and float types
        execute("UPDATE %s SET i = 3 WHERE k = ?", 0);
        execute("UPDATE %s SET i = (int)3 WHERE k = ?", 0);
        execute("UPDATE %s SET d = 3 WHERE k = ?", 0);
        execute("UPDATE %s SET d = (double)3 WHERE k = ?", 0);

        // But values for ints and doubles are not truly compatible (their binary representation differs)
        assertInvalid("UPDATE %s SET d = (int)3 WHERE k = ?", 0);
        assertInvalid("UPDATE %s SET i = (double)3 WHERE k = ?", 0);
    }
}
