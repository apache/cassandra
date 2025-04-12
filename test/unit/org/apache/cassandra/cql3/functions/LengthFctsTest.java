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

package org.apache.cassandra.cql3.functions;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.quicktheories.WithQuickTheories;

public class LengthFctsTest extends CQLTester implements WithQuickTheories
{
    @Test
    public void testOctetLengthNonStrings()
    {
        createTable("CREATE TABLE %s (a tinyint primary key,"
                    + " b smallint,"
                    + " c int,"
                    + " d bigint,"
                    + " e float,"
                    + " f double,"
                    + " g decimal,"
                    + " h varint,"
                    + " i int)");

        execute("INSERT INTO %s (a, b, c, d, e, f, g, h) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (byte) 1, (short) 2, 3, 4L, 5.2F, 6.3, BigDecimal.valueOf(6.3), BigInteger.valueOf(4));

        assertRows(execute("SELECT OCTET_LENGTH(a), " +
                           "OCTET_LENGTH(b), " +
                           "OCTET_LENGTH(c), " +
                           "OCTET_LENGTH(d), " +
                           "OCTET_LENGTH(e), " +
                           "OCTET_LENGTH(f), " +
                           "OCTET_LENGTH(g), " +
                           "OCTET_LENGTH(h), " +
                           "OCTET_LENGTH(i) FROM %s"),
                   row(1, 2, 4, 8, 4, 8, 5, 1, null));
    }

    @Test
    public void testStringLengthUTF8() throws Throwable
    {
        createTable("CREATE TABLE %s (key text primary key, value blob)");
        // UTF-8 7 codepoint, 21 byte encoded string
        String key = "こんにちは世界";
        execute("INSERT INTO %s (key) VALUES (?)", key);

        assertRows(execute("SELECT LENGTH(key), OCTET_LENGTH(key), OCTET_LENGTH(value) FROM %s where key = ?", key),
                   row(7, 21, null));

        // Quickly check that multiple arguments leads to an exception as expected
        assertInvalidMessage("Invalid number of arguments in call to function system.length",
                             "SELECT LENGTH(key, value) FROM %s where key = 'こんにちは世界'");
        assertInvalidMessage("Invalid call to function octet_length, none of its type signatures match",
                             "SELECT OCTET_LENGTH(key, value) FROM %s where key = 'こんにちは世界'");
    }

    @Test
    public void testOctetLengthStringFuzz()
    {
        createTable("CREATE TABLE %s (key text primary key, value blob)");

        qt().withExamples(1024).forAll(strings().allPossible().ofLengthBetween(32, 100)).checkAssert(
        (randString) -> {
            int sLen = randString.length();
            byte[] randBytes = randString.getBytes(StandardCharsets.UTF_8);

            // UTF-8 length (code unit count) and byte length are often
            // different. Spot checked a few of these, and they are different
            // most of the time in this test - but testing that reproducibly
            // requires seeding that would decrease the test power...
            execute("INSERT INTO %s (key, value) VALUES (?, ?)", randString, ByteBuffer.wrap(randBytes));
            assertRows(execute("SELECT LENGTH(key), OCTET_LENGTH(key), OCTET_LENGTH(value) FROM %s where key = ?", randString),
                       row(sLen, randBytes.length, randBytes.length));
        });
    }
}
