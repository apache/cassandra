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
package org.apache.cassandra.cql3;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.Arrays;
import java.util.Collections;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class NonNativeTimestampTest extends CQLTester
{
    @Test
    public void setServerTimestampForNonCqlNativeStatements() throws Throwable
    {
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v int)");

        execute("INSERT INTO %s (k, v) values (1, ?)", 2);

        UntypedResultSet.Row row = execute("SELECT v, writetime(v) AS wt FROM %s WHERE k = 1").one();
        assertEquals(2, row.getInt("v"));
        long timestamp1 = row.getLong("wt");
        assertFalse(timestamp1 == -1l);

        // per CASSANDRA-8246 the two updates will have the same (incorrect)
        // timestamp, so reconcilliation is by value and the "older" update wins
        execute("INSERT INTO %s (k, v) values (1, ?)", 1);

        row = execute("SELECT v, writetime(v) AS wt FROM %s WHERE k = 1").one();
        assertEquals(1, row.getInt("v"));
        assertTrue(row.getLong("wt") > timestamp1);
    }
}
