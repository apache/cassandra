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

import org.junit.Test;

public class UserTypesTest extends CQLTester
{
    @Test
    public void testInvalidField() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (f int)");
        createTable("CREATE TABLE %s (k int PRIMARY KEY, v frozen<" + myType + ">)");

        // 's' is not a field of myType
        assertInvalid("INSERT INTO %s (k, v) VALUES (?, {s : ?})", 0, 1);
    }

    @Test
    public void testFor7684() throws Throwable
    {
        String myType = createType("CREATE TYPE %s (x double)");
        createTable("CREATE TABLE %s (k int, v frozen<" + myType + ">, b boolean static, PRIMARY KEY (k, v))");

        execute("INSERT INTO %s(k, v) VALUES (?, {x:?})", 1, -104.99251);
        execute("UPDATE %s SET b = ? WHERE k = ?", true, 1);

        assertRows(execute("SELECT v.x FROM %s WHERE k = ? AND v = {x:?}", 1, -104.99251),
            row(-104.99251)
        );

        flush();

        assertRows(execute("SELECT v.x FROM %s WHERE k = ? AND v = {x:?}", 1, -104.99251),
            row(-104.99251)
        );
    }

    @Test
    public void testNonFrozenUDT() throws Throwable
    {
        // Using a UDT without frozen shouldn't work
        String myType = createType("CREATE TYPE %s (f int)");
        assertInvalid("CREATE TABLE wrong (k int PRIMARY KEY, v " + myType + ")");
    }
}
