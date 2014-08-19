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

public class SingleColumnRelationTest extends CQLTester
{
    @Test
    public void testInvalidCollectionEqualityRelation() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b set<int>, c list<int>, d map<int, int>)");
        createIndex("CREATE INDEX ON %s (b)");
        createIndex("CREATE INDEX ON %s (c)");
        createIndex("CREATE INDEX ON %s (d)");

        assertInvalid("SELECT * FROM %s WHERE a = 0 AND b=?", set(0));
        assertInvalid("SELECT * FROM %s WHERE a = 0 AND c=?", list(0));
        assertInvalid("SELECT * FROM %s WHERE a = 0 AND d=?", map(0, 0));
    }

    @Test
    public void testInvalidCollectionNonEQRelation() throws Throwable
    {
        createTable("CREATE TABLE %s (a int PRIMARY KEY, b set<int>, c int)");
        createIndex("CREATE INDEX ON %s (c)");
        execute("INSERT INTO %s (a, b, c) VALUES (0, {0}, 0)");

        // non-EQ operators
        assertInvalid("SELECT * FROM %s WHERE c = 0 AND b > ?", set(0));
        assertInvalid("SELECT * FROM %s WHERE c = 0 AND b >= ?", set(0));
        assertInvalid("SELECT * FROM %s WHERE c = 0 AND b < ?", set(0));
        assertInvalid("SELECT * FROM %s WHERE c = 0 AND b <= ?", set(0));
        assertInvalid("SELECT * FROM %s WHERE c = 0 AND b IN (?)", set(0));
    }
}
