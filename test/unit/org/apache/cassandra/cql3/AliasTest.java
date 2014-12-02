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

public class AliasTest extends CQLTester
{
    @Test
    public void testAlias() throws Throwable
    {
        createTable("CREATE TABLE %s (id int PRIMARY KEY, name text)");

        for (int i = 0; i < 5; i++)
            execute("INSERT INTO %s (id, name) VALUES (?, ?) USING TTL 10 AND TIMESTAMP 0", i, Integer.toString(i));

        assertInvalidMessage("Aliases aren't allowed in the where clause" ,
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE user_id = 0");

        // test that select throws a meaningful exception for aliases in order by clause
        assertInvalidMessage("Aliases are not allowed in order by clause",
                             "SELECT id AS user_id, name AS user_name FROM %s WHERE id IN (0) ORDER BY user_name");

    }
}
