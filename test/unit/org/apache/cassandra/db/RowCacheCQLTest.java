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
package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.service.CacheService;
import static org.junit.Assert.assertEquals;

public class RowCacheCQLTest extends CQLTester
{
    @Test
    public void test7636() throws Throwable
    {
        CacheService.instance.setRowCacheCapacityInMB(1);
        createTable("CREATE TABLE %s (p1 bigint, c1 int, PRIMARY KEY (p1, c1)) WITH caching = '{\"keys\":\"NONE\", \"rows_per_partition\":\"ALL\"}'");
        execute("INSERT INTO %s (p1, c1) VALUES (123, 10)");
        assertEmpty(execute("SELECT * FROM %s WHERE p1=123 and c1 > 1000"));
        UntypedResultSet res = execute("SELECT * FROM %s WHERE p1=123 and c1 > 0");
        assertEquals(1, res.size());
        assertEmpty(execute("SELECT * FROM %s WHERE p1=123 and c1 > 1000"));
    }
}
