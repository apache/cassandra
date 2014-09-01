/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.db;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;

public class RemoveCellTest extends CQLTester
{
    @Test
    public void testDeleteCell() throws Throwable
    {
        String tableName = createTable("CREATE TABLE %s (a int, b int, c int, PRIMARY KEY (a, b))");
        ColumnFamilyStore cfs = Keyspace.open(KEYSPACE).getColumnFamilyStore(tableName);
        execute("INSERT INTO %s (a, b, c) VALUES (?, ?, ?) USING TIMESTAMP ?", 0, 0, 0, 0L);
        cfs.forceBlockingFlush();
        execute("DELETE c FROM %s USING TIMESTAMP ? WHERE a = ? AND b = ?", 1L, 0, 0);
        assertRows(execute("SELECT * FROM %s WHERE a = ? AND b = ?", 0, 0), row(0, 0, null));
        assertRows(execute("SELECT c FROM %s WHERE a = ? AND b = ?", 0, 0), row(new Object[]{null}));
    }
}
