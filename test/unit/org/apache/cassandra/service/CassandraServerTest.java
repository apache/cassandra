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
package org.apache.cassandra.service;

import org.junit.Test;

import org.apache.cassandra.CleanupHelper;

public class CassandraServerTest extends CleanupHelper
{
    @Test
    public void test_get_column() throws Throwable {
        /*
        CassandraServer server = new CassandraServer();
        server.start();

        try {
            Column c1 = column("c1", "0", 0L);
            Column c2 = column("c2", "0", 0L);
            List<Column> columns = new ArrayList<Column>();
            columns.add(c1);
            columns.add(c2);
            Map<String, List<Column>> cfmap = new HashMap<String, List<Column>>();
            cfmap.put("Standard1", columns);
            cfmap.put("Standard2", columns);

            BatchMutation m = new BatchMutation("Keyspace1", "key1", cfmap);
            server.batch_insert(m, 1);

            Column column;
            column = server.get_column("Keyspace1", "key1", "Standard1:c2");
            assert column.value.equals("0");

            column = server.get_column("Keyspace1", "key1", "Standard2:c2");
            assert column.value.equals("0");

            ArrayList<Column> Columns = server.get_slice_strong("Keyspace1", "key1", "Standard1", -1, -1);
            assert Columns.size() == 2;
        } finally {
            server.shutdown();
        }
        */
    }
}
