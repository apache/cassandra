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
            column_t c1 = new column_t("c1", "0", 0L);
            column_t c2 = new column_t("c2", "0", 0L);
            List<column_t> columns = new ArrayList<column_t>();
            columns.add(c1);
            columns.add(c2);
            Map<String, List<column_t>> cfmap = new HashMap<String, List<column_t>>();
            cfmap.put("Standard1", columns);
            cfmap.put("Standard2", columns);

            batch_mutation_t m = new batch_mutation_t("Table1", "key1", cfmap);
            server.batch_insert(m, 1);

            column_t column;
            column = server.get_column("Table1", "key1", "Standard1:c2");
            assert column.value.equals("0");

            column = server.get_column("Table1", "key1", "Standard2:c2");
            assert column.value.equals("0");

            ArrayList<column_t> column_ts = server.get_slice_strong("Table1", "key1", "Standard1", -1, -1);
            assert column_ts.size() == 2;
        } finally {
            server.shutdown();
        }
        */
    }
}
