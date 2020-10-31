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

package org.apache.cassandra.distributed.test;

import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;

public class PagingTest extends TestBaseImpl
{
    @Test
    public void testPaging() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).start());
             Cluster singleNode = init(builder().withNodes(1).withSubnet(1).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));
            singleNode.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck))"));

            for (int i = 0; i < 10; i++)
            {
                for (int j = 0; j < 10; j++)
                {
                    cluster.coordinator(1)
                           .execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, ?, ?)"), QUORUM, i, j, i + i);
                    singleNode.coordinator(1)
                              .execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (1, ?, ?)"), QUORUM, i, j, i + i);
                }
            }

            int[] pageSizes = new int[]{ 1, 2, 3, 5, 10, 20, 50, Integer.MAX_VALUE };
            String[] statements = new String[]{ withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck >= 5"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 AND ck <= 10"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 LIMIT 3"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck >= 5 LIMIT 2"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 LIMIT 2"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 ORDER BY ck DESC LIMIT 3"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck >= 5 ORDER BY ck DESC LIMIT 2"),
                                                withKeyspace("SELECT * FROM %s.tbl WHERE pk = 1 AND ck > 5 AND ck <= 10 ORDER BY ck DESC LIMIT 2"),
                                                withKeyspace("SELECT DISTINCT pk FROM %s.tbl LIMIT 3"),
                                                withKeyspace("SELECT DISTINCT pk FROM %s.tbl WHERE pk IN (3,5,8,10)"),
                                                withKeyspace("SELECT DISTINCT pk FROM %s.tbl WHERE pk IN (3,5,8,10) LIMIT 2")
            };
            for (String statement : statements)
            {
                Object[][] noPagingRows = singleNode.coordinator(1).execute(statement, QUORUM);
                for (int pageSize : pageSizes)
                {
                    Iterator<Object[]> pagingRows = cluster.coordinator(1).executeWithPaging(statement, QUORUM, pageSize);
                    assertRows(Iterators.toArray(pagingRows, Object[].class), noPagingRows);
                }
            }
        }
    }
}
