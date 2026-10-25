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

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class GroupByShortReadProtectionTest extends TestBaseImpl
{
    /**
     * CASSANDRA-21340: reconciling partial deletions must not truncate multi-row groups during paging.
     */
    @Test
    public void testGroupByPartialCrossNodeDeletions() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                            .withConfig(config -> config.set("hinted_handoff_enabled", false))
                                            .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v int, PRIMARY KEY (pk, ck)) " +
                                              "WITH read_repair = 'NONE'"));

            for (int pk = 0; pk < 15; pk++)
                for (int ck = 0; ck < 6; ck++)
                    cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v) VALUES (?, ?, ?) USING TIMESTAMP 0"),
                                                   ALL, pk, ck, 1);

            // Each replica retains live rows, but reconciliation deletes all rows in partitions 0-4.
            for (int pk = 0; pk < 5; pk++)
            {
                for (int ck = 0; ck < 6; ck += 2)
                    cluster.get(1).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 1 WHERE pk = ? AND ck = ?"), pk, ck);
                for (int ck = 1; ck < 6; ck += 2)
                    cluster.get(2).executeInternal(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 1 WHERE pk = ? AND ck = ?"), pk, ck);
            }

            String query = withKeyspace("SELECT pk, count(*) FROM %s.tbl GROUP BY pk LIMIT 5");
            // The first five surviving partitions in token order, each containing six rows.
            Object[][] expected = { row(5, 6L), row(10, 6L), row(13, 6L), row(11, 6L), row(8, 6L) };
            assertRows(cluster.coordinator(1).execute(query, ALL), expected);
            for (int pageSize : new int[]{ 1, 2, 3, 5, 6, 10 })
                assertRows(cluster.coordinator(1).executeWithPaging(query, ALL, pageSize), expected);
        }
    }
}
