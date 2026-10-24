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

import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.service.StorageService;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * CASSANDRA-11745: byte-paged replica filtering must not buffer the entire partition before returning a page.
 */
public class BytePagingReplicaFilteringTest extends TestBaseImpl
{
    private static final int ROWS = 12;
    private static final String VALUE = "x".repeat(256);
    private static final String UPDATED_VALUE = "y".repeat(256);

    @Test
    public void replicaFilteringDoesNotOverloadForSmallBytePages() throws Throwable
    {
        // HARNESS: two real replicas are required for digest reconciliation and replica filtering protection.
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(config -> config.set("hinted_handoff_enabled", false))
                                           .start()))
        {
            String table = KEYSPACE + ".rfp_byte_pages";
            cluster.schemaChange("CREATE TABLE " + table + " (pk int, ck int, flag int, v text, PRIMARY KEY (pk, ck))");
            cluster.get(1).runOnInstance(() -> {
                StorageService.instance.setCachedReplicaRowsWarnThreshold(8);
                StorageService.instance.setCachedReplicaRowsFailThreshold(8);
            });

            for (int pk = 0; pk < 2; pk++)
            {
                for (int node = 1; node <= 2; node++)
                {
                    for (int ck = 0; ck < ROWS; ck++)
                        cluster.get(node).executeInternal("INSERT INTO " + table +
                                                          " (pk, ck, flag, v) VALUES (?, ?, 1, ?) USING TIMESTAMP 1",
                                                          pk, ck, VALUE);
                }

                // TRIGGER: differing first-page data forces reconciliation even though the filter matches on both nodes.
                cluster.get(2).executeInternal("UPDATE " + table + " USING TIMESTAMP 2 SET v = ? WHERE pk = ? AND ck = 0",
                                              UPDATED_VALUE, pk);
            }

            String query = "SELECT ck, v FROM " + table + " WHERE pk = ? AND flag = 1 ALLOW FILTERING";

            // Use a separate partition for the row-paging control so read repair cannot mask the byte-paging failure.
            assertResults(cluster.coordinator(1).executeWithPaging(query, ALL, 1, 0));
            assertTrue("the divergent control read must exercise replica filtering protection",
                       cluster.get(1).callOnInstance(() -> Keyspace.open(KEYSPACE)
                                                                 .getColumnFamilyStore("rfp_byte_pages")
                                                                 .metric.rfpRowsCachedPerQuery.getCount()) > 0);

            // ORACLE: one row fills each byte page, so the query must return all rows without exceeding eight buffered versions.
            assertResults(cluster.coordinator(1).executeWithPagingInBytes(query, ALL, 128, 1));
        }
    }

    private static void assertResults(Iterator<Object[]> rows)
    {
        int ck = 0;
        while (rows.hasNext())
        {
            assertTrue("query returned more rows than were written", ck < ROWS);
            assertArrayEquals("wrong result at clustering key " + ck,
                              new Object[]{ ck, ck == 0 ? UPDATED_VALUE : VALUE },
                              rows.next());
            ck++;
        }
        assertEquals("query must return every matching row", ROWS, ck);
    }
}
