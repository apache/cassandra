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

import org.junit.Test;
import org.junit.Assert;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.schema.TableId;

public class DurableWritesTest extends TestBaseImpl
{
    @Test
    public void durableWritesDisabledTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.set("commitlog_segment_size_in_mb", 1)
                                           )
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("DROP KEYSPACE %s"));
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1} AND DURABLE_WRITES = false"));
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (k INT PRIMARY KEY, a INT, b INT, c INT)"));

            for (int i = 1; i <= 1000; i++)
            {
                cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (k, a, b, c) VALUES (1, 1, 1, 1)"));
            }

            cluster.get(1).runOnInstance(() -> {
                TableId wanted = TableId.fromString(Keyspace.open(KEYSPACE).getColumnFamilyStore("tbl").metadata.id.toString());
                boolean containsTbl = CommitLog.instance.segmentManager
                    .getActiveSegments()
                    .stream()
                    .anyMatch(s -> s.getDirtyTableIds().contains(wanted));
                Assert.assertFalse("Commitlog should not contain data from 'tbl'", containsTbl);
            });
        }
    }

}
