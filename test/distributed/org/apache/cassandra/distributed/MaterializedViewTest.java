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

package org.apache.cassandra.distributed;

import org.junit.Test;

import org.apache.cassandra.distributed.test.TestBaseImpl;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;

public class MaterializedViewTest extends TestBaseImpl
{
    @Test
    public void testDisablingMaterializedViewsDontFailNodeToStart() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.set("materialized_views_enabled", true))
                                           .start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (id uuid, col1 text, col2 text, primary key (id));");
            cluster.schemaChange("CREATE MATERIALIZED VIEW " + KEYSPACE + ".a_view AS SELECT id, col1, col2 " +
                                 "FROM tbl WHERE col2 IS NOT NULL AND id IS NOT NULL PRIMARY KEY (col2, id) " +
                                 "WITH CLUSTERING ORDER BY (id ASC);");
            cluster.coordinator(1).execute(withKeyspace("select * from %s.a_view"), ALL);

            cluster.get(1).shutdown().get();
            cluster.get(1).config().set("materialized_views_enabled", false);
            cluster.get(1).startup();

            cluster.coordinator(1).execute(withKeyspace("select * from %s.a_view"), ALL);
        }
    }
}
