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
import org.apache.cassandra.distributed.api.ConsistencyLevel;

public class DistinctReadTest extends TestBaseImpl
{
    @Test
    public void test() throws IOException
    {
        try (Cluster cluster = init(Cluster.build()
                                           .withNodes(1)
                                           .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (id int, ck int, x int, PRIMARY KEY (id, ck))"));
            cluster.coordinator(1).execute(withKeyspace("DELETE FROM %s.tbl USING TIMESTAMP 100 WHERE id = 1 AND ck < 10 "), ConsistencyLevel.ONE);
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (id, ck, x) VALUES (1, 5, 7) USING TIMESTAMP 101"), ConsistencyLevel.ONE);
            cluster.get(1).flush(KEYSPACE);
            // all these failed before fix;
            cluster.coordinator(1).execute(withKeyspace("select distinct id from %s.tbl where token(id) > " + Long.MIN_VALUE), ConsistencyLevel.ONE);
            cluster.coordinator(1).execute(withKeyspace("select distinct id from %s.tbl where id > 0 allow filtering"), ConsistencyLevel.ONE);
            cluster.coordinator(1).execute(withKeyspace("select id from %s.tbl where token(id) > " + Long.MIN_VALUE +" PER PARTITION LIMIT 1"), ConsistencyLevel.ONE);
        }
    }
}
