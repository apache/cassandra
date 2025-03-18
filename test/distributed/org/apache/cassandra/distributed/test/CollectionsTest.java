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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;

import static org.apache.cassandra.distributed.shared.AssertUtils.assertRows;
import static org.apache.cassandra.distributed.shared.AssertUtils.row;

public class CollectionsTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setUpCluster() throws IOException
    {
        CLUSTER = init(Cluster.build(2).start());
    }

    @Test
    public void testMultipleSetsComplexDeletion()
    {
        CLUSTER.schemaChange(withKeyspace("CREATE TABLE %s.multi_collection (k int, c int, s1 set<int>, s2 set<int>, s3 set<int>, PRIMARY KEY (k, c)) WITH read_repair = 'NONE'"));
        CLUSTER.coordinator(1).execute(withKeyspace("INSERT INTO %s.multi_collection (k, c, s1, s2, s3) VALUES (?, ?, ?, ?, ?)"), ConsistencyLevel.ALL, 0, 0, set(1), set(1), set(1));
        CLUSTER.coordinator(1).execute(withKeyspace("UPDATE %s.multi_collection SET s2 = ?, s1 = s1 + ?, s3 = s3 + ? WHERE k = ? AND c = ?"), ConsistencyLevel.ALL, set(2), set(2), set(2), 0, 0);

        String select = withKeyspace("SELECT k, c, s1, s2, s3 FROM %s.multi_collection");
        assertRows(CLUSTER.get(1).executeInternal(select), row(0, 0, set(1, 2), set(2), set(1, 2)));
        
        // If the complex deletion is not properly serialized, node 2 will think the update on s2 was an append... 
        assertRows(CLUSTER.get(2).executeInternal(select), row(0, 0, set(1, 2), set(2), set(1, 2)));
    }
}
