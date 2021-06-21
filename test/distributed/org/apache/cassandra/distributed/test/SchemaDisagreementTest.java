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

import org.apache.cassandra.distributed.Cluster;

import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;

public class SchemaDisagreementTest extends TestBaseImpl
{
    /**
     * If a node isn't aware of a column, but receives a mutation without that column, the write should succeed.
     */
    @Test
    public void writeWithInconsequentialSchemaDisagreement() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(3).withConfig(config -> config.with(NETWORK)).start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck int, v1 int, PRIMARY KEY (pk, ck))"));

            cluster.get(1).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));
            cluster.get(2).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));
            cluster.get(3).executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (1, 1, 1)"));

            // Introduce schema disagreement
            cluster.schemaChange(withKeyspace("ALTER TABLE %s.tbl ADD v2 int"), 1);

            // this write shouldn't cause any problems because it doesn't write to the new column
            cluster.coordinator(1).execute(withKeyspace("INSERT INTO %s.tbl (pk, ck, v1) VALUES (2, 2, 2)"), ALL);
        }
    }
}
