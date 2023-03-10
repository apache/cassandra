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

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.distributed.api.ConsistencyLevel.ALL;

public class OversizedMutationTest extends TestBaseImpl
{
    @Test
    public void testSingleOversizedMutation() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(1).withConfig(c -> c.set("max_mutation_size", "48KiB"))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.t (key int PRIMARY KEY, val blob)"));
            String payload = StringUtils.repeat('1', 1024 * 49);
            String query = "INSERT INTO %s.t (key, val) VALUES (1, text_as_blob('" + payload + "'))";
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(withKeyspace(query), ALL))
                      .hasMessageContaining("Rejected an oversized mutation (")
                      .hasMessageContaining("/49152) for keyspace: distributed_test_keyspace. Top keys are: t.1");
        }
    }

    @Test
    public void testOversizedBatch() throws Throwable
    {
        try (Cluster cluster = init(builder().withNodes(1).withConfig(c -> c.set("max_mutation_size", "48KiB"))
                                             .start()))
        {
            cluster.schemaChange(withKeyspace("CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};"));
            cluster.schemaChange(withKeyspace("CREATE TABLE ks1.t (key int PRIMARY KEY, val blob)"));
            String payload = StringUtils.repeat('1', 1024 * 48);
            String query = "BEGIN BATCH\n" +
                           "INSERT INTO ks1.t (key, val) VALUES (1, text_as_blob('" + payload + "'))\n" +
                           "INSERT INTO ks1.t (key, val) VALUES (2, text_as_blob('222'))\n" +
                           "APPLY BATCH";
            Assertions.assertThatThrownBy(() -> cluster.coordinator(1).execute(withKeyspace(query), ALL))
                      .hasMessageContaining("Rejected an oversized mutation (")
                      .hasMessageContaining("/49152) for keyspace: ks1. Top keys are: t.1");
        }
    }
}
