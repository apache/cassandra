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

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;

public class PaxosUncommittedIndexTest extends TestBaseImpl
{
    @Test
    public void indexCqlIsExportableAndParsableTest() throws Throwable
    {
        String expectedCreateCustomIndex = "CREATE CUSTOM INDEX \"PaxosUncommittedIndex\" ON system.paxos () USING 'org.apache.cassandra.service.paxos.uncommitted.PaxosUncommittedIndex'";
        try (Cluster dtestCluster = init(Cluster.build(1).withConfig(c -> c.with(Feature.NATIVE_PROTOCOL)).start()))
        {
            try (com.datastax.driver.core.Cluster clientCluster = com.datastax.driver.core.Cluster.builder().addContactPoint("127.0.0.1").build())
            {
                Assert.assertTrue(clientCluster.getMetadata().exportSchemaAsString()
                                               .contains(expectedCreateCustomIndex));
                Throwable thrown = null;
                try
                {
                    dtestCluster.schemaChange(expectedCreateCustomIndex);
                }
                catch (Throwable tr)
                {
                    thrown = tr;
                }

                // Check parsing succeeds and index creation fails
                Assert.assertTrue(thrown.getMessage().contains("System keyspace 'system' is not user-modifiable"));
            }
        }
    }
}
