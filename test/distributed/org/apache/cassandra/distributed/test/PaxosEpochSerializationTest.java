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

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.CustomTransformation;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.cassandra.net.Verb.PAXOS_COMMIT_REQ;

public class PaxosEpochSerializationTest extends TestBaseImpl
{
    private static final Logger logger = LoggerFactory.getLogger(PaxosEpochSerializationTest.class);
    private static final String TABLE = "tbl";

    @Test
    public void epochBadDeserializationTest() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3).withConfig(cfg -> cfg.with(Feature.NETWORK)
                                                                          .with(Feature.GOSSIP))
                                           .withoutVNodes().start()))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + '.' + TABLE + " (pk text primary key, v int)");
            cluster.get(1).runOnInstance(() -> {
                // just execute transformations to get epoch bumped enough for the bug to occur
                for (int i = 0; i < 75; i++)
                    ClusterMetadataService.instance().commit(CustomTransformation.make("x"+i));
            });
            // and bump the epoch in the tablemetadata:
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + '.' + TABLE + " WITH comment='abc'");

            cluster.verbs(PAXOS_COMMIT_REQ).drop();
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + '.' + TABLE + " (pk, v) VALUES ('xyzxyzxyzxyzxyzxyzxyzxyz', 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                Assert.fail("expected write timeout");
            }
            catch (RuntimeException e)
            {
                // exception expected
            }

            cluster.filters().reset();
            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
        }
    }
}
