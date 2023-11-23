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

package org.apache.cassandra.distributed.test.log;

import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BounceResetHostIdTest extends TestBaseImpl
{
    @Test
    public void bounceTest() throws Exception
    {
        try (Cluster cluster = init(builder().withNodes(1)
                                             .start()))
        {
            String wrongId = UUID.randomUUID().toString();
            cluster.get(1).runOnInstance(() -> {
                SystemKeyspace.setLocalHostId(UUID.fromString(wrongId));
                assertFalse(NodeId.isValidNodeId(SystemKeyspace.getLocalHostId()));
            });
            cluster.get(1).shutdown().get();
            cluster.get(1).startup();
            cluster.get(1).logs().watchFor("NodeId is wrong, updating from "+wrongId+" to "+(new NodeId(1).toUUID()));
            cluster.get(1).runOnInstance(() -> assertTrue(NodeId.isValidNodeId(SystemKeyspace.getLocalHostId())));
        }
    }
}
