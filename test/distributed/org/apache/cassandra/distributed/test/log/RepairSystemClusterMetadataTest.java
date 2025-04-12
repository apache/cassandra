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

import java.io.IOException;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

public class RepairSystemClusterMetadataTest extends TestBaseImpl
{
    @Test
    public void testGlobalRepair() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(6).withConfig(c -> c.with(Feature.NETWORK, Feature.GOSSIP))
                                             .start()))
        {
            cluster.get(1).nodetoolResult("cms", "reconfigure", "3");
            cluster.get(4).nodetoolResult("repair", "-force", "-st", "-1", "-et", "3074457345618258601", "system_cluster_metadata").asserts().success();
        }
    }
}
