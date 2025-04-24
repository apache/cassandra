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
import org.apache.cassandra.distributed.api.NodeToolResult;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ClusterMetadataDumpTest extends TestBaseImpl
{
    @Test
    public void dumpLogTest() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .start()))
        {
            cluster.get(1).runOnInstance(() -> {
                for (int i = 0; i < 10; i++)
                    ClusterMetadataService.instance().commit(new CustomTransformation(CustomTransformation.PokeInt.NAME, new CustomTransformation.PokeInt(i)));
            });

            NodeToolResult res = cluster.get(1).nodetoolResult("cms", "dumplog");
            res.asserts().success();
            int unsafeJoinSeen = 0;
            int registerSeen = 0;
            int epochsSeen = 0;
            for (String l : res.getStdout().split("\n"))
            {
                if (l.contains("kind"))
                {
                    if (l.contains("REGISTER"))
                        registerSeen++;
                    else if (l.contains("UNSAFE_JOIN"))
                        unsafeJoinSeen++;
                }
                if (l.startsWith("Epoch:"))
                    epochsSeen++;
            }
            assertEquals(3, unsafeJoinSeen);
            assertEquals(3, registerSeen);
            assertTrue(epochsSeen > 15);

            res = cluster.get(1).nodetoolResult("cms", "dumplog", "--start", "10", "--end", "15");
            epochsSeen = 0;
            for (String l : res.getStdout().split("\n"))
            {
                if (l.startsWith("Epoch: "))
                {
                    epochsSeen++;
                    long epoch = Long.parseLong(l.split(": ")[1]);
                    assertTrue(epoch >= 10 && epoch <= 15);
                }
            }
            assertEquals(6, epochsSeen);
        }
    }

    @Test
    public void dumpDirectoryTest() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(3)
                                             .start()))
        {
            NodeToolResult res = cluster.get(1).nodetoolResult("cms", "dumpdirectory");
            res.asserts().success();
            int nodesFound = 0;
            for (String l : res.getStdout().split("\n"))
            {
                if (l.startsWith("NodeId"))
                    nodesFound++;
                assertFalse(l.contains("tokens"));
            }
            assertEquals(3, nodesFound);
            res = cluster.get(1).nodetoolResult("cms", "dumpdirectory", "--tokens");
            res.asserts().success();
            nodesFound = 0;
            int tokensFound = 0;
            for (String l : res.getStdout().split("\n"))
            {
                if (l.startsWith("NodeId"))
                    nodesFound++;

                if (l.contains("tokens"))
                    tokensFound++;
            }
            assertEquals(3, nodesFound);
            assertEquals(3, tokensFound);
        }
    }
}
