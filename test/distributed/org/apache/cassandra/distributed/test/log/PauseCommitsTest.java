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
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.tcm.ClusterMetadataService;

import static org.junit.Assert.fail;


public class PauseCommitsTest extends TestBaseImpl
{
    @Test
    public void pauseCommitsTest() throws IOException
    {
        try (Cluster cluster = init(builder().withNodes(2)
                                             .start()))
        {
            cluster.get(2).runOnInstance(() -> ClusterMetadataService.instance().pauseCommits());
            cluster.schemaChange(withKeyspace("create table %s.y (id int primary key)"));
            try
            {
                cluster.coordinator(2).execute(withKeyspace("create table %s.z (id int primary key)"), ConsistencyLevel.ONE);
                fail();
            }
            catch (IllegalStateException e)
            {
                //ignore
            }
            cluster.get(2).runOnInstance(() -> ClusterMetadataService.instance().resumeCommits());
            cluster.coordinator(2).execute(withKeyspace("create table %s.z (id int primary key)"), ConsistencyLevel.ONE);
        }
    }
}
