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
import org.apache.cassandra.utils.FBUtilities;

public class RestartTest extends TestBaseImpl
{
    @Test
    public void test() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2).withDataDirCount(1).start()))
        {
            FBUtilities.waitOnFuture(cluster.get(2).shutdown());
            FBUtilities.waitOnFuture(cluster.get(1).shutdown());
            cluster.get(1).startup();
            cluster.get(2).startup();
        }
    }
}
