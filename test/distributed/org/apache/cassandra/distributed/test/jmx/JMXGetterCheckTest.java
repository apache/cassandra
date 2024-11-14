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
package org.apache.cassandra.distributed.test.jmx;

import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.TestBaseImpl;

public class JMXGetterCheckTest extends TestBaseImpl
{

    @Test
    public void testGetters() throws Exception
    {
        for (int i=0; i < 2; i++)
        {
            try (Cluster cluster = Cluster.build(1).withConfig(c -> c.with(Feature.values())).start())
            {
                testAllValidGetters(cluster);
            }
        }
    }

    /**
     * Tests JMX getters and operations.
     * Useful for more than just testing getters, it also is used in JMXFeatureTest
     * to make sure we've touched the complete JMX code path. If you want to pass additional JMX Env for the client
     * JMX connection then use {@link JMXTestsUtil#testAllValidGetters(Cluster, Map)} instead.
     *
     * @param cluster the cluster to test
     * @throws Exception several kinds of exceptions can be thrown, mostly from JMX infrastructure issues.
     */
    public static void testAllValidGetters(Cluster cluster) throws Exception
    {
        JMXTestsUtil.testAllValidGetters(cluster, null);
    }
}
