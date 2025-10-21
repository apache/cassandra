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

package org.apache.cassandra.distributed.test.ring;

import java.util.Collections;

import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.TokenSupplier;
import org.apache.cassandra.distributed.test.TestBaseImpl;

public class AssignSameTokenToMultipleNodesTest extends TestBaseImpl
{
    @Test
    public void testAssignSameTokenToMultipleNodes() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(2)
                                        .withTokenSupplier((TokenSupplier) i -> Collections.singleton("0"))
                                        .createWithoutStarting())
        {
            cluster.get(1).startup();
            try
            {
                cluster.get(2).startup();
                Assert.fail("Startup should have failed");
            }
            catch (Throwable t)
            {
                Assert.assertTrue(t.getClass().getName().contains("ConfigurationException"));
                Assert.assertTrue(t.getMessage().contains("Bootstrapping to existing token 0 is not allowed"));
            }
        }
    }
}
