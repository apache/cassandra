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

import org.apache.cassandra.distributed.api.ICluster;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;

// TODO: this test should be removed after running in-jvm dtests is set up via the shared API repository
public class GossipSettlesTest extends TestBaseImpl
{

    @Test
    public void testGossipSettles() throws Throwable
    {
        /* Use withSubnet(1) to prove seed provider is set correctly - without the fix to pass a seed provider, this test fails */
        try (ICluster cluster = builder().withNodes(3)
                                         .withConfig(config -> config.with(GOSSIP).with(NETWORK))
                                         .withSubnet(1)
                                         .start())
        {
        }
    }

}
