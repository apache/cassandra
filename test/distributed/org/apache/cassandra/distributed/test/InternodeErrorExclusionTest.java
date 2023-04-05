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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.transport.SimpleClient;

import static org.assertj.core.api.Assertions.assertThat;

public class InternodeErrorExclusionTest extends TestBaseImpl
{
    @BeforeClass
    public static void beforeClass2()
    {
        DatabaseDescriptor.clientInitialization();
    }

    // Connect a simple native client to the internode port (which fails on the protocol magic check)
    // and make sure the exception is swallowed.
    @Test
    public void ignoreExcludedInternodeErrors() throws IOException, TimeoutException
    {
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(c -> c
                                                       .with(Feature.NETWORK)
                                                       .set("internode_error_reporting_exclusions", ImmutableMap.of("subnets", Arrays.asList("127.0.0.1"))))
                                      .start())
        {
            try (SimpleClient client = SimpleClient.builder("127.0.0.1", 7012).build())
            {
                client.connect(true);
                Assert.fail("Connection should fail");
            }
            catch (Exception e)
            {
                // expected
            }
            assertThat(cluster.get(1).logs().watchFor("address contained in internode_error_reporting_exclusions").getResult()).hasSize(1);
        }
    }
}
