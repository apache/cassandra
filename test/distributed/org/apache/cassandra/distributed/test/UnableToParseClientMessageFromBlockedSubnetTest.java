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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeoutException;

import com.google.common.collect.ImmutableMap;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.api.LogAction;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.SimpleClient;
import org.apache.cassandra.transport.messages.ErrorMessage;
import org.assertj.core.api.Assertions;

@RunWith(Parameterized.class)
public class UnableToParseClientMessageFromBlockedSubnetTest extends TestBaseImpl
{
    private static Cluster CLUSTER;
    private static List<String> CLUSTER_EXCLUDED_SUBNETS;

    @SuppressWarnings("DefaultAnnotationParam")
    @Parameterized.Parameter(0)
    public List<String> excludeSubnets;
    @Parameterized.Parameter(1)
    public ProtocolVersion version;

    @Parameterized.Parameters(name = "domains={0},version={1}")
    public static Iterable<Object[]> params()
    {
        List<Object[]> tests = new ArrayList<>();
        for (List<String> domains : Arrays.asList(Collections.singletonList("127.0.0.1"), Collections.singletonList("127.0.0.0/31")))
        {
            for (ProtocolVersion version : ProtocolVersion.SUPPORTED)
            {
                tests.add(new Object[] { domains, version });
            }
        }
        return tests;
    }

    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @AfterClass
    public static void cleanup()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void badMessageCausesProtocolExceptionFromExcludeList() throws IOException, TimeoutException
    {
        Cluster cluster = getCluster();
        // write gibberish to the native protocol
        IInvokableInstance node = cluster.get(1);
        // make sure everything is fine at the start
        Assertions.assertThat(node.metrics().getCounter("org.apache.cassandra.metrics.Client.ProtocolException")).isEqualTo(0);
        Assertions.assertThat(node.metrics().getCounter("org.apache.cassandra.metrics.Client.UnknownException")).isEqualTo(0);

        LogAction logs = node.logs();
        long mark = logs.mark();
        try (SimpleClient client = SimpleClient.builder("127.0.0.1", 9042).protocolVersion(version).useBeta().build())
        {
            client.connect(false, true);

            // this should return a failed response
            // disable waiting on procol errors as that logic was reverted until we can figure out its 100% safe
            // right now ProtocolException is thrown for fatal and non-fatal issues, so closing the channel
            // on non-fatal issues could cause other issues for the cluster
            byte expectedVersion = (byte) (80 + version.asInt());
            Message.Response response = client.execute(new UnableToParseClientMessageTest.CustomHeaderMessage(new byte[]{ expectedVersion, 1, 2, 3, 4, 5, 6, 7, 8, 9 }), false);
            Assertions.assertThat(response).isInstanceOf(ErrorMessage.class);

            logs.watchFor(mark, "address contained in client_error_reporting_exclusions");
            Assertions.assertThat(node.metrics().getCounter("org.apache.cassandra.metrics.Client.ProtocolException")).isEqualTo(0);
            Assertions.assertThat(node.metrics().getCounter("org.apache.cassandra.metrics.Client.UnknownException")).isEqualTo(0);

            Assertions.assertThat(logs.grep(mark, "Excluding client exception fo").getResult()).hasSize(1);
            Assertions.assertThat(logs.grep(mark, "Unexpected exception during request").getResult()).isEmpty();
        }
    }

    private Cluster getCluster()
    {
        if (CLUSTER == null || CLUSTER_EXCLUDED_SUBNETS != excludeSubnets)
        {
            if (CLUSTER != null)
            {
                CLUSTER.close();
                CLUSTER = null;
            }
            try
            {
                CLUSTER = init(Cluster.build(1)
                                      .withConfig(c -> c.with(Feature.values()).set("client_error_reporting_exclusions", ImmutableMap.of("subnets", excludeSubnets)))
                                      .start());
                CLUSTER_EXCLUDED_SUBNETS = excludeSubnets;
            }
            catch (IOException e)
            {
                throw new UncheckedIOException(e);
            }
        }
        return CLUSTER;
    }
}
