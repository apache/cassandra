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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.assertj.core.api.Assertions;

public class NativeMixedVersionTest extends TestBaseImpl
{
    @Test
    public void v5NoLongerLeavesClientWarnings() throws IOException
    {
        System.setProperty("io.netty.eventLoopThreads", "1");
        try (Cluster cluster = Cluster.build(1)
                                      .withConfig(c ->
                                                  c.with(Feature.values())
                                                   .set("track_warnings", ImmutableMap.of(
                                                       "enabled", true,
                                                       "local_read_size", ImmutableMap.of("warn_threshold_kb", 1)
                                                   ))
                                      )
                                      .start())
        {
            init(cluster);
            cluster.schemaChange(withKeyspace("CREATE TABLE %s.tbl (pk int, ck1 int, value blob, PRIMARY KEY (pk, ck1))"));
            IInvokableInstance node = cluster.get(1);

            ByteBuffer blob = ByteBuffer.wrap("This is just some large string to get a some number of bytes".getBytes(StandardCharsets.UTF_8));

            for (int i = 0; i < 100; i++)
                node.executeInternal(withKeyspace("INSERT INTO %s.tbl (pk, ck1, value) VALUES (?, ?, ?)"), 0, i, blob);

            // make requests with v5 to corrupt network state
            try (com.datastax.driver.core.Cluster driver = JavaDriverUtils.create(cluster, ProtocolVersion.V5);
                 Session session = driver.connect())
            {
                ResultSet rs = session.execute(withKeyspace("SELECT * FROM %s.tbl"));
                Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isNotEmpty();
            }

            try (com.datastax.driver.core.Cluster driver = JavaDriverUtils.create(cluster, ProtocolVersion.V3);
                 Session session = driver.connect())
            {
                ResultSet rs = session.execute(withKeyspace("SELECT * FROM %s.tbl"));
                Assertions.assertThat(rs.getExecutionInfo().getWarnings()).isEmpty();
            }

            // this should no longer happen; so make sure no logs are found
            List<String> result = node.logs().grep("Warnings present in message with version less than").getResult();
            Assertions.assertThat(result).isEmpty();
        }
        finally
        {
            System.clearProperty("io.netty.eventLoopThreads");
        }
    }
}
