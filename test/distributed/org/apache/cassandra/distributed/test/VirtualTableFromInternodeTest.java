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
import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.locator.InetAddressAndPort;

import static org.apache.cassandra.distributed.util.QueryResultUtil.assertThat;
import static org.assertj.core.api.Assertions.assertThat;

public class VirtualTableFromInternodeTest extends TestBaseImpl
{
    private static Cluster CLUSTER;

    @BeforeClass
    public static void setup() throws IOException
    {
        CLUSTER = Cluster.build(2)
                         .withConfig(c -> c.with(Feature.values()))
                         .start();
    }

    @AfterClass
    public static void cleanup()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    @Test
    public void normal()
    {
        assertThat(CLUSTER.coordinator(1).executeWithResult("SELECT * FROM system_views.settings", ConsistencyLevel.ONE))
        .hasSizeGreaterThan(2)
        .contains("rpc_address", "127.0.0.1")
        .contains("broadcast_address", "127.0.0.1");

        assertThat(CLUSTER.coordinator(1).executeWithResult("SELECT * FROM system_views.settings WHERE name=?", ConsistencyLevel.ONE, "rpc_address"))
        .isEqualTo("rpc_address", "127.0.0.1");

        assertThat(CLUSTER.coordinator(1).executeWithResult("SELECT * FROM system_views.settings WHERE name IN (?, ?)", ConsistencyLevel.ONE, "rpc_address", "broadcast_address"))
        .contains("rpc_address", "127.0.0.1")
        .contains("broadcast_address", "127.0.0.1")
        .hasSize(2);
    }

    @Test
    public void readCommandAccessVirtualTable()
    {
        CLUSTER.get(1).runOnInstance(() -> {
            boolean didWork = false;
            for (InetAddressAndPort address : Gossiper.instance.getLiveMembers())
            {
                didWork = true;
                UntypedResultSet rs = QueryProcessor.executeAsync(address, "SELECT * FROM system_views.settings")
                                                    .syncUninterruptibly().getNow();
                assertThat(rs.isEmpty()).isFalse();
                for (UntypedResultSet.Row row : rs)
                {
                    String name = row.getString("name");
                    switch (name)
                    {
                        case "broadcast_address":
                        case "rpc_address":
                            assertThat(row.getString("value")).isEqualTo(address.getAddress().getHostAddress());
                            break;
                    }
                }
            }
            assertThat(didWork).isTrue();
        });
    }

    @Test
    public void readCommandAccessVirtualTableSinglePartition()
    {
        CLUSTER.get(1).runOnInstance(() -> {
            boolean didWork = false;
            for (InetAddressAndPort address : Gossiper.instance.getLiveMembers())
            {
                didWork = true;
                UntypedResultSet rs = QueryProcessor.executeAsync(address, "SELECT * FROM system_views.settings WHERE name=?", "rpc_address")
                                                    .syncUninterruptibly().getNow();
                assertThat(rs.isEmpty()).isFalse();
                assertThat(rs.one().getString("value")).isEqualTo(address.getAddress().getHostAddress());
            }
            assertThat(didWork).isTrue();
        });
    }

    @Test
    public void readCommandAccessVirtualTableMultiplePartition()
    {
        CLUSTER.get(1).runOnInstance(() -> {
            boolean didWork = false;
            for (InetAddressAndPort address : Gossiper.instance.getLiveMembers())
            {
                didWork = true;
                UntypedResultSet rs = QueryProcessor.executeAsync(address, "SELECT * FROM system_views.settings WHERE name IN (?, ?)", "rpc_address", "broadcast_address")
                                                    .syncUninterruptibly().getNow();
                assertThat(rs.isEmpty()).isFalse();
                Set<String> columns = new HashSet<>();
                for (UntypedResultSet.Row row : rs)
                {
                    columns.add(row.getString("name"));
                    assertThat(row.getString("value")).isEqualTo(address.getAddress().getHostAddress());
                }
                assertThat(columns).isEqualTo(ImmutableSet.of("rpc_address", "broadcast_address"));
            }
            assertThat(didWork).isTrue();
        });
    }
}
