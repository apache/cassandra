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

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.datastax.driver.core.ProtocolVersion;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;

public final class JavaDriverUtils
{
    private JavaDriverUtils()
    {
    }

    public static com.datastax.driver.core.Cluster create(ICluster<? extends IInstance> dtest)
    {
        return create(dtest, null, null);
    }

    public static com.datastax.driver.core.Cluster create(ICluster<? extends IInstance> dtest, ProtocolVersion version)
    {
        return create(dtest, version, null);
    }

    public static com.datastax.driver.core.Cluster create(ICluster<? extends IInstance> dtest, ProtocolVersion version,
                                                          Consumer<com.datastax.driver.core.Cluster.Builder> overrideBuilder)
    {
        if (dtest.size() == 0)
            throw new IllegalArgumentException("Attempted to open java driver for empty cluster");

        // make sure the needed Features are added
        dtest.stream().forEach(i -> {
            if (!(i.config().has(Feature.NATIVE_PROTOCOL) && i.config().has(Feature.GOSSIP))) // gossip is needed as currently Host.getHostId is empty without it
                throw new IllegalStateException("java driver requires Feature.NATIVE_PROTOCOL and Feature.GOSSIP; but one or more is missing");
        });

        com.datastax.driver.core.Cluster.Builder builder = com.datastax.driver.core.Cluster.builder();

        List<InetSocketAddress> contactPoints = buildContactPoints(dtest);
        builder.addContactPointsWithPorts(contactPoints);

        if (version != null)
            builder.withProtocolVersion(version);

        if (overrideBuilder != null)
            overrideBuilder.accept(builder);

        return builder.build();
    }

    public static List<InetSocketAddress> buildContactPoints(ICluster<? extends IInstance> dtest)
    {
        return dtest.stream()
                    .map(ClusterUtils::getNativeInetSocketAddress)
                    .collect(Collectors.toList());
    }
}
