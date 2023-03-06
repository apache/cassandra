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

package org.apache.cassandra.distributed.fuzz;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.locator.EndpointsForToken;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.tcm.ClusterMetadata;

public class InJvmSut extends InJvmSutBase<IInvokableInstance, Cluster>
{
    public static void init()
    {
        Configuration.registerSubtypes(InJvmSutConfiguration.class);
    }

    private static final Logger logger = LoggerFactory.getLogger(InJvmSut.class);

    public InJvmSut(Cluster cluster)
    {
        super(cluster, 10);
    }

    @JsonTypeName("in_jvm")
    public static class InJvmSutConfiguration extends InJvmSutBaseConfiguration<IInvokableInstance, Cluster>
    {
        @JsonCreator
        public InJvmSutConfiguration(@JsonProperty(value = "nodes", defaultValue = "3") int nodes,
                                     @JsonProperty(value = "worker_threads", defaultValue = "10") int worker_threads,
                                     @JsonProperty("root") String root)
        {
            super(nodes, worker_threads, root);
        }

        protected Cluster cluster(Consumer<IInstanceConfig> cfg, int nodes, File root)
        {
            try
            {
                return Cluster.build().withConfig(cfg)
                               .withNodes(nodes)
                               .withRoot(root)
                              .createWithoutStarting();
            }
            catch (IOException e)
            {
                throw new IllegalStateException(e);
            }
        }

        protected InJvmSutBase<IInvokableInstance, Cluster> sut(Cluster cluster)
        {
            return new InJvmSut(cluster);
        }
    }

    // TODO: this would only return _read_ (or natural) replicas for the token
    public int[] getReadReplicasFor(Object[] partitionKey, String keyspace, String table)
    {
        return cluster.get(1).appliesOnInstance(InJvmSut::getReadReplicasForCallable).apply(partitionKey, keyspace, table);
    }

    public static int[] getReadReplicasForCallable(Object[] pk, String ks, String table)
    {
        String pkString = Arrays.stream(pk).map(Object::toString).collect(Collectors.joining(":"));
        EndpointsForToken endpoints = StorageService.instance.getNaturalReplicasForToken(ks, table, pkString);
        int[] nodes = new int[endpoints.size()];
        for (int i = 0; i < endpoints.size(); i++)
            nodes[i] = endpoints.get(i).endpoint().getAddress().getAddress()[3];

        sanity_check:
        {
            Keyspace ksp = Keyspace.open(ks);
            Token token = DatabaseDescriptor.getPartitioner().getToken(ksp.getMetadata().getTableOrViewNullable(table).partitionKeyType.fromString(pkString));

            ClusterMetadata metadata = ClusterMetadata.current();
            EndpointsForToken replicas = metadata.placements.get(ksp.getMetadata().params.replication).reads.forToken(token);

            assert replicas.endpoints().equals(endpoints.endpoints()) : String.format("Consistent metadata endpoints %s disagree with token metadata computation %s", endpoints.endpoints(), replicas.endpoints());
        }
        return nodes;
    }
}