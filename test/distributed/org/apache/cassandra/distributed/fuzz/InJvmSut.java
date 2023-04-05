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
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import harry.core.Configuration;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;

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

    public InJvmSut(Cluster cluster, int threads)
    {
        super(cluster, threads);
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
}