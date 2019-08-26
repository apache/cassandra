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

package org.apache.cassandra.distributed;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.IInvokableInstance;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.impl.Versions;

/**
 * A simple cluster supporting only the 'current' Cassandra version, offering easy access to the convenience methods
 * of IInvokableInstance on each node.
 */
public class Cluster extends AbstractCluster<IInvokableInstance> implements ICluster, AutoCloseable
{
    private Cluster(File root, Versions.Version version, List<InstanceConfig> configs, ClassLoader sharedClassLoader)
    {
        super(root, version, configs, sharedClassLoader);
    }

    protected IInvokableInstance newInstanceWrapper(int generation, Versions.Version version, InstanceConfig config)
    {
        return new Wrapper(generation, version, config);
    }

    public static Builder<IInvokableInstance, Cluster> build()
    {
        return new Builder<>(Cluster::new);
    }

    public static Builder<IInvokableInstance, Cluster> build(int nodeCount)
    {
        return build().withNodes(nodeCount);
    }

    public static Cluster create(int nodeCount, Consumer<InstanceConfig> configUpdater) throws IOException
    {
        return build(nodeCount).withConfig(configUpdater).start();
    }

    public static Cluster create(int nodeCount) throws Throwable
    {
        return build(nodeCount).start();
    }
}

