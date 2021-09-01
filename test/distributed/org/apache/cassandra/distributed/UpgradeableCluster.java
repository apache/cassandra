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

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IUpgradeableInstance;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.Versions;

/**
 * A multi-version cluster, offering only the cross-version API
 *
 * TODO: we could perhaps offer some convenience methods for nodes we know to be of the 'current' version,
 * to permit upgrade tests to perform cluster operations without updating the cross-version API,
 * so long as one node is up-to-date.
 */
public class UpgradeableCluster extends AbstractCluster<IUpgradeableInstance> implements AutoCloseable
{
    private UpgradeableCluster(Builder builder)
    {
        super(builder);
    }

    protected IUpgradeableInstance newInstanceWrapper(Versions.Version version, IInstanceConfig config)
    {
        config.set(Constants.KEY_DTEST_API_CONFIG_CHECK, false);
        return new Wrapper(version, config);
    }

    public static Builder build()
    {
        return new Builder();
    }

    public static Builder build(int nodeCount)
    {
        return build().withNodes(nodeCount);
    }

    public static UpgradeableCluster create(int nodeCount) throws Throwable
    {
        return build(nodeCount).start();
    }
    public static UpgradeableCluster create(int nodeCount, Versions.Version version, Consumer<IInstanceConfig> configUpdater) throws IOException
    {
        return create(nodeCount, version, configUpdater, null);
    }

    public static UpgradeableCluster create(int nodeCount, Versions.Version version, Consumer<IInstanceConfig> configUpdater, Consumer<Builder> builderUpdater) throws IOException
    {
        Builder builder = build(nodeCount).withConfig(configUpdater).withVersion(version);
        if (builderUpdater != null)
            builderUpdater.accept(builder);
        return builder.start();
    }

    public static UpgradeableCluster create(int nodeCount, Versions.Version version) throws Throwable
    {
        return build(nodeCount).withVersion(version).start();
    }

    public static final class Builder extends AbstractBuilder<IUpgradeableInstance, UpgradeableCluster, Builder>
    {

        public Builder()
        {
            super(UpgradeableCluster::new);
        }
    }
}

