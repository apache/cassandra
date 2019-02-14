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
import java.nio.file.Files;
import java.util.List;

import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.impl.IUpgradeableInstance;
import org.apache.cassandra.distributed.impl.InstanceConfig;
import org.apache.cassandra.distributed.impl.Versions;

/**
 * A multi-version cluster, offering only the cross-version API
 *
 * TODO: we could perhaps offer some convenience methods for nodes we know to be of the 'current' version,
 * to permit upgrade tests to perform cluster operations without updating the cross-version API,
 * so long as one node is up-to-date.
 */
public class UpgradeableCluster extends AbstractCluster<IUpgradeableInstance> implements ICluster, AutoCloseable
{
    private UpgradeableCluster(File root, Versions.Version version, List<InstanceConfig> configs, ClassLoader sharedClassLoader)
    {
        super(root, version, configs, sharedClassLoader);
    }

    protected IUpgradeableInstance newInstanceWrapper(Versions.Version version, InstanceConfig config)
    {
        return new Wrapper(version, config);
    }

    public static UpgradeableCluster create(int nodeCount) throws Throwable
    {
        return create(nodeCount, UpgradeableCluster::new);
    }
    public static UpgradeableCluster create(int nodeCount, File root)
    {
        return create(nodeCount, Versions.CURRENT, root, UpgradeableCluster::new);
    }

    public static UpgradeableCluster create(int nodeCount, Versions.Version version) throws IOException
    {
        return create(nodeCount, version, Files.createTempDirectory("dtests").toFile(), UpgradeableCluster::new);
    }
    public static UpgradeableCluster create(int nodeCount, Versions.Version version, File root)
    {
        return create(nodeCount, version, root, UpgradeableCluster::new);
    }

}

