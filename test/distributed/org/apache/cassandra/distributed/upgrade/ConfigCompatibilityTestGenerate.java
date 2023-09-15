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
package org.apache.cassandra.distributed.upgrade;

import java.io.File;
import java.util.Arrays;

import org.apache.commons.lang3.ArrayUtils;

import com.vdurmont.semver4j.Semver;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.distributed.UpgradeableCluster;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.impl.AbstractCluster;
import org.apache.cassandra.distributed.shared.Versions;

import static org.apache.cassandra.config.ConfigCompatibilityTest.TEST_DIR;
import static org.apache.cassandra.config.ConfigCompatibilityTest.dump;
import static org.apache.cassandra.config.ConfigCompatibilityTest.toTree;
import static org.apache.cassandra.distributed.upgrade.UpgradeTestBase.v30;
import static org.apache.cassandra.distributed.upgrade.UpgradeTestBase.v3X;
import static org.apache.cassandra.distributed.upgrade.UpgradeTestBase.v40;
import static org.apache.cassandra.distributed.upgrade.UpgradeTestBase.v41;
import static org.apache.cassandra.distributed.upgrade.UpgradeTestBase.v50;

/**
 * This class is to generate YAML dumps per version, this is a manual process and should be updated for each release.
 */
public class ConfigCompatibilityTestGenerate
{
    public static void main(String[] args) throws Throwable
    {
        ICluster.setup();
        Versions versions = Versions.find();
        for (Semver version : Arrays.asList(v30, v3X, v40, v41, v50))
        {
            File path = new File(TEST_DIR, "version=" + version + ".yml");
            path.getParentFile().mkdirs();
            Versions.Version latest = versions.getLatest(version);
            // this class isn't present so the lambda can't be deserialized... so add to the classpath
            latest = new Versions.Version(latest.version, ArrayUtils.addAll(latest.classpath, AbstractCluster.CURRENT_VERSION.classpath));

            try (UpgradeableCluster cluster = UpgradeableCluster.create(1, latest))
            {
                IInvokableInstance inst = (IInvokableInstance) cluster.get(1);
                Class<?> klass = inst.callOnInstance(() -> Config.class);
                assert klass.getClassLoader() != ConfigCompatibilityTestGenerate.class.getClassLoader();
                dump(toTree(klass), path.getAbsolutePath());
            }
        }
    }
}
