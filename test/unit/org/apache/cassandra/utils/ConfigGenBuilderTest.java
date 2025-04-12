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

package org.apache.cassandra.utils;

import java.util.Map;

import com.google.common.jimfs.Jimfs;
import org.junit.Test;

import accord.utils.Gen;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.config.YamlConfigurationLoader;
import org.apache.cassandra.io.util.File;
import org.apache.cassandra.locator.SimpleSeedProvider;

import static accord.utils.Property.qt;

public class ConfigGenBuilderTest
{
    static
    {
        File.unsafeSetFilesystem(Jimfs.newFileSystem("testing"));
    }

    private static final Gen<Map<String, Object>> GEN = new ConfigGenBuilder().build();

    @Test
    public void validConfigs()
    {
        qt().forAll(GEN).check(config -> validate(config, defaultConfig()));
    }

    @Test
    public void validConfigsWithDefaults()
    {
        qt().forAll(GEN).check(config -> validate(config, simpleConfig()));
    }

    private static void validate(Map<String, Object> config, Config c)
    {
        YamlConfigurationLoader.updateFromMap(config, true, c);
        DatabaseDescriptor.unsafeDaemonInitialization(() -> ConfigGenBuilder.sanitize(c));
    }

    private static Config defaultConfig()
    {
        return ConfigGenBuilder.prepare(DatabaseDescriptor.loadConfig());
    }

    private static Config simpleConfig()
    {
        Config c = new Config();
        c.commitlog_directory = "/commitlog";
        c.hints_directory = "/hints";
        c.saved_caches_directory = "/saved_caches_directory";
        c.data_file_directories = new String[]{"/data_file_directories"};
        c.endpoint_snitch = "SimpleSnitch";
        c.seed_provider = new ParameterizedClass(SimpleSeedProvider.class.getName());
        return c;
    }
}