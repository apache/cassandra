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

package org.apache.cassandra.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

public class UnitConfigOverride
{
    private enum ConfigType
    {
        CDC("cassandra.yaml", "cdc.yaml"),
        COMPRESSION("cassandra.yaml", "commitlog_compression_LZ4.yaml"), // not 100% correct, LZ4 could be changed at build time
        OA("cassandra.yaml", "storage_compatibility_mode_none.yaml"),
        SYSTEM_KEYSPACE_DIRECTORY("cassandra.yaml", "system_keyspaces_directory.yaml"),
        TRIE("cassandra.yaml", "trie_memtable.yaml", "storage_compatibility_mode_none.yaml");

        public final List<String> configs;

        ConfigType(String... configs)
        {
            this.configs = Arrays.asList(configs);
        }
    }

    @Nullable
    private static final ConfigType CONFIG_TYPE = null;

    @SuppressWarnings("ConstantConditions")
    public static void maybeOverrideConfig()
    {
        // CONFIG_TYPE is meant to be changed by users while debugging, so this condition won't be false on every machine
        if (CONFIG_TYPE != null)
            setConfigType(CONFIG_TYPE);
    }

    @SuppressWarnings("SameParameterValue")
    private static void setConfigType(ConfigType type)
    {
        File confDir = new File("test/conf");
        try
        {
            File tmp = Files.createTempFile("cassandra-conf", ".yaml").toFile();
            tmp.deleteOnExit();
            for (String name : type.configs)
            {
                try (FileOutputStream out = new FileOutputStream(tmp, true);
                     InputStream in = new FileInputStream(new File(confDir, name)))
                {
                    in.transferTo(out);
                }
            }
            CassandraRelevantProperties.CASSANDRA_CONFIG.setString(tmp.toURI().toString());
        } catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }
}
