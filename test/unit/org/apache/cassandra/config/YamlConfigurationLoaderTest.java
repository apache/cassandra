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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.io.util.File;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;


public class YamlConfigurationLoaderTest
{
    @Test
    public void readThresholdsFromConfig()
    {
        Config c = load("test/conf/cassandra.yaml");

        assertThat(c.read_thresholds_enabled).isTrue();

        assertThat(c.coordinator_read_size_warn_threshold).isEqualTo(DataStorageSpec.inKibibytes(1 << 10));
        assertThat(c.coordinator_read_size_fail_threshold).isEqualTo(DataStorageSpec.inKibibytes(1 << 12));

        assertThat(c.local_read_size_warn_threshold).isEqualTo(DataStorageSpec.inKibibytes(1 << 12));
        assertThat(c.local_read_size_fail_threshold).isEqualTo(DataStorageSpec.inKibibytes(1 << 13));

        assertThat(c.row_index_size_warn_threshold).isEqualTo(DataStorageSpec.inKibibytes(1 << 12));
        assertThat(c.row_index_size_fail_threshold).isEqualTo(DataStorageSpec.inKibibytes(1 << 13));
    }

    @Test
    public void readThresholdsFromMap()
    {

        Map<String, Object> map = ImmutableMap.of(
        "read_thresholds_enabled", true,
        "coordinator_read_size_warn_threshold", "1024KiB",
        "local_read_size_fail_threshold", "1024KiB",
        "row_index_size_warn_threshold", "1024KiB",
        "row_index_size_fail_threshold", "1024KiB"
        );

        Config c = YamlConfigurationLoader.fromMap(map, Config.class);
        assertThat(c.read_thresholds_enabled).isTrue();

        assertThat(c.coordinator_read_size_warn_threshold).isEqualTo(DataStorageSpec.inKibibytes(1024));
        assertThat(c.coordinator_read_size_fail_threshold).isNull();

        assertThat(c.local_read_size_warn_threshold).isNull();
        assertThat(c.local_read_size_fail_threshold).isEqualTo(DataStorageSpec.inKibibytes(1024));

        assertThat(c.row_index_size_warn_threshold).isEqualTo(DataStorageSpec.inKibibytes(1024));
        assertThat(c.row_index_size_fail_threshold).isEqualTo(DataStorageSpec.inKibibytes(1024));
    }

    @Test
    public void fromMapTest()
    {
        int storagePort = 123;
        Config.CommitLogSync commitLogSync = Config.CommitLogSync.batch;
        ParameterizedClass seedProvider = new ParameterizedClass("org.apache.cassandra.locator.SimpleSeedProvider", Collections.emptyMap());
        Map<String,Object> encryptionOptions = ImmutableMap.of("cipher_suites", Collections.singletonList("FakeCipher"),
                                                               "optional", false,
                                                               "enabled", true);
        Map<String,Object> map = new ImmutableMap.Builder<String, Object>()
                                 .put("storage_port", storagePort)
                                 .put("commitlog_sync", commitLogSync)
                                 .put("seed_provider", seedProvider)
                                 .put("client_encryption_options", encryptionOptions)
                                 .put("internode_socket_send_buffer_size", "5B")
                                 .put("internode_socket_receive_buffer_size", "5B")
                                 .build();

        Config config = YamlConfigurationLoader.fromMap(map, Config.class);
        assertEquals(storagePort, config.storage_port); // Check a simple integer
        assertEquals(commitLogSync, config.commitlog_sync); // Check an enum
        assertEquals(seedProvider, config.seed_provider); // Check a parameterized class
        assertEquals(false, config.client_encryption_options.optional); // Check a nested object
        assertEquals(true, config.client_encryption_options.enabled); // Check a nested object
        assertEquals(new DataStorageSpec("5B"), config.internode_socket_send_buffer_size); // Check names backward compatibility (CASSANDRA-17141 and CASSANDRA-15234)
        assertEquals(new DataStorageSpec("5B"), config.internode_socket_receive_buffer_size); // Check names backward compatibility (CASSANDRA-17141 and CASSANDRA-15234)
    }

    @Test
    public void sharedErrorReportingExclusions()
    {
        Config config = load("data/config/YamlConfigurationLoaderTest/shared_client_error_reporting_exclusions.yaml");
        SubnetGroups expected = new SubnetGroups(Arrays.asList("127.0.0.1", "127.0.0.0/31"));
        assertThat(config.client_error_reporting_exclusions).isEqualTo(expected);
        assertThat(config.internode_error_reporting_exclusions).isEqualTo(expected);
    }

    private static Config load(String path)
    {
        URL url = YamlConfigurationLoaderTest.class.getClassLoader().getResource(path);
        if (url == null)
        {
            try
            {
                url = new File(path).toPath().toUri().toURL();
            }
            catch (MalformedURLException e)
            {
                throw new AssertionError(e);
            }
        }
        return new YamlConfigurationLoader().loadConfig(url);
    }
}
