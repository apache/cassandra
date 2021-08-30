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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;


public class YamlConfigurationLoaderTest
{
    @Test
    public void trackWarningsFromConfig()
    {
        // this test just makes sure snakeyaml loads the test config properly and populates the fields (track warnings uses final in some places)
        // if the config is changed, its ok to update this test to reflect that change
        TrackWarnings tw = load("test/conf/cassandra.yaml").track_warnings;
        assertThat(tw.enabled).isTrue();

        assertThat(tw.coordinator_read_size.warn_threshold_kb).isGreaterThan(0);
        assertThat(tw.coordinator_read_size.abort_threshold_kb).isGreaterThan(0);

        assertThat(tw.local_read_size.warn_threshold_kb).isGreaterThan(0);
        assertThat(tw.local_read_size.abort_threshold_kb).isGreaterThan(0);

        assertThat(tw.row_index_size.warn_threshold_kb).isGreaterThan(0);
        assertThat(tw.row_index_size.abort_threshold_kb).isGreaterThan(0);
    }

    @Test
    public void trackWarningsFromMap()
    {
        Map<String, Object> map = ImmutableMap.of("track_warnings", ImmutableMap.of(
        "enabled", true,
        "coordinator_read_size", ImmutableMap.of("warn_threshold_kb", 1024),
        "local_read_size", ImmutableMap.of("abort_threshold_kb", 1024),
        "row_index_size", ImmutableMap.of("warn_threshold_kb", 1024, "abort_threshold_kb", 1024)
        ));

        Config config = YamlConfigurationLoader.fromMap(map, Config.class);
        TrackWarnings tw = config.track_warnings;
        assertThat(tw.enabled).isTrue();

        assertThat(tw.coordinator_read_size.warn_threshold_kb).isEqualTo(1024);
        assertThat(tw.coordinator_read_size.abort_threshold_kb).isEqualTo(0);

        assertThat(tw.local_read_size.warn_threshold_kb).isEqualTo(0);
        assertThat(tw.local_read_size.abort_threshold_kb).isEqualTo(1024);

        assertThat(tw.row_index_size.warn_threshold_kb).isEqualTo(1024);
        assertThat(tw.row_index_size.abort_threshold_kb).isEqualTo(1024);
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
        Map<String,Object> map = ImmutableMap.of("storage_port", storagePort,
                                                 "commitlog_sync", commitLogSync,
                                                 "seed_provider", seedProvider,
                                                 "client_encryption_options", encryptionOptions);

        Config config = YamlConfigurationLoader.fromMap(map, Config.class);
        assertEquals(storagePort, config.storage_port); // Check a simple integer
        assertEquals(commitLogSync, config.commitlog_sync); // Check an enum
        assertEquals(seedProvider, config.seed_provider); // Check a parameterized class
        assertEquals(false, config.client_encryption_options.optional); // Check a nested object
        assertEquals(true, config.client_encryption_options.enabled); // Check a nested object
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
                url = new File(path).toURI().toURL();
            }
            catch (MalformedURLException e)
            {
                throw new AssertionError(e);
            }
        }
        return new YamlConfigurationLoader().loadConfig(url);
    }
}
