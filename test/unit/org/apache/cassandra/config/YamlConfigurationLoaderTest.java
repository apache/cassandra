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

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.io.util.File;
import org.yaml.snakeyaml.error.YAMLException;

import static org.apache.cassandra.config.CassandraRelevantProperties.CONFIG_ALLOW_SYSTEM_PROPERTIES;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.KIBIBYTES;
import static org.apache.cassandra.config.YamlConfigurationLoader.SYSTEM_PROPERTY_PREFIX;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


public class YamlConfigurationLoaderTest
{
    @Test
    public void repairRetryEmpty()
    {
        RepairRetrySpec repair_retries = loadRepairRetry(ImmutableMap.of());
        // repair is empty
        assertThat(repair_retries.isEnabled()).isFalse();
        assertThat(repair_retries.isMerkleTreeRetriesEnabled()).isFalse();
    }

    @Test
    public void repairRetryInheritance()
    {
        RepairRetrySpec repair_retries = loadRepairRetry(ImmutableMap.of("max_attempts", "3"));
        assertThat(repair_retries.isEnabled()).isTrue();
        assertThat(repair_retries.getMaxAttempts()).isEqualTo(3);
        RetrySpec spec = repair_retries.getMerkleTreeResponseSpec();
        assertThat(spec.isEnabled()).isTrue();
        assertThat(spec.getMaxAttempts()).isEqualTo(3);
    }

    @Test
    public void repairRetryOverride()
    {
        RepairRetrySpec repair_retries = loadRepairRetry(ImmutableMap.of(
        "merkle_tree_response", ImmutableMap.of("max_attempts", 10,
                                                "base_sleep_time", "1s",
                                                "max_sleep_time", "10s")
        ));
        assertThat(repair_retries.isEnabled()).isFalse();
        assertThat(repair_retries.getMaxAttempts()).isNull();
        assertThat(repair_retries.baseSleepTime).isEqualTo(RetrySpec.DEFAULT_BASE_SLEEP);
        assertThat(repair_retries.maxSleepTime).isEqualTo(RetrySpec.DEFAULT_MAX_SLEEP);

        RetrySpec spec = repair_retries.getMerkleTreeResponseSpec();
        assertThat(spec.isEnabled()).isTrue();
        assertThat(spec.maxAttempts).isEqualTo(10);
        assertThat(spec.baseSleepTime).isEqualTo(RetrySpec.DEFAULT_MAX_SLEEP);
        assertThat(spec.maxSleepTime).isEqualTo(new DurationSpec.LongMillisecondsBound("10s"));
    }

    private static RepairRetrySpec loadRepairRetry(Map<String, Object> map)
    {
        return YamlConfigurationLoader.fromMap(ImmutableMap.of("repair", ImmutableMap.of("retries", map)), true, Config.class).repair.retries;
    }

    @Test
    public void validateTypes()
    {
        Predicate<Field> isDurationSpec = f -> f.getType().getTypeName().equals("org.apache.cassandra.config.DurationSpec");
        Predicate<Field> isDataStorageSpec = f -> f.getType().getTypeName().equals("org.apache.cassandra.config.DataStorageSpec");
        Predicate<Field> isDataRateSpec = f -> f.getType().getTypeName().equals("org.apache.cassandra.config.DataRateSpec");

        assertEquals("You have wrongly defined a config parameter of abstract type DurationSpec, DataStorageSpec or DataRateSpec." +
                     "Please check the config docs, otherwise Cassandra won't be able to start with this parameter being set in cassandra.yaml.",
                     Arrays.stream(Config.class.getFields())
                    .filter(f -> !Modifier.isStatic(f.getModifiers()))
                    .filter(isDurationSpec.or(isDataRateSpec).or(isDataStorageSpec)).count(), 0);
    }

    @Test
    public void updateInPlace()
    {
        Config config = new Config();
        Map<String, Object> map = ImmutableMap.<String, Object>builder().put("storage_port", 123)
                                                                        .put("commitlog_sync", Config.CommitLogSync.batch)
                                                                        .put("seed_provider.class_name", "org.apache.cassandra.locator.SimpleSeedProvider")
                                                                        .put("client_encryption_options.cipher_suites", Collections.singletonList("FakeCipher"))
                                                                        .put("client_encryption_options.optional", false)
                                                                        .put("client_encryption_options.enabled", true)
                                                                        .build();
        Config updated = YamlConfigurationLoader.updateFromMap(map, true, config);
        assert updated == config : "Config pointers do not match";
        assertThat(config.storage_port).isEqualTo(123);
        assertThat(config.commitlog_sync).isEqualTo(Config.CommitLogSync.batch);
        assertThat(config.seed_provider.class_name).isEqualTo("org.apache.cassandra.locator.SimpleSeedProvider");
        assertThat(config.client_encryption_options.cipher_suites).isEqualTo(Collections.singletonList("FakeCipher"));
        assertThat(config.client_encryption_options.optional).isFalse();
        assertThat(config.client_encryption_options.enabled).isTrue();
    }

    @Test
    public void withSystemProperties()
    {
        // for primitive types or data-types which use a String constructor, we can support these as nested
        // if the type is a collection, then the string format doesn't make sense and will fail with an error such as
        //   Cannot create property=client_encryption_options.cipher_suites for JavaBean=org.apache.cassandra.config.Config@1f59a598
        //   No single argument constructor found for interface java.util.List : null
        // the reason is that its not a scalar but a complex type (collection type), so the map we use needs to have a collection to match.
        // It is possible that we define a common string representation for these types so they can be written to; this
        // is an issue that SettingsTable may need to worry about.
        try (WithProperties ignore = new WithProperties()
                                     .set(CONFIG_ALLOW_SYSTEM_PROPERTIES, true)
                                     .with(SYSTEM_PROPERTY_PREFIX + "storage_port", "123",
                                           SYSTEM_PROPERTY_PREFIX + "commitlog_sync", "batch",
                                           SYSTEM_PROPERTY_PREFIX + "seed_provider.class_name", "org.apache.cassandra.locator.SimpleSeedProvider",
                                           SYSTEM_PROPERTY_PREFIX + "client_encryption_options.optional", Boolean.FALSE.toString(),
                                           SYSTEM_PROPERTY_PREFIX + "client_encryption_options.enabled", Boolean.TRUE.toString(),
                                           SYSTEM_PROPERTY_PREFIX + "doesnotexist", Boolean.TRUE.toString()))
        {
            Config config = YamlConfigurationLoader.fromMap(Collections.emptyMap(), true, Config.class);
            assertThat(config.storage_port).isEqualTo(123);
            assertThat(config.commitlog_sync).isEqualTo(Config.CommitLogSync.batch);
            assertThat(config.seed_provider.class_name).isEqualTo("org.apache.cassandra.locator.SimpleSeedProvider");
            assertThat(config.client_encryption_options.optional).isFalse();
            assertThat(config.client_encryption_options.enabled).isTrue();
        }
    }

    @Test
    public void readConvertersSpecialCasesFromConfig()
    {
        Config c = load("test/conf/cassandra-converters-special-cases.yaml");
        assertThat(c.sstable_preemptive_open_interval).isNull();
        assertThat(c.index_summary_resize_interval).isNull();
        assertThat(c.cache_load_timeout).isEqualTo(new DurationSpec.IntSecondsBound("0s"));

        c = load("test/conf/cassandra-converters-special-cases-old-names.yaml");
        assertThat(c.sstable_preemptive_open_interval).isNull();
        assertThat(c.index_summary_resize_interval).isNull();
        assertThat(c.cache_load_timeout).isEqualTo(new DurationSpec.IntSecondsBound("0s"));
    }

    @Test
    public void readConvertersSpecialCasesFromMap()
    {
        Map<String, Object> map = new HashMap<>();
        map.put("sstable_preemptive_open_interval", null);
        map.put("index_summary_resize_interval", null);
        map.put("credentials_update_interval", null);

        Config c = YamlConfigurationLoader.fromMap(map, true, Config.class);
        assertThat(c.sstable_preemptive_open_interval).isNull();
        assertThat(c.index_summary_resize_interval).isNull();
        assertThat(c.credentials_update_interval).isNull();

        map = ImmutableMap.of(
        "sstable_preemptive_open_interval_in_mb", "-1",
        "index_summary_resize_interval_in_minutes", "-1",
        "cache_load_timeout_seconds", "-1",
        "credentials_update_interval_in_ms", "-1"
        );
        c = YamlConfigurationLoader.fromMap(map, Config.class);

        assertThat(c.sstable_preemptive_open_interval).isNull();
        assertThat(c.index_summary_resize_interval).isNull();
        assertThat(c.cache_load_timeout).isEqualTo(new DurationSpec.IntSecondsBound("0s"));
        assertThat(c.credentials_update_interval).isNull();
    }

    @Test
    public void readThresholdsFromConfig()
    {
        Config c = load("test/conf/cassandra.yaml");

        assertThat(c.read_thresholds_enabled).isTrue();

        assertThat(c.coordinator_read_size_warn_threshold).isEqualTo(new DataStorageSpec.LongBytesBound(1 << 10, KIBIBYTES));
        assertThat(c.coordinator_read_size_fail_threshold).isEqualTo(new DataStorageSpec.LongBytesBound(1 << 12, KIBIBYTES));

        assertThat(c.local_read_size_warn_threshold).isEqualTo(new DataStorageSpec.LongBytesBound(1 << 12, KIBIBYTES));
        assertThat(c.local_read_size_fail_threshold).isEqualTo(new DataStorageSpec.LongBytesBound(1 << 13, KIBIBYTES));

        assertThat(c.row_index_read_size_warn_threshold).isEqualTo(new DataStorageSpec.LongBytesBound(1 << 12, KIBIBYTES));
        assertThat(c.row_index_read_size_fail_threshold).isEqualTo(new DataStorageSpec.LongBytesBound(1 << 13, KIBIBYTES));
    }

    @Test
    public void readThresholdsFromMap()
    {

        Map<String, Object> map = ImmutableMap.of(
        "read_thresholds_enabled", true,
        "coordinator_read_size_warn_threshold", "1024KiB",
        "local_read_size_fail_threshold", "1024KiB",
        "row_index_read_size_warn_threshold", "1024KiB",
        "row_index_read_size_fail_threshold", "1024KiB"
        );

        Config c = YamlConfigurationLoader.fromMap(map, Config.class);
        assertThat(c.read_thresholds_enabled).isTrue();

        assertThat(c.coordinator_read_size_warn_threshold).isEqualTo(new DataStorageSpec.LongBytesBound(1024, KIBIBYTES));
        assertThat(c.coordinator_read_size_fail_threshold).isNull();

        assertThat(c.local_read_size_warn_threshold).isNull();
        assertThat(c.local_read_size_fail_threshold).isEqualTo(new DataStorageSpec.LongBytesBound(1024, KIBIBYTES));

        assertThat(c.row_index_read_size_warn_threshold).isEqualTo(new DataStorageSpec.LongBytesBound(1024, KIBIBYTES));
        assertThat(c.row_index_read_size_fail_threshold).isEqualTo(new DataStorageSpec.LongBytesBound(1024, KIBIBYTES));
    }

    @Test
    public void notNullableLegacyProperties()
    {
        // In  the past commitlog_sync_period and commitlog_sync_group_window were int in Config. So that meant they can't
        // be assigned null value from the yaml file. To ensure this behavior was not changed when we moved to DurationSpec
        // in CASSANDRA-15234, we assigned those 0 value.

        Map<String, Object> map = ImmutableMap.of(
        "commitlog_sync_period", ""
        );
        try
        {
            Config config = YamlConfigurationLoader.fromMap(map, Config.class);
        }
        catch (YAMLException e)
        {
            assertTrue(e.getMessage().contains("Cannot create property=commitlog_sync_period for JavaBean=org.apache.cassandra.config.Config"));
        }

        // loadConfig will catch this exception on startup and throw a ConfigurationException
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
                                 .put("commitlog_sync_group_window_in_ms", "42")
                                 .build();

        Config config = YamlConfigurationLoader.fromMap(map, Config.class);
        assertEquals(storagePort, config.storage_port); // Check a simple integer
        assertEquals(commitLogSync, config.commitlog_sync); // Check an enum
        assertEquals(seedProvider, config.seed_provider); // Check a parameterized class
        assertEquals(false, config.client_encryption_options.optional); // Check a nested object
        assertEquals(true, config.client_encryption_options.enabled); // Check a nested object
        assertEquals(new DataStorageSpec.IntBytesBound("5B"), config.internode_socket_send_buffer_size); // Check names backward compatibility (CASSANDRA-17141 and CASSANDRA-15234)
        assertEquals(new DataStorageSpec.IntBytesBound("5B"), config.internode_socket_receive_buffer_size); // Check names backward compatibility (CASSANDRA-17141 and CASSANDRA-15234)
    }

    @Test
    public void typeChange()
    {
        Config old = YamlConfigurationLoader.fromMap(ImmutableMap.of("key_cache_save_period", 42,
                                                                     "row_cache_save_period", 42,
                                                                     "counter_cache_save_period", 42), Config.class);
        Config latest = YamlConfigurationLoader.fromMap(ImmutableMap.of("key_cache_save_period", "42s",
                                                                        "row_cache_save_period", "42s",
                                                                        "counter_cache_save_period", "42s"), Config.class);
        assertThat(old.key_cache_save_period).isEqualTo(latest.key_cache_save_period).isEqualTo(new DurationSpec.IntSecondsBound(42));
        assertThat(old.row_cache_save_period).isEqualTo(latest.row_cache_save_period).isEqualTo(new DurationSpec.IntSecondsBound(42));
        assertThat(old.counter_cache_save_period).isEqualTo(latest.counter_cache_save_period).isEqualTo(new DurationSpec.IntSecondsBound(42));
    }

    @Test
    public void sharedErrorReportingExclusions()
    {
        Config config = load("data/config/YamlConfigurationLoaderTest/shared_client_error_reporting_exclusions.yaml");
        SubnetGroups expected = new SubnetGroups(Arrays.asList("127.0.0.1", "127.0.0.0/31"));
        assertThat(config.client_error_reporting_exclusions).isEqualTo(expected);
        assertThat(config.internode_error_reporting_exclusions).isEqualTo(expected);
    }

    @Test
    public void converters()
    {
        // MILLIS_DURATION
        assertThat(from("permissions_validity_in_ms", "42").permissions_validity.toMilliseconds()).isEqualTo(42);
        assertThatThrownBy(() -> from("permissions_validity", -2).permissions_validity.toMilliseconds())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid duration: -2 Accepted units:[MILLISECONDS, SECONDS, MINUTES, HOURS, DAYS] where case matters and only non-negative values.");

        // MILLIS_DOUBLE_DURATION
        assertThat(from("commitlog_sync_group_window_in_ms", "42").commitlog_sync_group_window.toMilliseconds()).isEqualTo(42);
        assertThat(from("commitlog_sync_group_window_in_ms", "0.2").commitlog_sync_group_window.toMilliseconds()).isEqualTo(0);
        assertThat(from("commitlog_sync_group_window_in_ms", "42.5").commitlog_sync_group_window.toMilliseconds()).isEqualTo(43);
        assertThat(from("commitlog_sync_group_window_in_ms", "NaN").commitlog_sync_group_window.toMilliseconds()).isEqualTo(0);
        assertThatThrownBy(() -> from("commitlog_sync_group_window_in_ms", -2).commitlog_sync_group_window.toMilliseconds())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid duration: value must be non-negative");

        // MILLIS_CUSTOM_DURATION
        assertThat(from("permissions_update_interval_in_ms", 42).permissions_update_interval).isEqualTo(new DurationSpec.IntMillisecondsBound(42));
        assertThat(from("permissions_update_interval_in_ms", -1).permissions_update_interval).isNull();
        assertThatThrownBy(() -> from("permissions_update_interval_in_ms", -2))
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid duration: value must be non-negative");

        // SECONDS_DURATION
        assertThat(from("streaming_keep_alive_period_in_secs", "42").streaming_keep_alive_period.toSeconds()).isEqualTo(42);
        assertThatThrownBy(() -> from("streaming_keep_alive_period_in_secs", -2).streaming_keep_alive_period.toSeconds())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid duration: value must be non-negative");

        // NEGATIVE_SECONDS_DURATION
        assertThat(from("validation_preview_purge_head_start_in_sec", -1).validation_preview_purge_head_start.toSeconds()).isEqualTo(0);
        assertThat(from("validation_preview_purge_head_start_in_sec", 0).validation_preview_purge_head_start.toSeconds()).isEqualTo(0);
        assertThat(from("validation_preview_purge_head_start_in_sec", 42).validation_preview_purge_head_start.toSeconds()).isEqualTo(42);

        // SECONDS_CUSTOM_DURATION already tested in type change

        // MINUTES_CUSTOM_DURATION
        assertThat(from("index_summary_resize_interval_in_minutes", "42").index_summary_resize_interval.toMinutes()).isEqualTo(42);
        assertThat(from("index_summary_resize_interval_in_minutes", "-1").index_summary_resize_interval).isNull();
        assertThatThrownBy(() -> from("index_summary_resize_interval_in_minutes", -2).index_summary_resize_interval.toMinutes())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid duration: value must be non-negative");

        // BYTES_CUSTOM_DATASTORAGE
        assertThat(from("native_transport_max_concurrent_requests_in_bytes_per_ip", -1).native_transport_max_request_data_in_flight_per_ip).isEqualTo(null);
        assertThat(from("native_transport_max_concurrent_requests_in_bytes_per_ip", 0).native_transport_max_request_data_in_flight_per_ip.toBytes()).isEqualTo(0);
        assertThat(from("native_transport_max_concurrent_requests_in_bytes_per_ip", 42).native_transport_max_request_data_in_flight_per_ip.toBytes()).isEqualTo(42);

        // MEBIBYTES_DATA_STORAGE
        assertThat(from("memtable_heap_space_in_mb", "42").memtable_heap_space.toMebibytes()).isEqualTo(42);
        assertThatThrownBy(() -> from("memtable_heap_space_in_mb", -2).memtable_heap_space.toMebibytes())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid data storage: value must be non-negative");

        // KIBIBYTES_DATASTORAGE
        assertThat(from("column_index_size_in_kb", "42").column_index_size.toKibibytes()).isEqualTo(42);
        assertThatThrownBy(() -> from("column_index_size_in_kb", -2).column_index_size.toKibibytes())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid data storage: value must be non-negative");

        // BYTES_DATASTORAGE
        assertThat(from("internode_max_message_size_in_bytes", "42").internode_max_message_size.toBytes()).isEqualTo(42);
        assertThatThrownBy(() -> from("internode_max_message_size_in_bytes", -2).internode_max_message_size.toBytes())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid data storage: value must be non-negative");

        // BYTES_DATASTORAGE
        assertThat(from("internode_max_message_size_in_bytes", "42").internode_max_message_size.toBytes()).isEqualTo(42);
        assertThatThrownBy(() -> from("internode_max_message_size_in_bytes", -2).internode_max_message_size.toBytes())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid data storage: value must be non-negative");

        // MEBIBYTES_PER_SECOND_DATA_RATE
        assertThat(from("compaction_throughput_mb_per_sec", "42").compaction_throughput.toMebibytesPerSecondAsInt()).isEqualTo(42);
        assertThatThrownBy(() -> from("compaction_throughput_mb_per_sec", -2).compaction_throughput.toMebibytesPerSecondAsInt())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid data rate: value must be non-negative");

        // MEGABITS_TO_BYTES_PER_SECOND_DATA_RATE
        assertThat(from("stream_throughput_outbound_megabits_per_sec", "42").stream_throughput_outbound.toMegabitsPerSecondAsInt()).isEqualTo(42);
        assertThatThrownBy(() -> from("stream_throughput_outbound_megabits_per_sec", -2).stream_throughput_outbound.toMegabitsPerSecondAsInt())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid data rate: value must be non-negative");

        // NEGATIVE_MEBIBYTES_DATA_STORAGE_INT
        assertThat(from("sstable_preemptive_open_interval_in_mb", "1").sstable_preemptive_open_interval.toMebibytes()).isEqualTo(1);
        assertThat(from("sstable_preemptive_open_interval_in_mb", -2).sstable_preemptive_open_interval).isNull();

        // LONG_BYTES_DATASTORAGE_MEBIBYTES_INT
        assertThat(from("compaction_large_partition_warning_threshold_mb", "42").partition_size_warn_threshold.toMebibytesInt()).isEqualTo(42);
        assertThatThrownBy(() -> from("compaction_large_partition_warning_threshold_mb", -1).partition_size_warn_threshold.toMebibytesInt())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid data storage: value must be non-negative");

        // LONG_BYTES_DATASTORAGE_MEBIBYTES_DATASTORAGE
        assertThat(from("compaction_large_partition_warning_threshold", "42MiB").partition_size_warn_threshold.toMebibytesInt()).isEqualTo(42);
        assertThat(from("compaction_large_partition_warning_threshold", "42GiB").partition_size_warn_threshold.toMebibytesInt()).isEqualTo(42 * 1024);
        assertThatThrownBy(() -> from("compaction_large_partition_warning_threshold", "42B").partition_size_warn_threshold.toBytes())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid data storage: 42B Accepted units:[MEBIBYTES, GIBIBYTES]");
        assertThatThrownBy(() -> from("compaction_large_partition_warning_threshold", -1).partition_size_warn_threshold.toMebibytesInt())
        .hasRootCauseInstanceOf(IllegalArgumentException.class)
        .hasRootCauseMessage("Invalid data storage: -1 Accepted units:[MEBIBYTES, GIBIBYTES] where case matters and only non-negative values are accepted");

        // IDENTITY
        assertThat(from("compaction_tombstone_warning_threshold", "42").partition_tombstones_warn_threshold).isEqualTo(42);
        assertThat(from("compaction_tombstone_warning_threshold", "-1").partition_tombstones_warn_threshold).isEqualTo(-1);
        assertThat(from("compaction_tombstone_warning_threshold", "0").partition_tombstones_warn_threshold).isEqualTo(0);
    }

    private static Config from(Object... values)
    {
        assert values.length % 2 == 0 : "Map can only be created with an even number of inputs: given " + values.length;
        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        for (int i = 0; i < values.length; i += 2)
            builder.put((String) values[i], values[i + 1]);
        return YamlConfigurationLoader.fromMap(builder.build(), Config.class);
    }

    @Test
    public void testBackwardCompatibilityOfInternodeAuthenticatorPropertyAsMap()
    {
        Config config = load("cassandra-mtls.yaml");
        assertEquals(config.internode_authenticator.class_name, "org.apache.cassandra.auth.MutualTlsInternodeAuthenticator");
        assertFalse(config.internode_authenticator.parameters.isEmpty());
        assertEquals(config.internode_authenticator.parameters.get("validator_class_name"), "org.apache.cassandra.auth.SpiffeCertificateValidator");
    }

    @Test
    public void testBackwardCompatibilityOfInternodeAuthenticatorPropertyAsString()
    {
        Config config = load("cassandra-mtls-backward-compatibility.yaml");
        assertEquals(config.internode_authenticator.class_name, "org.apache.cassandra.auth.AllowAllInternodeAuthenticator");
        assertTrue(config.internode_authenticator.parameters.isEmpty());
    }

    @Test
    public void testBackwardCompatibilityOfAuthenticatorPropertyAsMap()
    {
        Config config = load("cassandra-mtls.yaml");
        assertEquals(config.authenticator.class_name, "org.apache.cassandra.auth.MutualTlsAuthenticator");
        assertFalse(config.authenticator.parameters.isEmpty());
        assertEquals(config.authenticator.parameters.get("validator_class_name"), "org.apache.cassandra.auth.SpiffeCertificateValidator");
    }

    @Test
    public void testBackwardCompatibilityOfAuthenticatorPropertyAsString() throws IOException, TimeoutException
    {
        Config config = load("cassandra-mtls-backward-compatibility.yaml");
        assertEquals(config.authenticator.class_name, "org.apache.cassandra.auth.AllowAllAuthenticator");
        assertTrue(config.authenticator.parameters.isEmpty());
    }

    public static Config load(String path)
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
