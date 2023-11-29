/*
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing,
* software distributed under the License is distributed on an
* "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
* KIND, either express or implied.  See the License for the
* specific language governing permissions and limitations
* under the License.
*/
package org.apache.cassandra.config;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.function.Consumer;

import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.distributed.shared.WithProperties;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.security.EncryptionContext;
import org.apache.cassandra.security.EncryptionContextGenerator;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.CassandraRelevantProperties.ALLOW_UNLIMITED_CONCURRENT_VALIDATIONS;
import static org.apache.cassandra.config.CassandraRelevantProperties.CONFIG_LOADER;
import static org.apache.cassandra.config.CassandraRelevantProperties.PARTITIONER;
import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.KIBIBYTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DatabaseDescriptorTest
{
    @BeforeClass
    public static void setupDatabaseDescriptor()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    // this came as a result of CASSANDRA-995
    @Test
    public void testConfigurationLoader() throws Exception
    {
        // By default, we should load from the yaml
        Config config = DatabaseDescriptor.loadConfig();
        assertEquals("Test Cluster", config.cluster_name);
        Keyspace.setInitialized();

        // Now try custom loader
        ConfigurationLoader testLoader = new TestLoader();
        CONFIG_LOADER.setString(testLoader.getClass().getName());

        config = DatabaseDescriptor.loadConfig();
        assertEquals("ConfigurationLoader Test", config.cluster_name);
    }

    public static class TestLoader implements ConfigurationLoader
    {
        public Config loadConfig() throws ConfigurationException
        {
            Config testConfig = new Config();
            testConfig.cluster_name = "ConfigurationLoader Test";
            return testConfig;
        }
    }

    static NetworkInterface suitableInterface = null;
    static boolean hasIPv4andIPv6 = false;

    /*
     * Server only accepts interfaces by name if they have a single address
     * OS X seems to always have an ipv4 and ipv6 address on all interfaces which means some tests fail
     * if not checked for and skipped
     */
    @BeforeClass
    public static void selectSuitableInterface() throws Exception {
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        while(interfaces.hasMoreElements()) {
            NetworkInterface intf = interfaces.nextElement();

            System.out.println("Evaluating " + intf.getName());

            if (intf.isLoopback()) {
                suitableInterface = intf;

                boolean hasIPv4 = false;
                boolean hasIPv6 = false;
                Enumeration<InetAddress> addresses = suitableInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    if (addresses.nextElement() instanceof Inet6Address)
                        hasIPv6 = true;
                    else
                        hasIPv4 = true;
                }
                hasIPv4andIPv6 = hasIPv4 && hasIPv6;
                return;
            }
        }
    }

    @Test
    public void testRpcInterface()
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.rpc_interface = suitableInterface.getName();
        testConfig.rpc_address = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);

        /*
         * Confirm ability to select between IPv4 and IPv6
         */
        if (hasIPv4andIPv6)
        {
            testConfig = DatabaseDescriptor.loadConfig();
            testConfig.rpc_interface = suitableInterface.getName();
            testConfig.rpc_address = null;
            testConfig.rpc_interface_prefer_ipv6 = true;
            DatabaseDescriptor.applyAddressConfig(testConfig);

            assertEquals(DatabaseDescriptor.getRpcAddress().getClass(), Inet6Address.class);

            testConfig = DatabaseDescriptor.loadConfig();
            testConfig.rpc_interface = suitableInterface.getName();
            testConfig.rpc_address = null;
            testConfig.rpc_interface_prefer_ipv6 = false;
            DatabaseDescriptor.applyAddressConfig(testConfig);

            assertEquals(DatabaseDescriptor.getRpcAddress().getClass(), Inet4Address.class);
        }
        else
        {
            /*
             * Confirm first address of interface is selected
             */
            assertEquals(DatabaseDescriptor.getRpcAddress(), suitableInterface.getInetAddresses().nextElement());
        }
    }

    @Test
    public void testListenInterface() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.listen_interface = suitableInterface.getName();
        testConfig.listen_address = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);

        /*
         * Confirm ability to select between IPv4 and IPv6
         */
        if (hasIPv4andIPv6)
        {
            testConfig = DatabaseDescriptor.loadConfig();
            testConfig.listen_interface = suitableInterface.getName();
            testConfig.listen_address = null;
            testConfig.listen_interface_prefer_ipv6 = true;
            DatabaseDescriptor.applyAddressConfig(testConfig);

            assertEquals(DatabaseDescriptor.getListenAddress().getClass(), Inet6Address.class);

            testConfig = DatabaseDescriptor.loadConfig();
            testConfig.listen_interface = suitableInterface.getName();
            testConfig.listen_address = null;
            testConfig.listen_interface_prefer_ipv6 = false;
            DatabaseDescriptor.applyAddressConfig(testConfig);

            assertEquals(DatabaseDescriptor.getListenAddress().getClass(), Inet4Address.class);
        }
        else
        {
            /*
             * Confirm first address of interface is selected
             */
            assertEquals(DatabaseDescriptor.getRpcAddress(), suitableInterface.getInetAddresses().nextElement());
        }
    }

    @Test
    public void testListenAddress() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.listen_address = suitableInterface.getInterfaceAddresses().get(0).getAddress().getHostAddress();
        testConfig.listen_interface = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);
    }

    @Test
    public void testRpcAddress()
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.rpc_address = suitableInterface.getInterfaceAddresses().get(0).getAddress().getHostAddress();
        testConfig.rpc_interface = null;
        DatabaseDescriptor.applyAddressConfig(testConfig);

    }

    @Test
    public void testInvalidPartition() throws Exception
    {
        Config testConfig = DatabaseDescriptor.loadConfig();
        testConfig.partitioner = "ThisDoesNotExist";

        try
        {
            DatabaseDescriptor.applyPartitioner(testConfig);
            Assert.fail("Partition does not exist, so should fail");
        }
        catch (ConfigurationException e)
        {
            Assert.assertEquals("Invalid partitioner class ThisDoesNotExist", e.getMessage());
            Throwable cause = Throwables.getRootCause(e);
            Assert.assertNotNull("Unable to find root cause why partitioner was rejected", cause);
            // this is a bit implementation specific, so free to change; mostly here to make sure reason isn't lost
            Assert.assertEquals(ClassNotFoundException.class, cause.getClass());
            Assert.assertEquals("org.apache.cassandra.dht.ThisDoesNotExist", cause.getMessage());
        }
    }

    @Test
    public void testInvalidPartitionPropertyOverride() throws Exception
    {
        try (WithProperties properties = new WithProperties().set(PARTITIONER, "ThisDoesNotExist"))
        {
            Config testConfig = DatabaseDescriptor.loadConfig();
            testConfig.partitioner = "Murmur3Partitioner";

            try
            {
                DatabaseDescriptor.applyPartitioner(testConfig);
                Assert.fail("Partition does not exist, so should fail");
            }
            catch (ConfigurationException e)
            {
                Assert.assertEquals("Invalid partitioner class ThisDoesNotExist", e.getMessage());
                Throwable cause = Throwables.getRootCause(e);
                Assert.assertNotNull("Unable to find root cause why partitioner was rejected", cause);
                // this is a bit implementation specific, so free to change; mostly here to make sure reason isn't lost
                Assert.assertEquals(ClassNotFoundException.class, cause.getClass());
                Assert.assertEquals("org.apache.cassandra.dht.ThisDoesNotExist", cause.getMessage());
            }
        }
    }

    @Test
    public void testTokensFromString()
    {
        assertTrue(DatabaseDescriptor.tokensFromString(null).isEmpty());
        Collection<String> tokens = DatabaseDescriptor.tokensFromString(" a,b ,c , d, f,g,h");
        assertEquals(7, tokens.size());
        assertTrue(tokens.containsAll(Arrays.asList(new String[]{"a", "b", "c", "d", "f", "g", "h"})));
    }

    @Test
    public void testExceptionsForInvalidConfigValues() {
        try
        {
            DatabaseDescriptor.setColumnIndexCacheSize(-1);
            fail("Should have received a IllegalArgumentException column_index_cache_size = -1");
        }
        catch (IllegalArgumentException ignored) { }
        Assert.assertEquals(2048, DatabaseDescriptor.getColumnIndexCacheSize());

        try
        {
            DatabaseDescriptor.setColumnIndexCacheSize(2 * 1024 * 1024);
            fail("Should have received a ConfigurationException column_index_cache_size= 2GiB");
        }
        catch (ConfigurationException ignored) { }
        Assert.assertEquals(2048, DatabaseDescriptor.getColumnIndexCacheSize());

        try
        {
            DatabaseDescriptor.setColumnIndexSizeInKiB(-5);
            fail("Should have received a IllegalArgumentException column_index_size = -5");
        }
        catch (IllegalArgumentException ignored) { }
        Assert.assertEquals(4096, DatabaseDescriptor.getColumnIndexSize(0));

        try
        {
            DatabaseDescriptor.setColumnIndexSizeInKiB(2 * 1024 * 1024);
            fail("Should have received a ConfigurationException column_index_size = 2GiB");
        }
        catch (ConfigurationException ignored) { }
        Assert.assertEquals(4096, DatabaseDescriptor.getColumnIndexSize(0));

        DatabaseDescriptor.setColumnIndexSizeInKiB(-1);  // set undefined
        Assert.assertEquals(8192, DatabaseDescriptor.getColumnIndexSize(8192));

        try
        {
            DatabaseDescriptor.setBatchSizeWarnThresholdInKiB(-1);
            fail("Should have received a IllegalArgumentException batch_size_warn_threshold = -1");
        }
        catch (IllegalArgumentException ignored) { }
        Assert.assertEquals(5120, DatabaseDescriptor.getBatchSizeWarnThreshold());

        try
        {
            DatabaseDescriptor.setBatchSizeWarnThresholdInKiB(2 * 1024 * 1024);
            fail("Should have received a ConfigurationException batch_size_warn_threshold = 2GiB");
        }
        catch (ConfigurationException ignored) { }
        Assert.assertEquals(5120, DatabaseDescriptor.getBatchSizeWarnThreshold());
    }

    @Test
    public void testWidenToLongInBytes() throws ConfigurationException
    {
        Config conf = DatabaseDescriptor.getRawConfig();
        int maxInt = Integer.MAX_VALUE - 1;
        long maxIntMebibytesAsBytes = (long) maxInt * 1024 * 1024;
        long maxIntKibibytesAsBytes = (long) maxInt * 1024;

        conf.min_free_space_per_drive = new DataStorageSpec.IntMebibytesBound(maxInt);
        Assert.assertEquals(maxIntMebibytesAsBytes, DatabaseDescriptor.getMinFreeSpacePerDriveInBytes());

        conf.max_hints_file_size = new DataStorageSpec.IntMebibytesBound(maxInt);
        Assert.assertEquals(maxIntMebibytesAsBytes, DatabaseDescriptor.getMaxHintsFileSize());

        DatabaseDescriptor.setBatchSizeFailThresholdInKiB(maxInt);
        Assert.assertEquals((maxIntKibibytesAsBytes), DatabaseDescriptor.getBatchSizeFailThreshold());
    }

    @Test
    public void testLowestAcceptableTimeouts() throws ConfigurationException
    {
        Config testConfig = new Config();

        DurationSpec.LongMillisecondsBound greaterThanLowestTimeout = new DurationSpec.LongMillisecondsBound(DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT.toMilliseconds() + 1);

        testConfig.read_request_timeout = greaterThanLowestTimeout;
        testConfig.range_request_timeout = greaterThanLowestTimeout;
        testConfig.write_request_timeout = greaterThanLowestTimeout;
        testConfig.truncate_request_timeout = greaterThanLowestTimeout;
        testConfig.cas_contention_timeout = greaterThanLowestTimeout;
        testConfig.counter_write_request_timeout = greaterThanLowestTimeout;
        testConfig.request_timeout = greaterThanLowestTimeout;

        assertEquals(testConfig.read_request_timeout, greaterThanLowestTimeout);
        assertEquals(testConfig.range_request_timeout, greaterThanLowestTimeout);
        assertEquals(testConfig.write_request_timeout, greaterThanLowestTimeout);
        assertEquals(testConfig.truncate_request_timeout, greaterThanLowestTimeout);
        assertEquals(testConfig.cas_contention_timeout, greaterThanLowestTimeout);
        assertEquals(testConfig.counter_write_request_timeout, greaterThanLowestTimeout);
        assertEquals(testConfig.request_timeout, greaterThanLowestTimeout);

        //set less than Lowest acceptable value
        DurationSpec.LongMillisecondsBound lowerThanLowestTimeout = new DurationSpec.LongMillisecondsBound(DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT.toMilliseconds() - 1);

        testConfig.read_request_timeout = lowerThanLowestTimeout;
        testConfig.range_request_timeout = lowerThanLowestTimeout;
        testConfig.write_request_timeout = lowerThanLowestTimeout;
        testConfig.truncate_request_timeout = lowerThanLowestTimeout;
        testConfig.cas_contention_timeout = lowerThanLowestTimeout;
        testConfig.counter_write_request_timeout = lowerThanLowestTimeout;
        testConfig.request_timeout = lowerThanLowestTimeout;

        DatabaseDescriptor.checkForLowestAcceptedTimeouts(testConfig);

        assertEquals(testConfig.read_request_timeout, DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertEquals(testConfig.range_request_timeout, DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertEquals(testConfig.write_request_timeout, DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertEquals(testConfig.truncate_request_timeout, DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertEquals(testConfig.cas_contention_timeout, DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertEquals(testConfig.counter_write_request_timeout, DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertEquals(testConfig.request_timeout, DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
    }

    @Test
    public void testRepairSessionMemorySizeToggles()
    {
        int previousSize = DatabaseDescriptor.getRepairSessionSpaceInMiB();
        try
        {
            Assert.assertEquals((Runtime.getRuntime().maxMemory() / (1024 * 1024) / 16),
                                DatabaseDescriptor.getRepairSessionSpaceInMiB());

            int targetSize = (int) (Runtime.getRuntime().maxMemory() / (1024 * 1024) / 4) + 1;

            DatabaseDescriptor.setRepairSessionSpaceInMiB(targetSize);
            Assert.assertEquals(targetSize, DatabaseDescriptor.getRepairSessionSpaceInMiB());

            DatabaseDescriptor.setRepairSessionSpaceInMiB(10);
            Assert.assertEquals(10, DatabaseDescriptor.getRepairSessionSpaceInMiB());

            try
            {
                DatabaseDescriptor.setRepairSessionSpaceInMiB(0);
                fail("Should have received a ConfigurationException for depth of 0");
            }
            catch (ConfigurationException ignored) { }

            Assert.assertEquals(10, DatabaseDescriptor.getRepairSessionSpaceInMiB());
        }
        finally
        {
            DatabaseDescriptor.setRepairSessionSpaceInMiB(previousSize);
        }
    }

    @Test
    public void testRepairSessionSizeToggles()
    {
        int previousDepth = DatabaseDescriptor.getRepairSessionMaxTreeDepth();
        try
        {
            Assert.assertEquals(20, DatabaseDescriptor.getRepairSessionMaxTreeDepth());
            DatabaseDescriptor.setRepairSessionMaxTreeDepth(10);
            Assert.assertEquals(10, DatabaseDescriptor.getRepairSessionMaxTreeDepth());

            try
            {
                DatabaseDescriptor.setRepairSessionMaxTreeDepth(9);
                fail("Should have received a ConfigurationException for depth of 9");
            }
            catch (ConfigurationException ignored) { }
            Assert.assertEquals(10, DatabaseDescriptor.getRepairSessionMaxTreeDepth());

            try
            {
                DatabaseDescriptor.setRepairSessionMaxTreeDepth(-20);
                fail("Should have received a ConfigurationException for depth of -20");
            }
            catch (ConfigurationException ignored) { }
            Assert.assertEquals(10, DatabaseDescriptor.getRepairSessionMaxTreeDepth());

            DatabaseDescriptor.setRepairSessionMaxTreeDepth(22);
            Assert.assertEquals(22, DatabaseDescriptor.getRepairSessionMaxTreeDepth());
        }
        finally
        {
            DatabaseDescriptor.setRepairSessionMaxTreeDepth(previousDepth);
        }
    }

    @Test
    public void testCalculateDefaultSpaceInMiB()
    {
        // check prefered size is used for a small storage volume
        int preferredInMiB = 667;
        int numerator = 2;
        int denominator = 3;
        int spaceInBytes = 999 * 1024 * 1024;

        assertEquals(666, // total size is less than preferred, so return lower limit
                     DatabaseDescriptor.calculateDefaultSpaceInMiB("type", "/path", "setting_name", preferredInMiB, spaceInBytes, numerator, denominator));

        // check preferred size is used for a small storage volume
        preferredInMiB = 100;
        numerator = 1;
        denominator = 3;
        spaceInBytes = 999 * 1024 * 1024;

        assertEquals(100, // total size is more than preferred so keep the configured limit
                     DatabaseDescriptor.calculateDefaultSpaceInMiB("type", "/path", "setting_name", preferredInMiB, spaceInBytes, numerator, denominator));
    }

    @Test
    public void testConcurrentValidations()
    {
        Config conf = new Config();
        conf.concurrent_compactors = 8;
        // if concurrent_validations is < 1 (including being unset) it should default to concurrent_compactors
        assertThat(conf.concurrent_validations).isLessThan(1);
        DatabaseDescriptor.applyConcurrentValidations(conf);
        assertThat(conf.concurrent_validations).isEqualTo(conf.concurrent_compactors);

        // otherwise, it must be <= concurrent_compactors
        conf.concurrent_validations = conf.concurrent_compactors + 1;
        try
        {
            DatabaseDescriptor.applyConcurrentValidations(conf);
            fail("Expected exception");
        }
        catch (ConfigurationException e)
        {
            assertThat(e.getMessage()).isEqualTo("To set concurrent_validations > concurrent_compactors, " +
                                                 "set the system property -D" + ALLOW_UNLIMITED_CONCURRENT_VALIDATIONS.getKey() + "=true");
        }

        // unless we disable that check (done with a system property at startup or via JMX)
        DatabaseDescriptor.allowUnlimitedConcurrentValidations = true;
        conf.concurrent_validations = conf.concurrent_compactors + 1;
        DatabaseDescriptor.applyConcurrentValidations(conf);
        assertThat(conf.concurrent_validations).isEqualTo(conf.concurrent_compactors + 1);
    }

    @Test
    public void testRepairCommandPoolSize()
    {
        Config conf = new Config();
        conf.concurrent_validations = 3;
        // if repair_command_pool_size is < 1 (including being unset) it should default to concurrent_validations
        assertThat(conf.repair_command_pool_size).isLessThan(1);
        DatabaseDescriptor.applyRepairCommandPoolSize(conf);
        assertThat(conf.repair_command_pool_size).isEqualTo(conf.concurrent_validations);

        // but it can be overridden
        conf.repair_command_pool_size = conf.concurrent_validations + 1;
        DatabaseDescriptor.applyRepairCommandPoolSize(conf);
        assertThat(conf.repair_command_pool_size).isEqualTo(conf.concurrent_validations + 1);
    }

    @Test
    public void testApplyTokensConfigInitialTokensSetNumTokensSetAndDoesMatch()
    {
        Config config = DatabaseDescriptor.loadConfig();
        config.initial_token = "0,256,1024";
        config.num_tokens = 3;

        try
        {
            DatabaseDescriptor.applyTokensConfig(config);
            Assert.assertEquals(Integer.valueOf(3), config.num_tokens);
            Assert.assertEquals(3, DatabaseDescriptor.tokensFromString(config.initial_token).size());
        }
        catch (ConfigurationException e)
        {
            Assert.fail("number of tokens in initial_token=0,256,1024 does not match num_tokens = 3");
        }
    }

    @Test
    public void testApplyTokensConfigInitialTokensSetNumTokensSetAndDoesntMatch()
    {
        Config config = DatabaseDescriptor.loadConfig();
        config.initial_token = "0,256,1024";
        config.num_tokens = 10;

        try
        {
            DatabaseDescriptor.applyTokensConfig(config);

            Assert.fail("initial_token = 0,256,1024 and num_tokens = 10 but applyTokensConfig() did not fail!");
        }
        catch (ConfigurationException ex)
        {
            Assert.assertEquals("The number of initial tokens (by initial_token) specified (3) is different from num_tokens value (10)",
                                ex.getMessage());
        }
    }

    @Test
    public void testApplyTokensConfigInitialTokensSetNumTokensNotSet()
    {
        Config config = DatabaseDescriptor.loadConfig();
        config.initial_token = "0,256,1024";

        try
        {
            DatabaseDescriptor.applyTokensConfig(config);
            Assert.fail("setting initial_token and not setting num_tokens is invalid");
        }
        catch (ConfigurationException ex)
        {
            Assert.assertEquals("initial_token was set but num_tokens is not!", ex.getMessage());
        }
    }

    @Test
    public void testUpperBoundStreamingConfigOnStartup()
    {
        Config config = DatabaseDescriptor.loadConfig();

        String expectedMsg = "Invalid value of entire_sstable_stream_throughput_outbound:";
        config.entire_sstable_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(Integer.MAX_VALUE, DataRateSpec.DataRateUnit.MEBIBYTES_PER_SECOND);
        validateProperty(expectedMsg);

        expectedMsg = "Invalid value of entire_sstable_stream_throughput_outbound:";
        config.entire_sstable_inter_dc_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(Integer.MAX_VALUE, DataRateSpec.DataRateUnit.MEBIBYTES_PER_SECOND);
        validateProperty(expectedMsg);

        expectedMsg = "Invalid value of stream_throughput_outbound:";
        config.stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(Integer.MAX_VALUE * 125_000L);
        validateProperty(expectedMsg);

        expectedMsg = "Invalid value of inter_dc_stream_throughput_outbound:";
        config.inter_dc_stream_throughput_outbound = new DataRateSpec.LongBytesPerSecondBound(Integer.MAX_VALUE * 125_000L);
        validateProperty(expectedMsg);

        expectedMsg = "compaction_throughput:";
        config.compaction_throughput = new DataRateSpec.LongBytesPerSecondBound(Integer.MAX_VALUE, DataRateSpec.DataRateUnit.MEBIBYTES_PER_SECOND);
        validateProperty(expectedMsg);
    }

    private static void validateProperty(String expectedMsg)
    {
        try
        {
            DatabaseDescriptor.validateUpperBoundStreamingConfig();
        }
        catch (ConfigurationException ex)
        {
            Assert.assertEquals(expectedMsg, ex.getMessage());
        }
    }

    @Test
    public void testApplyTokensConfigInitialTokensNotSetNumTokensSet()
    {
        Config config = DatabaseDescriptor.loadConfig();
        config.num_tokens = 3;

        DatabaseDescriptor.applyTokensConfig(config);

        Assert.assertEquals(Integer.valueOf(3), config.num_tokens);
        Assert.assertTrue(DatabaseDescriptor.tokensFromString(config.initial_token).isEmpty());
    }

    @Test
    public void testApplyTokensConfigInitialTokensNotSetNumTokensNotSet()
    {
        Config config = DatabaseDescriptor.loadConfig();
        DatabaseDescriptor.applyTokensConfig(config);

        Assert.assertEquals(Integer.valueOf(1), config.num_tokens);
        Assert.assertTrue(DatabaseDescriptor.tokensFromString(config.initial_token).isEmpty());
    }

    @Test
    public void testApplyTokensConfigInitialTokensOneNumTokensNotSet()
    {
        Config config = DatabaseDescriptor.loadConfig();
        config.initial_token = "123";
        config.num_tokens = null;

        DatabaseDescriptor.applyTokensConfig(config);

        Assert.assertEquals(Integer.valueOf(1), config.num_tokens);
        Assert.assertEquals(1, DatabaseDescriptor.tokensFromString(config.initial_token).size());
    }

    @Test
    public void testDenylistInvalidValuesRejected()
    {
        DatabaseDescriptor.loadConfig();

        expectIllegalArgumentException(DatabaseDescriptor::setDenylistRefreshSeconds, 0, "denylist_refresh must be a positive integer.");
        expectIllegalArgumentException(DatabaseDescriptor::setDenylistRefreshSeconds, -1, "denylist_refresh must be a positive integer.");
        expectIllegalArgumentException(DatabaseDescriptor::setDenylistMaxKeysPerTable, 0, "denylist_max_keys_per_table must be a positive integer.");
        expectIllegalArgumentException(DatabaseDescriptor::setDenylistMaxKeysPerTable, -1, "denylist_max_keys_per_table must be a positive integer.");
        expectIllegalArgumentException(DatabaseDescriptor::setDenylistMaxKeysTotal, 0, "denylist_max_keys_total must be a positive integer.");
        expectIllegalArgumentException(DatabaseDescriptor::setDenylistMaxKeysTotal, -1, "denylist_max_keys_total must be a positive integer.");
    }

    private void expectIllegalArgumentException(Consumer<Integer> c, int val, String expectedMessage)
    {
        assertThatThrownBy(() -> c.accept(val))
            .isInstanceOf(IllegalArgumentException.class)
            .hasMessageContaining(expectedMessage);
    }

    // coordinator read
    @Test
    public void testClientLargeReadWarnGreaterThanAbort()
    {
        Config conf = new Config();
        conf.coordinator_read_size_warn_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        conf.coordinator_read_size_fail_threshold = new DataStorageSpec.LongBytesBound(1, KIBIBYTES);
        Assertions.assertThatThrownBy(() -> DatabaseDescriptor.applyReadThresholdsValidations(conf))
                  .isInstanceOf(ConfigurationException.class)
                  .hasMessage("coordinator_read_size_fail_threshold (1KiB) must be greater than or equal to coordinator_read_size_warn_threshold (2KiB)");
    }

    @Test
    public void testClientLargeReadWarnEqAbort()
    {
        Config conf = new Config();
        conf.coordinator_read_size_warn_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        conf.coordinator_read_size_fail_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        DatabaseDescriptor.applyReadThresholdsValidations(conf);
    }

    @Test
    public void testClientLargeReadWarnEnabledAbortDisabled()
    {
        Config conf = new Config();
        conf.coordinator_read_size_warn_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        conf.coordinator_read_size_fail_threshold = null;
        DatabaseDescriptor.applyReadThresholdsValidations(conf);
    }

    @Test
    public void testClientLargeReadAbortEnabledWarnDisabled()
    {
        Config conf = new Config();
        conf.coordinator_read_size_warn_threshold = new DataStorageSpec.LongBytesBound(0, KIBIBYTES);
        conf.coordinator_read_size_fail_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        DatabaseDescriptor.applyReadThresholdsValidations(conf);
    }

    // local read

    @Test
    public void testLocalLargeReadWarnGreaterThanAbort()
    {
        Config conf = new Config();
        conf.local_read_size_warn_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        conf.local_read_size_fail_threshold = new DataStorageSpec.LongBytesBound(1, KIBIBYTES);
        Assertions.assertThatThrownBy(() -> DatabaseDescriptor.applyReadThresholdsValidations(conf))
                  .isInstanceOf(ConfigurationException.class)
                  .hasMessage("local_read_size_fail_threshold (1KiB) must be greater than or equal to local_read_size_warn_threshold (2KiB)");
    }

    @Test
    public void testLocalLargeReadWarnEqAbort()
    {
        Config conf = new Config();
        conf.local_read_size_warn_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        conf.local_read_size_fail_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        DatabaseDescriptor.applyReadThresholdsValidations(conf);
    }

    @Test
    public void testLocalLargeReadWarnEnabledAbortDisabled()
    {
        Config conf = new Config();
        conf.local_read_size_warn_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        conf.local_read_size_fail_threshold = null;
        DatabaseDescriptor.applyReadThresholdsValidations(conf);
    }

    @Test
    public void testLocalLargeReadAbortEnabledWarnDisabled()
    {
        Config conf = new Config();
        conf.local_read_size_warn_threshold = new DataStorageSpec.LongBytesBound(0, KIBIBYTES);
        conf.local_read_size_fail_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        DatabaseDescriptor.applyReadThresholdsValidations(conf);
    }

    // row index entry

    @Test
    public void testRowIndexSizeWarnGreaterThanAbort()
    {
        Config conf = new Config();
        conf.row_index_read_size_warn_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        conf.row_index_read_size_fail_threshold = new DataStorageSpec.LongBytesBound(1, KIBIBYTES);
        Assertions.assertThatThrownBy(() -> DatabaseDescriptor.applyReadThresholdsValidations(conf))
                  .isInstanceOf(ConfigurationException.class)
                  .hasMessage("row_index_read_size_fail_threshold (1KiB) must be greater than or equal to row_index_read_size_warn_threshold (2KiB)");
    }

    @Test
    public void testRowIndexSizeWarnEqAbort()
    {
        Config conf = new Config();
        conf.row_index_read_size_warn_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        conf.row_index_read_size_fail_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        DatabaseDescriptor.applyReadThresholdsValidations(conf);
    }

    @Test
    public void testRowIndexSizeWarnEnabledAbortDisabled()
    {
        Config conf = new Config();
        conf.row_index_read_size_warn_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        conf.row_index_read_size_fail_threshold = null;
        DatabaseDescriptor.applyReadThresholdsValidations(conf);
    }

    @Test
    public void testRowIndexSizeAbortEnabledWarnDisabled()
    {
        Config conf = new Config();
        conf.row_index_read_size_warn_threshold = new DataStorageSpec.LongBytesBound(0, KIBIBYTES);
        conf.row_index_read_size_fail_threshold = new DataStorageSpec.LongBytesBound(2, KIBIBYTES);
        DatabaseDescriptor.applyReadThresholdsValidations(conf);
    }

    @Test
    public void testDefaultSslContextFactoryConfiguration()
    {
        Config config = DatabaseDescriptor.loadConfig();
        Assert.assertEquals("org.apache.cassandra.security.DefaultSslContextFactory",
                            config.client_encryption_options.ssl_context_factory.class_name);
        Assert.assertTrue(config.client_encryption_options.ssl_context_factory.parameters.isEmpty());
        Assert.assertEquals("org.apache.cassandra.security.DefaultSslContextFactory",
                            config.server_encryption_options.ssl_context_factory.class_name);
        Assert.assertTrue(config.server_encryption_options.ssl_context_factory.parameters.isEmpty());
    }

    @Test (expected = IllegalArgumentException.class)
    public void testInvalidSub1DefaultRFs() throws IllegalArgumentException
    {
        DatabaseDescriptor.setDefaultKeyspaceRF(0);
    }

    @Test
    public void testCommitLogDiskAccessMode() throws IOException
    {
        ParameterizedClass savedCompression = DatabaseDescriptor.getCommitLogCompression();
        EncryptionContext savedEncryptionContexg = DatabaseDescriptor.getEncryptionContext();
        Config.DiskAccessMode savedCommitLogDOS = DatabaseDescriptor.getCommitLogWriteDiskAccessMode();
        String savedCommitLogLocation = DatabaseDescriptor.getCommitLogLocation();

        try
        {
            // block size available
            DatabaseDescriptor.setCommitLogLocation(Files.createTempDirectory("testCommitLogDiskAccessMode").toString());

            // no encryption or compression
            DatabaseDescriptor.setCommitLogCompression(null);
            DatabaseDescriptor.setEncryptionContext(null);
            DatabaseDescriptor.getRawConfig().disk_optimization_strategy = Config.DiskOptimizationStrategy.spinning;
            assertCommitLogDiskAccessModes(Config.DiskAccessMode.mmap, Config.DiskAccessMode.mmap, Config.DiskAccessMode.mmap, Config.DiskAccessMode.direct);
            DatabaseDescriptor.getRawConfig().disk_optimization_strategy = Config.DiskOptimizationStrategy.ssd;
            assertCommitLogDiskAccessModes(Config.DiskAccessMode.mmap, Config.DiskAccessMode.direct, Config.DiskAccessMode.mmap, Config.DiskAccessMode.direct);

            // compression enabled
            DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor", null));
            DatabaseDescriptor.setEncryptionContext(null);
            DatabaseDescriptor.getRawConfig().disk_optimization_strategy = Config.DiskOptimizationStrategy.spinning;
            assertCommitLogDiskAccessModes(Config.DiskAccessMode.standard, Config.DiskAccessMode.standard, Config.DiskAccessMode.standard);
            DatabaseDescriptor.getRawConfig().disk_optimization_strategy = Config.DiskOptimizationStrategy.ssd;
            assertCommitLogDiskAccessModes(Config.DiskAccessMode.standard, Config.DiskAccessMode.standard, Config.DiskAccessMode.standard);

            // encryption enabled
            DatabaseDescriptor.setCommitLogCompression(null);
            DatabaseDescriptor.setEncryptionContext(new EncryptionContext(EncryptionContextGenerator.createEncryptionOptions()));
            DatabaseDescriptor.getRawConfig().disk_optimization_strategy = Config.DiskOptimizationStrategy.spinning;
            assertCommitLogDiskAccessModes(Config.DiskAccessMode.standard, Config.DiskAccessMode.standard, Config.DiskAccessMode.standard);
            DatabaseDescriptor.getRawConfig().disk_optimization_strategy = Config.DiskOptimizationStrategy.ssd;
            assertCommitLogDiskAccessModes(Config.DiskAccessMode.standard, Config.DiskAccessMode.standard, Config.DiskAccessMode.standard);

            // block size not available
            DatabaseDescriptor.setCommitLogLocation(null);

            // no encryption or compression
            DatabaseDescriptor.setCommitLogCompression(null);
            DatabaseDescriptor.setEncryptionContext(null);
            DatabaseDescriptor.getRawConfig().disk_optimization_strategy = Config.DiskOptimizationStrategy.spinning;
            assertCommitLogDiskAccessModes(Config.DiskAccessMode.mmap, Config.DiskAccessMode.mmap, Config.DiskAccessMode.mmap, Config.DiskAccessMode.direct);
            DatabaseDescriptor.getRawConfig().disk_optimization_strategy = Config.DiskOptimizationStrategy.ssd;
            assertCommitLogDiskAccessModes(Config.DiskAccessMode.mmap, Config.DiskAccessMode.mmap, Config.DiskAccessMode.mmap, Config.DiskAccessMode.direct);

            // compression enabled
            DatabaseDescriptor.setCommitLogCompression(new ParameterizedClass("LZ4Compressor", null));
            DatabaseDescriptor.setEncryptionContext(null);
            DatabaseDescriptor.getRawConfig().disk_optimization_strategy = Config.DiskOptimizationStrategy.spinning;
            assertCommitLogDiskAccessModes(Config.DiskAccessMode.standard, Config.DiskAccessMode.standard, Config.DiskAccessMode.standard);
            DatabaseDescriptor.getRawConfig().disk_optimization_strategy = Config.DiskOptimizationStrategy.ssd;
            assertCommitLogDiskAccessModes(Config.DiskAccessMode.standard, Config.DiskAccessMode.standard, Config.DiskAccessMode.standard);

            // encryption enabled
            DatabaseDescriptor.setCommitLogCompression(null);
            DatabaseDescriptor.setEncryptionContext(new EncryptionContext(EncryptionContextGenerator.createEncryptionOptions()));
            DatabaseDescriptor.getRawConfig().disk_optimization_strategy = Config.DiskOptimizationStrategy.spinning;
            assertCommitLogDiskAccessModes(Config.DiskAccessMode.standard, Config.DiskAccessMode.standard, Config.DiskAccessMode.standard);
            DatabaseDescriptor.getRawConfig().disk_optimization_strategy = Config.DiskOptimizationStrategy.ssd;
            assertCommitLogDiskAccessModes(Config.DiskAccessMode.standard, Config.DiskAccessMode.standard, Config.DiskAccessMode.standard);
        }
        finally
        {
            DatabaseDescriptor.setCommitLogCompression(savedCompression);
            DatabaseDescriptor.setEncryptionContext(savedEncryptionContexg);
            DatabaseDescriptor.setCommitLogWriteDiskAccessMode(savedCommitLogDOS);
            DatabaseDescriptor.setCommitLogLocation(savedCommitLogLocation);
        }
    }

    private void assertCommitLogDiskAccessModes(Config.DiskAccessMode expectedLegacy, Config.DiskAccessMode expectedAuto, Config.DiskAccessMode... allowedModesArray)
    {
        EnumSet<Config.DiskAccessMode> allowedModes = EnumSet.copyOf(Arrays.asList(allowedModesArray));
        allowedModes.add(Config.DiskAccessMode.legacy);
        allowedModes.add(Config.DiskAccessMode.auto);

        EnumSet<Config.DiskAccessMode> disallowedModes = EnumSet.complementOf(allowedModes);

        for (Config.DiskAccessMode mode : disallowedModes)
        {
            DatabaseDescriptor.setCommitLogWriteDiskAccessMode(mode);
            assertThatExceptionOfType(ConfigurationException.class).isThrownBy(DatabaseDescriptor::initializeCommitLogDiskAccessMode);
        }

        for (Config.DiskAccessMode mode : allowedModes)
        {
            DatabaseDescriptor.setCommitLogWriteDiskAccessMode(mode);
            DatabaseDescriptor.initializeCommitLogDiskAccessMode();
            boolean changed = DatabaseDescriptor.getCommitLogWriteDiskAccessMode() != mode;
            assertThat(changed).isEqualTo(mode == Config.DiskAccessMode.legacy || mode == Config.DiskAccessMode.auto);
            if (mode == Config.DiskAccessMode.legacy)
                assertThat(DatabaseDescriptor.getCommitLogWriteDiskAccessMode()).isEqualTo(expectedLegacy);
            else if (mode == Config.DiskAccessMode.auto)
                assertThat(DatabaseDescriptor.getCommitLogWriteDiskAccessMode()).isEqualTo(expectedAuto);
            else
                assertThat(DatabaseDescriptor.getCommitLogWriteDiskAccessMode()).isEqualTo(mode);
        }
    }
}
