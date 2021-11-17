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

import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.function.Consumer;


import com.google.common.base.Throwables;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.assertj.core.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;
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
    public void testConfigurationLoader()
    {
        // By default, we should load from the yaml
        Config config = DatabaseDescriptor.loadConfig();
        assertEquals("Test Cluster", config.cluster_name);
        Keyspace.setInitialized();

        // Now try custom loader
        ConfigurationLoader testLoader = new TestLoader();
        System.setProperty("cassandra.config.loader", testLoader.getClass().getName());

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
    public void testListenInterface()
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
    public void testListenAddress()
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
    public void testInvalidPartition()
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
    public void testInvalidPartitionPropertyOverride()
    {
        String key = Config.PROPERTY_PREFIX + "partitioner";
        String previous = System.getProperty(key);
        try
        {
            System.setProperty(key, "ThisDoesNotExist");
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
        finally
        {
            if (previous == null)
            {
                System.getProperties().remove(key);
            }
            else
            {
                System.setProperty(key, previous);
            }
        }
    }

    @Test
    public void testTokensFromString()
    {
        assertTrue(DatabaseDescriptor.tokensFromString(null).isEmpty());
        Collection<String> tokens = DatabaseDescriptor.tokensFromString(" a,b ,c , d, f,g,h");
        assertEquals(7, tokens.size());
        assertTrue(tokens.containsAll(Arrays.asList("a", "b", "c", "d", "f", "g", "h")));
    }

    @Test
    public void testExceptionsForInvalidConfigValues()
    {
        try
        {
            DatabaseDescriptor.setColumnIndexCacheSize(-1);
            fail("Should have received a ConfigurationException column_index_cache_size_in_kb = -1");
        }
        catch (ConfigurationException ignored) { }
        Assert.assertEquals(2048, DatabaseDescriptor.getColumnIndexCacheSize());

        try
        {
            DatabaseDescriptor.setColumnIndexCacheSize(2 * 1024 * 1024);
            fail("Should have received a ConfigurationException column_index_cache_size_in_kb = 2GiB");
        }
        catch (ConfigurationException ignored) { }
        Assert.assertEquals(2048, DatabaseDescriptor.getColumnIndexCacheSize());

        try
        {
            DatabaseDescriptor.setColumnIndexSize(-1);
            fail("Should have received a ConfigurationException column_index_size_in_kb = -1");
        }
        catch (ConfigurationException ignored) { }
        Assert.assertEquals(4096, DatabaseDescriptor.getColumnIndexSize());

        try
        {
            DatabaseDescriptor.setColumnIndexSize(2 * 1024 * 1024);
            fail("Should have received a ConfigurationException column_index_size_in_kb = 2GiB");
        }
        catch (ConfigurationException ignored) { }
        Assert.assertEquals(4096, DatabaseDescriptor.getColumnIndexSize());

        try
        {
            DatabaseDescriptor.setBatchSizeWarnThresholdInKB(-1);
            fail("Should have received a ConfigurationException batch_size_warn_threshold_in_kb = -1");
        }
        catch (ConfigurationException ignored) { }
        Assert.assertEquals(5120, DatabaseDescriptor.getBatchSizeWarnThreshold());

        try
        {
            DatabaseDescriptor.setBatchSizeWarnThresholdInKB(2 * 1024 * 1024);
            fail("Should have received a ConfigurationException batch_size_warn_threshold_in_kb = 2GiB");
        }
        catch (ConfigurationException ignored) { }
        Assert.assertEquals(4096, DatabaseDescriptor.getColumnIndexSize());
    }

    @Test
    public void testLowestAcceptableTimeouts() throws ConfigurationException
    {
        Config testConfig = new Config();

        Duration greaterThanLowestTimeout = Duration.inMilliseconds(DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT.toMilliseconds()+1);

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
        Duration lowerThanLowestTimeout = Duration.inMilliseconds(DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT.toMilliseconds() - 1);

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
        int previousSize = DatabaseDescriptor.getRepairSessionSpaceInMegabytes();
        try
        {
            Assert.assertEquals((Runtime.getRuntime().maxMemory() / (1024 * 1024) / 16),
                                DatabaseDescriptor.getRepairSessionSpaceInMegabytes());

            int targetSize = (int) (Runtime.getRuntime().maxMemory() / (1024 * 1024) / 4) + 1;

            DatabaseDescriptor.setRepairSessionSpaceInMegabytes(targetSize);
            Assert.assertEquals(targetSize, DatabaseDescriptor.getRepairSessionSpaceInMegabytes());

            DatabaseDescriptor.setRepairSessionSpaceInMegabytes(10);
            Assert.assertEquals(10, DatabaseDescriptor.getRepairSessionSpaceInMegabytes());

            try
            {
                DatabaseDescriptor.setRepairSessionSpaceInMegabytes(0);
                fail("Should have received a ConfigurationException for depth of 9");
            }
            catch (ConfigurationException ignored) { }

            Assert.assertEquals(10, DatabaseDescriptor.getRepairSessionSpaceInMegabytes());
        }
        finally
        {
            DatabaseDescriptor.setRepairSessionSpaceInMegabytes(previousSize);
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
    public void testCalculateDefaultSpaceInMB()
    {
        // check prefered size is used for a small storage volume
        int preferredInMB = 667;
        int numerator = 2;
        int denominator = 3;
        int spaceInBytes = 999 * 1024 * 1024;

        assertEquals(666, // total size is less than preferred, so return lower limit
                     DatabaseDescriptor.calculateDefaultSpaceInMB("type", "/path", "setting_name", preferredInMB, spaceInBytes, numerator, denominator));

        // check preferred size is used for a small storage volume
        preferredInMB = 100;
        numerator = 1;
        denominator = 3;
        spaceInBytes = 999 * 1024 * 1024;

        assertEquals(100, // total size is more than preferred so keep the configured limit
                     DatabaseDescriptor.calculateDefaultSpaceInMB("type", "/path", "setting_name", preferredInMB, spaceInBytes, numerator, denominator));
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
                                                 "set the system property cassandra.allow_unlimited_concurrent_validations=true");
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

        expectIllegalArgumentException(DatabaseDescriptor::setDenylistRefreshSeconds, 0, "denylist_refresh_seconds must be a positive integer.");
        expectIllegalArgumentException(DatabaseDescriptor::setDenylistRefreshSeconds, -1, "denylist_refresh_seconds must be a positive integer.");
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
    public void testClientLargeReadWarnAndAbortNegative()
    {
        Config conf = new Config();
        conf.track_warnings.coordinator_read_size.warn_threshold_kb = -2;
        conf.track_warnings.coordinator_read_size.abort_threshold_kb = -2;
        DatabaseDescriptor.applyTrackWarningsValidations(conf);
        Assertions.assertThat(conf.track_warnings.coordinator_read_size.warn_threshold_kb).isEqualTo(0);
        Assertions.assertThat(conf.track_warnings.coordinator_read_size.abort_threshold_kb).isEqualTo(0);
    }

    @Test
    public void testClientLargeReadWarnGreaterThanAbort()
    {
        Config conf = new Config();
        conf.track_warnings.coordinator_read_size.warn_threshold_kb = 2;
        conf.track_warnings.coordinator_read_size.abort_threshold_kb = 1;
        Assertions.assertThatThrownBy(() -> DatabaseDescriptor.applyTrackWarningsValidations(conf))
                  .isInstanceOf(ConfigurationException.class)
                  .hasMessage("abort_threshold_kb (1) must be greater than or equal to warn_threshold_kb (2); see track_warnings.coordinator_read_size");
    }

    @Test
    public void testClientLargeReadWarnEqAbort()
    {
        Config conf = new Config();
        conf.track_warnings.coordinator_read_size.warn_threshold_kb = 2;
        conf.track_warnings.coordinator_read_size.abort_threshold_kb = 2;
        DatabaseDescriptor.applyTrackWarningsValidations(conf);
    }

    @Test
    public void testClientLargeReadWarnEnabledAbortDisabled()
    {
        Config conf = new Config();
        conf.track_warnings.coordinator_read_size.warn_threshold_kb = 2;
        conf.track_warnings.coordinator_read_size.abort_threshold_kb = 0;
        DatabaseDescriptor.applyTrackWarningsValidations(conf);
    }

    @Test
    public void testClientLargeReadAbortEnabledWarnDisabled()
    {
        Config conf = new Config();
        conf.track_warnings.coordinator_read_size.warn_threshold_kb = 0;
        conf.track_warnings.coordinator_read_size.abort_threshold_kb = 2;
        DatabaseDescriptor.applyTrackWarningsValidations(conf);
    }

    // local read
    @Test
    public void testLocalLargeReadWarnAndAbortNegative()
    {
        Config conf = new Config();
        conf.track_warnings.local_read_size.warn_threshold_kb = -2;
        conf.track_warnings.local_read_size.abort_threshold_kb = -2;
        DatabaseDescriptor.applyTrackWarningsValidations(conf);
        Assertions.assertThat(conf.track_warnings.local_read_size.warn_threshold_kb).isEqualTo(0);
        Assertions.assertThat(conf.track_warnings.local_read_size.abort_threshold_kb).isEqualTo(0);
    }

    @Test
    public void testLocalLargeReadWarnGreaterThanAbort()
    {
        Config conf = new Config();
        conf.track_warnings.local_read_size.warn_threshold_kb = 2;
        conf.track_warnings.local_read_size.abort_threshold_kb = 1;
        Assertions.assertThatThrownBy(() -> DatabaseDescriptor.applyTrackWarningsValidations(conf))
                  .isInstanceOf(ConfigurationException.class)
                  .hasMessage("abort_threshold_kb (1) must be greater than or equal to warn_threshold_kb (2); see track_warnings.local_read_size");
    }

    @Test
    public void testLocalLargeReadWarnEqAbort()
    {
        Config conf = new Config();
        conf.track_warnings.local_read_size.warn_threshold_kb = 2;
        conf.track_warnings.local_read_size.abort_threshold_kb = 2;
        DatabaseDescriptor.applyTrackWarningsValidations(conf);
    }

    @Test
    public void testLocalLargeReadWarnEnabledAbortDisabled()
    {
        Config conf = new Config();
        conf.track_warnings.local_read_size.warn_threshold_kb = 2;
        conf.track_warnings.local_read_size.abort_threshold_kb = 0;
        DatabaseDescriptor.applyTrackWarningsValidations(conf);
    }

    @Test
    public void testLocalLargeReadAbortEnabledWarnDisabled()
    {
        Config conf = new Config();
        conf.track_warnings.local_read_size.warn_threshold_kb = 0;
        conf.track_warnings.local_read_size.abort_threshold_kb = 2;
        DatabaseDescriptor.applyTrackWarningsValidations(conf);
    }

    // row index entry
    @Test
    public void testRowIndexSizeWarnAndAbortNegative()
    {
        Config conf = new Config();
        conf.track_warnings.row_index_size.warn_threshold_kb = -2;
        conf.track_warnings.row_index_size.abort_threshold_kb = -2;
        DatabaseDescriptor.applyTrackWarningsValidations(conf);
        Assertions.assertThat(conf.track_warnings.row_index_size.warn_threshold_kb).isEqualTo(0);
        Assertions.assertThat(conf.track_warnings.row_index_size.abort_threshold_kb).isEqualTo(0);
    }

    @Test
    public void testRowIndexSizeWarnGreaterThanAbort()
    {
        Config conf = new Config();
        conf.track_warnings.row_index_size.warn_threshold_kb = 2;
        conf.track_warnings.row_index_size.abort_threshold_kb = 1;
        Assertions.assertThatThrownBy(() -> DatabaseDescriptor.applyTrackWarningsValidations(conf))
                  .isInstanceOf(ConfigurationException.class)
                  .hasMessage("abort_threshold_kb (1) must be greater than or equal to warn_threshold_kb (2); see track_warnings.row_index_size");
    }

    @Test
    public void testRowIndexSizeWarnEqAbort()
    {
        Config conf = new Config();
        conf.track_warnings.row_index_size.warn_threshold_kb = 2;
        conf.track_warnings.row_index_size.abort_threshold_kb = 2;
        DatabaseDescriptor.applyTrackWarningsValidations(conf);
    }

    @Test
    public void testRowIndexSizeWarnEnabledAbortDisabled()
    {
        Config conf = new Config();
        conf.track_warnings.row_index_size.warn_threshold_kb = 2;
        conf.track_warnings.row_index_size.abort_threshold_kb = 0;
        DatabaseDescriptor.applyTrackWarningsValidations(conf);
    }

    @Test
    public void testRowIndexSizeAbortEnabledWarnDisabled()
    {
        Config conf = new Config();
        conf.track_warnings.row_index_size.warn_threshold_kb = 0;
        conf.track_warnings.row_index_size.abort_threshold_kb = 2;
        DatabaseDescriptor.applyTrackWarningsValidations(conf);
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

    @Test (expected = ConfigurationException.class)
    public void testInvalidSub1DefaultRFs() throws ConfigurationException
    {
        DatabaseDescriptor.setDefaultKeyspaceRF(0);
    }

    @Test (expected = ConfigurationException.class)
    public void testInvalidSub0MinimumRFs() throws ConfigurationException
    {
        DatabaseDescriptor.setMinimumKeyspaceRF(-1);
    }

    @Test (expected = ConfigurationException.class)
    public void testDefaultRfLessThanMinRF()
    {
        DatabaseDescriptor.setMinimumKeyspaceRF(2);
        DatabaseDescriptor.setDefaultKeyspaceRF(1);
    }

    @Test (expected = ConfigurationException.class)
    public void testMinimumRfGreaterThanDefaultRF()
    {
        DatabaseDescriptor.setDefaultKeyspaceRF(1);
        DatabaseDescriptor.setMinimumKeyspaceRF(2);
    }
}
