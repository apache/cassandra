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
import java.nio.file.FileStore;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;

import com.google.common.base.Throwables;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import org.apache.cassandra.OrderedJUnit4ClassRunner;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

@RunWith(OrderedJUnit4ClassRunner.class)
public class DatabaseDescriptorTest
{
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

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
    public void testRpcInterface() throws Exception
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
    public void testRpcAddress() throws Exception
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
        assertTrue(tokens.containsAll(Arrays.asList(new String[]{"a", "b", "c", "d", "f", "g", "h"})));
    }

    @Test
    public void testExceptionsForInvalidConfigValues() {
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
            DatabaseDescriptor.getGuardrailsConfig().setBatchSizeWarnThresholdInKB(-2);
            fail("Should have received a ConfigurationException batch_size_warn_threshold_in_kb = -2");
        }
        catch (ConfigurationException ignored) { }
        Assert.assertEquals(65536, DatabaseDescriptor.getGuardrailsConfig().getBatchSizeWarnThreshold());

        try
        {
            DatabaseDescriptor.getGuardrailsConfig().setBatchSizeWarnThresholdInKB(2 * 1024 * 1024);
            fail("Should have received a ConfigurationException batch_size_warn_threshold_in_kb = 2GiB");
        }
        catch (ConfigurationException ignored) { }
        Assert.assertEquals(4096, DatabaseDescriptor.getColumnIndexSize());
    }

    @Test
    public void testLowestAcceptableTimeouts() throws ConfigurationException
    {
        Config testConfig = new Config();
        testConfig.read_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        testConfig.range_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        testConfig.write_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        testConfig.truncate_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        testConfig.cas_contention_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        testConfig.counter_write_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        testConfig.request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT + 1;
        
        assertTrue(testConfig.read_request_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.range_request_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.write_request_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.truncate_request_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.cas_contention_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.counter_write_request_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.request_timeout_in_ms > DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);

        //set less than Lowest acceptable value
        testConfig.read_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;
        testConfig.range_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;
        testConfig.write_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;
        testConfig.truncate_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;
        testConfig.cas_contention_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;
        testConfig.counter_write_request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;
        testConfig.request_timeout_in_ms = DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT - 1;

        DatabaseDescriptor.checkForLowestAcceptedTimeouts(testConfig);

        assertTrue(testConfig.read_request_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.range_request_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.write_request_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.truncate_request_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.cas_contention_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.counter_write_request_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
        assertTrue(testConfig.request_timeout_in_ms == DatabaseDescriptor.LOWEST_ACCEPTED_TIMEOUT);
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
    public void testDataFileDirectoriesMinTotalSpaceInGB() throws IOException
    {
        DatabaseDescriptor.getRawConfig().data_file_directories = new String[]{};
        assertEquals(0L, DatabaseDescriptor.getDataFileDirectoriesMinTotalSpaceInGB());

        DatabaseDescriptor.getRawConfig().data_file_directories = new String[] { temporaryFolder.newFolder("data").toString() };
        assertTrue(DatabaseDescriptor.getDataFileDirectoriesMinTotalSpaceInGB() > 0);

        Multiset<FileStore> fileStoreMultiset = HashMultiset.create();

        // single disk (i.e. mockFileStore1)
        FileStore mockFileStore1 = Mockito.mock(FileStore.class);
        when(mockFileStore1.getTotalSpace()).thenReturn(1L << 43); // 8 TB
        fileStoreMultiset.add(mockFileStore1);
        assertEquals(8192L, DatabaseDescriptor.getDataFileDirectoriesMinTotalSpaceInGB(fileStoreMultiset));

        // two different disks (i.e. mockFileStore1, mockFileStore2)
        FileStore mockFileStore2 = Mockito.mock(FileStore.class);
        when(mockFileStore2.getTotalSpace()).thenReturn(1L << 41); // 2 TB
        fileStoreMultiset.add(mockFileStore2);
        assertEquals(4096L, DatabaseDescriptor.getDataFileDirectoriesMinTotalSpaceInGB(fileStoreMultiset));

        // two different disks with three directories. Two directories are on disk 1 (i.e. mockFileStore1)
        fileStoreMultiset.add(mockFileStore1);
        assertEquals(6144L, DatabaseDescriptor.getDataFileDirectoriesMinTotalSpaceInGB(fileStoreMultiset));

        fileStoreMultiset.clear();

        FileStore mockLargeFileStore = Mockito.mock(FileStore.class);
        when(mockLargeFileStore.getTotalSpace()).thenReturn(-1L);
        fileStoreMultiset.add(mockLargeFileStore);
        assertEquals(Long.MAX_VALUE >> 30, DatabaseDescriptor.getDataFileDirectoriesMinTotalSpaceInGB(fileStoreMultiset));

        FileStore mockSmallFileStore = Mockito.mock(FileStore.class);
        when(mockSmallFileStore.getTotalSpace()).thenReturn(1L << 29); // 512 MB
        fileStoreMultiset.add(mockSmallFileStore);
        assertEquals(0L, DatabaseDescriptor.getDataFileDirectoriesMinTotalSpaceInGB(fileStoreMultiset));
    }
}
