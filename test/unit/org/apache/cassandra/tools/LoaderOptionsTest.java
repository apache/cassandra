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

package org.apache.cassandra.tools;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.file.Paths;
import java.security.Permission;

import com.google.common.net.HostAndPort;
import org.junit.Test;

import org.apache.cassandra.io.util.File;

import static org.apache.cassandra.tools.OfflineToolUtils.sstableDirName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

// LoaderOptionsTester for custom configuration
public class LoaderOptionsTest
{
    @Test
    public void testNativePort() throws Exception
    {
        //Default Cassandra config
        File config = new File(Paths.get(".", "test", "conf", "cassandra.yaml").normalize());
        String[] args = { "-d", "127.9.9.1", "-f", config.absolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple") };
        LoaderOptions options = LoaderOptions.builder().parseArgs(args).build();
        assertEquals(9042, options.nativePort);


        // SSL Enabled Cassandra config
        config = new File(Paths.get(".", "test", "conf", "unit-test-conf/test-native-port.yaml").normalize());
        String[] args2 = { "-d", "127.9.9.1", "-f", config.absolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple") };
        options = LoaderOptions.builder().parseArgs(args2).build();
        assertEquals(9142, options.nativePort);

        HostAndPort hap = HostAndPort.fromString("127.9.9.1");
        InetAddress byName = InetAddress.getByName(hap.getHost());
        assertTrue(options.hosts.contains(new InetSocketAddress(byName, 9142)));

        // test native port set from command line

        config = new File(Paths.get(".", "test", "conf", "unit-test-conf/test-native-port.yaml").normalize().toFile());
        String[] args3 = { "-d", "127.9.9.1", "-p", "9300", "-f", config.absolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple") };
        options = LoaderOptions.builder().parseArgs(args3).build();
        assertEquals(9300, options.nativePort);

        assertTrue(options.hosts.contains(new InetSocketAddress(byName, 9300)));
    }

    /**
     * Regression testing for CASSANDRA-16280
     *
     * Check that providing encryption parameters to the loader (such as keystore and truststore) won't break loading
     * the options.
     *
     * @throws Exception
     */
    @Test
    public void testEncryptionSettings() throws Exception
    {
        String[] args = { "-d", "127.9.9.1", "-ts", "test.jks", "-tspw", "truststorePass1", "-ks", "test.jks", "-kspw",
                          "testdata1", "--ssl-ciphers", "TLS_RSA_WITH_AES_256_CBC_SHA",
                          "--ssl-alg", "SunX509", "--store-type", "JKS", "--ssl-protocol", "TLS",
                          sstableDirName("legacy_sstables", "legacy_ma_simple") };
        LoaderOptions options = LoaderOptions.builder().parseArgs(args).build();
        assertEquals("test.jks", options.clientEncOptions.keystore);
    }

    @Test
    public void testThrottleDefaultSettings()
    {
        LoaderOptions options = LoaderOptions.builder().build();
        assertEquals(0, options.throttleBytes, 0);
        assertEquals(0, options.interDcThrottleBytes, 0);
    }

    @Test
    public void testDeprecatedThrottleSettings() throws IOException
    {
        // Default Cassandra config
        File config = new File(Paths.get(".", "test", "conf", "cassandra.yaml").normalize());
        String[] args = { "-t", "200", "-idct", "400", "-d", "127.9.9.1", "-f", config.absolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple") };
        LoaderOptions options = LoaderOptions.builder().parseArgs(args).build();
        // converts from megabits to bytes
        assertEquals(200 * 125_000, options.throttleBytes, 0);
        assertEquals(400 * 125_000, options.interDcThrottleBytes, 0);
    }

    @Test
    public void testDeprecatedThrottleSettingsWithLongSettingNames() throws IOException
    {
        // Default Cassandra config
        File config = new File(Paths.get(".", "test", "conf", "cassandra.yaml").normalize());
        String[] args = { "--throttle", "200", "--inter-dc-throttle", "400", "-d", "127.9.9.1", "-f", config.absolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple") };
        LoaderOptions options = LoaderOptions.builder().parseArgs(args).build();
        // converts from megabits to bytes
        assertEquals(200 * 125_000, options.throttleBytes, 0);
        assertEquals(400 * 125_000, options.interDcThrottleBytes, 0);
    }

    @Test
    public void testThrottleSettingsWithLongSettingNames() throws IOException
    {
        // Default Cassandra config
        File config = new File(Paths.get(".", "test", "conf", "cassandra.yaml").normalize());
        String[] args = { "--throttle-mib", "24", "--inter-dc-throttle-mib", "48", "-d", "127.9.9.1", "-f", config.absolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple") };
        LoaderOptions options = LoaderOptions.builder().parseArgs(args).build();
        // converts from mebibytes to bytes
        assertEquals(24 * 1024 * 1024, options.throttleBytes, 0);
        assertEquals(48 * 1024 * 1024, options.interDcThrottleBytes, 0);
    }

    @Test
    public void failsWhenThrottleSettingAndDeprecatedAreProvided() throws IOException
    {
        File config = new File(Paths.get(".", "test", "conf", "cassandra.yaml").normalize());
        String[] args = { "-t", "200", "-tmib", "200", "-d", "127.9.9.1", "-f", config.absolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple") };

        failureHelper(args, 1);
    }

    @Test
    public void failsWhenThrottleSettingAndDeprecatedAreProvidedWithLongSettingNames() throws IOException
    {
        File config = new File(Paths.get(".", "test", "conf", "cassandra.yaml").normalize());
        String[] args = { "--throttle", "200", "--throttle-mib", "200", "-d", "127.9.9.1", "-f", config.absolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple") };

        failureHelper(args, 1);
    }

    @Test
    public void failsWhenInterDCThrottleSettingAndDeprecatedAreProvided() throws IOException
    {
        File config = new File(Paths.get(".", "test", "conf", "cassandra.yaml").normalize());
        String[] args = { "-idct", "200", "-idctmib", "200", "-d", "127.9.9.1", "-f", config.absolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple") };

        failureHelper(args, 1);
    }

    @Test
    public void failsWhenInterDCThrottleSettingAndDeprecatedAreProvidedWithLongSettingNames() throws IOException
    {
        File config = new File(Paths.get(".", "test", "conf", "cassandra.yaml").normalize());
        String[] args = { "--inter-dc-throttle", "200", "--inter-dc-throttle-mib", "200", "-d", "127.9.9.1", "-f", config.absolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple") };

        failureHelper(args, 1);
    }

    @Test
    public void testEntireSSTableDefaultSettings()
    {
        LoaderOptions options = LoaderOptions.builder().build();
        assertEquals(0, options.entireSSTableThrottleMebibytes);
        assertEquals(0, options.entireSSTableInterDcThrottleMebibytes);
    }

    @Test
    public void testEntireSSTableSettingsWithLongSettingNames() throws IOException
    {
        // Use long names for the args, i.e. entire-sstable-throttle
        File config = new File(Paths.get(".", "test", "conf", "cassandra.yaml").normalize());
        String[] args = new String[]{ "--entire-sstable-throttle-mib", "350", "--entire-sstable-inter-dc-throttle-mib", "600", "-d", "127.9.9.1", "-f", config.absolutePath(), sstableDirName("legacy_sstables", "legacy_ma_simple") };
        LoaderOptions options = LoaderOptions.builder().parseArgs(args).build();
        assertEquals(350, options.entireSSTableThrottleMebibytes);
        assertEquals(600, options.entireSSTableInterDcThrottleMebibytes);
    }

    private void failureHelper(String[] args, int expectedErrorCode)
    {
        // install security manager to get informed about the exit-code
        System.setSecurityManager(new SecurityManager()
        {
            public void checkExit(int status)
            {
                throw new SystemExitException(status);
            }

            public void checkPermission(Permission perm)
            {
            }

            public void checkPermission(Permission perm, Object context)
            {
            }
        });
        try
        {
            LoaderOptions.builder().parseArgs(args).build();
        }
        catch (SystemExitException e)
        {
            assertEquals(expectedErrorCode, e.status);
        }
        finally
        {
            System.setSecurityManager(null);
        }
    }
}

