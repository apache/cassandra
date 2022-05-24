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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.io.util.File;
import org.junit.Assert;
import org.junit.Test;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.assertj.core.api.Assertions;

import static org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.all;
import static org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.dc;
import static org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.none;
import static org.apache.cassandra.config.EncryptionOptions.ServerEncryptionOptions.InternodeEncryption.rack;
import static org.apache.cassandra.config.EncryptionOptions.TlsEncryptionPolicy.ENCRYPTED;
import static org.apache.cassandra.config.EncryptionOptions.TlsEncryptionPolicy.OPTIONAL;
import static org.apache.cassandra.config.EncryptionOptions.TlsEncryptionPolicy.UNENCRYPTED;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class EncryptionOptionsTest
{
    static class EncryptionOptionsTestCase
    {
        final EncryptionOptions encryptionOptions;
        final EncryptionOptions.TlsEncryptionPolicy expected;
        final String description;

        public EncryptionOptionsTestCase(EncryptionOptions encryptionOptions, EncryptionOptions.TlsEncryptionPolicy expected, String description)
        {
            this.encryptionOptions = encryptionOptions;
            this.expected = expected;
            this.description = description;
        }

        public static EncryptionOptionsTestCase of(Boolean optional, String keystorePath, Boolean enabled, EncryptionOptions.TlsEncryptionPolicy expected)
        {
            return new EncryptionOptionsTestCase(new EncryptionOptions(new ParameterizedClass("org.apache.cassandra.security.DefaultSslContextFactory",
                                                                                              new HashMap<>()),
                                                                       keystorePath, "dummypass",
                                                                       "dummytruststore", "dummypass",
                                                                       Collections.emptyList(), null, null, null, "JKS", false, false, enabled, optional)
                                                 .applyConfig(),
                                                 expected,
                                                 String.format("optional=%s keystore=%s enabled=%s", optional, keystorePath, enabled));
        }

        public static EncryptionOptionsTestCase of(Boolean optional, String keystorePath, Boolean enabled,
                                                   Map<String,String> customSslContextFactoryParams,
                                                   EncryptionOptions.TlsEncryptionPolicy expected)
        {
            return new EncryptionOptionsTestCase(new EncryptionOptions(new ParameterizedClass("org.apache.cassandra.security.DefaultSslContextFactory",
                                                                                              customSslContextFactoryParams),
                                                                       keystorePath, "dummypass",
                                                                       "dummytruststore", "dummypass",
                                                                       Collections.emptyList(), null, null, null, "JKS", false, false, enabled, optional)
                                                 .applyConfig(),
                                                 expected,
                                                 String.format("optional=%s keystore=%s enabled=%s", optional, keystorePath, enabled));
        }
    }

    static final String absentKeystore = "test/conf/missing-keystore-is-not-here";
    static final String presentKeystore = "test/conf/keystore.jks";
    final EncryptionOptionsTestCase[] encryptionOptionTestCases = {
        //                         Optional    Keystore     Enabled  Expected
        EncryptionOptionsTestCase.of(null, absentKeystore,  false, UNENCRYPTED),
        EncryptionOptionsTestCase.of(null, absentKeystore,  true,  ENCRYPTED),
        EncryptionOptionsTestCase.of(null, presentKeystore, false, OPTIONAL),
        EncryptionOptionsTestCase.of(null, presentKeystore, true,  ENCRYPTED),
        EncryptionOptionsTestCase.of(false, absentKeystore, false, UNENCRYPTED),
        EncryptionOptionsTestCase.of(false, absentKeystore, true,  ENCRYPTED),
        EncryptionOptionsTestCase.of(true, presentKeystore, false, OPTIONAL),
        EncryptionOptionsTestCase.of(true, presentKeystore, true,  OPTIONAL)
    };

    @Test
    public void testEncryptionOptionPolicy()
    {
        assertTrue(new File(presentKeystore).exists());
        assertFalse(new File(absentKeystore).exists());
        for (EncryptionOptionsTestCase testCase : encryptionOptionTestCases)
        {
            Assert.assertSame(testCase.description, testCase.expected, testCase.encryptionOptions.tlsEncryptionPolicy());
        }
    }

    static class ServerEncryptionOptionsTestCase
    {
        final EncryptionOptions encryptionOptions;
        final EncryptionOptions.TlsEncryptionPolicy expected;
        final String description;

        public ServerEncryptionOptionsTestCase(EncryptionOptions encryptionOptions, EncryptionOptions.TlsEncryptionPolicy expected, String description)
        {
            this.encryptionOptions = encryptionOptions;
            this.expected = expected;
            this.description = description;
        }

        public static ServerEncryptionOptionsTestCase of(Boolean optional, String keystorePath,
                                                         EncryptionOptions.ServerEncryptionOptions.InternodeEncryption internodeEncryption,
                                                         EncryptionOptions.TlsEncryptionPolicy expected)
        {
            return new ServerEncryptionOptionsTestCase(new EncryptionOptions.ServerEncryptionOptions(new ParameterizedClass("org.apache.cassandra.security.DefaultSslContextFactory",
                                                                                                                            new HashMap<>()), keystorePath, "dummypass", keystorePath, "dummypass", "dummytruststore", "dummypass",
                                                                                               Collections.emptyList(), null, null, null, "JKS", false, false, optional, internodeEncryption, false)
                                                       .applyConfig(),
                                                 expected,
                                                 String.format("optional=%s keystore=%s internode=%s", optional, keystorePath, internodeEncryption));
        }
    }

    @Test
    public void isEnabledServer()
    {
        Map<String, Object> yaml = ImmutableMap.of(
        "server_encryption_options", ImmutableMap.of(
            "isEnabled", false
            )
        );

        Assertions.assertThatThrownBy(() -> YamlConfigurationLoader.fromMap(yaml, Config.class))
                  .isInstanceOf(ConfigurationException.class)
                  .hasMessage("Invalid yaml. Please remove properties [isEnabled] from your cassandra.yaml");
    }

    @Test
    public void isOptionalServer()
    {
        Map<String, Object> yaml = ImmutableMap.of(
        "server_encryption_options", ImmutableMap.of(
            "isOptional", false
            )
        );

        Assertions.assertThatThrownBy(() -> YamlConfigurationLoader.fromMap(yaml, Config.class))
                  .isInstanceOf(ConfigurationException.class)
                  .hasMessage("Invalid yaml. Please remove properties [isOptional] from your cassandra.yaml");
    }

    final ServerEncryptionOptionsTestCase[] serverEncryptionOptionTestCases = {

        //                               Optional    Keystore    Internode  Expected
        ServerEncryptionOptionsTestCase.of(null, absentKeystore, none, UNENCRYPTED),
        ServerEncryptionOptionsTestCase.of(null, absentKeystore, rack, OPTIONAL),
        ServerEncryptionOptionsTestCase.of(null, absentKeystore, dc,   OPTIONAL),
        ServerEncryptionOptionsTestCase.of(null, absentKeystore, all,  ENCRYPTED),

        ServerEncryptionOptionsTestCase.of(null, presentKeystore, none, OPTIONAL),
        ServerEncryptionOptionsTestCase.of(null, presentKeystore, rack, OPTIONAL),
        ServerEncryptionOptionsTestCase.of(null, absentKeystore,  dc,   OPTIONAL),
        ServerEncryptionOptionsTestCase.of(null, absentKeystore,  all,  ENCRYPTED),

        ServerEncryptionOptionsTestCase.of(false, absentKeystore, none, UNENCRYPTED),
        ServerEncryptionOptionsTestCase.of(false, absentKeystore, rack, OPTIONAL),
        ServerEncryptionOptionsTestCase.of(false, absentKeystore, dc,   OPTIONAL),
        ServerEncryptionOptionsTestCase.of(false, absentKeystore, all,  ENCRYPTED),

        ServerEncryptionOptionsTestCase.of(true, presentKeystore, none, OPTIONAL),
        ServerEncryptionOptionsTestCase.of(true, presentKeystore, rack, OPTIONAL),
        ServerEncryptionOptionsTestCase.of(true, absentKeystore,  dc,   OPTIONAL),
        ServerEncryptionOptionsTestCase.of(true, absentKeystore,  all,  OPTIONAL),
    };

    @Test
    public void testServerEncryptionOptionPolicy()
    {
        assertTrue(new File(presentKeystore).exists());
        assertFalse(new File(absentKeystore).exists());
        for (ServerEncryptionOptionsTestCase testCase : serverEncryptionOptionTestCases)
        {
            Assert.assertSame(testCase.description, testCase.expected, testCase.encryptionOptions.tlsEncryptionPolicy());
        }
    }

    @Test(expected =  IllegalArgumentException.class)
    public void testMisplacedConfigKey()
    {
        Map<String, String> customSslContextFactoryParams = new HashMap<>();

        for(EncryptionOptions.ConfigKey configKey: EncryptionOptions.ConfigKey.values())
        {
            customSslContextFactoryParams.put(configKey.getKeyName(), "my-custom-value");
        }

        EncryptionOptionsTestCase.of(null, absentKeystore, true, customSslContextFactoryParams, ENCRYPTED);
    }
}
