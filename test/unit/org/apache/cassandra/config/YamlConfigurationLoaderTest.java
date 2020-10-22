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
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertArrayEquals;


public class YamlConfigurationLoaderTest
{
    @Test
    public void fromMapTest()
    {
        int storagePort = 123;
        Config.CommitLogSync commitLogSync = Config.CommitLogSync.batch;
        ParameterizedClass seedProvider = new ParameterizedClass("org.apache.cassandra.locator.SimpleSeedProvider", Collections.emptyMap());
        EncryptionOptions encryptionOptions = new EncryptionOptions.ClientEncryptionOptions();
        encryptionOptions.keystore = "myNewKeystore";
        encryptionOptions.cipher_suites = new String[] {"SomeCipher"};

        Map<String,Object> map = ImmutableMap.of("storage_port", storagePort,
                                                 "commitlog_sync", commitLogSync,
                                                 "seed_provider", seedProvider,
                                                 "client_encryption_options", encryptionOptions);
        Config config = YamlConfigurationLoader.fromMap(map, Config.class);
        assertEquals(storagePort, config.storage_port); // Check a simple integer
        assertEquals(commitLogSync, config.commitlog_sync); // Check an enum
        assertEquals(seedProvider, config.seed_provider); // Check a parameterized class
        assertEquals(encryptionOptions.keystore, config.client_encryption_options.keystore); // Check a nested object
        assertArrayEquals(encryptionOptions.cipher_suites, config.client_encryption_options.cipher_suites);
    }
}
