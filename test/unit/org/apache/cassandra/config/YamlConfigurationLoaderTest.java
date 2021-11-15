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


public class YamlConfigurationLoaderTest
{
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
                                 .put("internode_send_buff_size_in_bytes", 5)
                                 .put("internode_recv_buff_size_in_bytes", 5)
                                 .build();

        Config config = YamlConfigurationLoader.fromMap(map, Config.class);
        assertEquals(storagePort, config.storage_port); // Check a simple integer
        assertEquals(commitLogSync, config.commitlog_sync); // Check an enum
        assertEquals(seedProvider, config.seed_provider); // Check a parameterized class
        assertEquals(false, config.client_encryption_options.optional); // Check a nested object
        assertEquals(true, config.client_encryption_options.enabled); // Check a nested object
        assertEquals(5, config.internode_socket_send_buffer_size_in_bytes); // Check names backward compatibility (CASSANDRA-17141)
        assertEquals(5, config.internode_socket_receive_buffer_size_in_bytes); // Check names backward compatibility (CASSANDRA-17141)
    }
}
