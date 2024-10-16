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

package org.apache.cassandra.distributed.test;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.test.jmx.JMXGetterCheckTest;

public class JMXEncryptionOptionsTest extends AbstractEncryptionOptionsImpl
{
    @Test
    public void testDefaultSettings() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.JMX);
            c.set("jmx_encryption_options",
                  ImmutableMap.builder().putAll(validKeystore)
                              .put("enabled", true)
                              .put("require_client_auth", false)
                              .build());
        }).start())
        {
            // Invoke the same code vs duplicating any code from the JMXGetterCheckTest
            JMXGetterCheckTest.testAllValidGetters(cluster);
        }
    }

    @Test
    public void testPEMBasedEncryptionOptions() throws Throwable
    {
        try (Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.JMX);
            c.set("jmx_encryption_options",
                  ImmutableMap.builder().putAll(validPEMKeystore)
                              .put("enabled", true)
                              .put("require_client_auth", false)
                              .build());
        }).start())
        {
            // Invoke the same code vs duplicating any code from the JMXGetterCheckTest
            JMXGetterCheckTest.testAllValidGetters(cluster);
        }
    }

    @Test
    public void testInvalidKeystorePath() throws Throwable
    {
        try(Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.JMX);
            c.set("jmx_encryption_options",
                  ImmutableMap.builder()
                              .put("enabled", true)
                              .put("keystore", "/path/to/bad/keystore/that/should/not/exist")
                              .put("keystore_password", "cassandra")
                              .build());
        }).createWithoutStarting())
        {
            assertCannotStartDueToConfigurationException(cluster);
        }
    }

    /**
     * Tests {@code disabled} jmx_encryption_options. Here even if the configured {@code keystore} is invalid, it will
     * not matter and the JMX server/client will start.
     */
    @Test
    public void testDisabledEncryptionOptions() throws Throwable
    {
        try(Cluster cluster = builder().withNodes(1).withConfig(c -> {
            c.with(Feature.JMX);
            c.set("jmx_encryption_options",
                  ImmutableMap.builder()
                              .put("enabled", false)
                              .put("keystore", "/path/to/bad/keystore/that/should/not/exist")
                              .put("keystore_password", "cassandra")
                              .build());
        }).start())
        {
            // Invoke the same code vs duplicating any code from the JMXGetterCheckTest
            JMXGetterCheckTest.testAllValidGetters(cluster);
        }
    }
}
