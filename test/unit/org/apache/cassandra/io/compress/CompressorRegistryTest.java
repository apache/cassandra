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

package org.apache.cassandra.io.compress;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

public class CompressorRegistryTest
{
    static class TestCompressionProvider extends AbstractCompressionProvider
    {
        boolean healthy = false;

        TestCompressionProvider(Map<String, String> args) { super(args); }

        @Override
        public String getProviderName() { return "TestCompressionProvider"; }

        @Override
        public String getProviderSimpleName() { return "TestCompressionProvider"; }

        @Override
        public boolean isHealthy() { return healthy; }

        @Override
        public ICompressor createCompressor(Class<?> compressorClass, Map<String, String> options)
        {
            return NoopCompressor.create(Collections.emptyMap());
        }
    }

    private CompressorRegistry registry;

    @Before
    public void setup() {
        DatabaseDescriptor.daemonInitialization();
        registry = new CompressorRegistry();
    }

    @Test
    public void testRegisterAndGetProvider()
    {
        AbstractCompressionProvider provider = registry.get("TestCompressor"); // should register default provider
        assertThat(provider).isNotNull();
        //Make sure provider name is correct & it holds the correct compressor name
        assertThat(provider.getProviderSimpleName()).isEqualTo("DefaultCompressionProvider");
        assertThat(provider.getAlgorithmName()).contains("TestCompressor");
    }

    @Test
    public void testFallbackToDefaultProvider()
    {
        Config config = DatabaseDescriptor.getRawConfig();
        config.compression_provider = new Config.CompressionProviderOptions();
        LinkedHashMap compressionProviderConfig = new LinkedHashMap();
        compressionProviderConfig.put("NoopCompressor", new ParameterizedClass(TestCompressionProvider.class.getName(), Collections.emptyMap()));
        config.compression_provider.configurations = compressionProviderConfig;
        AbstractCompressionProvider provider = registry.get("NoopCompressor");
        assertThat(provider).isNotNull();
        assertThat(provider.getProviderSimpleName()).isEqualTo("DefaultCompressionProvider");
    }

    @Test
    public void testProviderProperties()
    {
        Map<String, String> params = new HashMap<>();
        params.put("foo", "bar");
        TestCompressionProvider provider = new TestCompressionProvider(params);
        assertThat(provider.getProperties()).containsEntry("foo", "bar");
    }

    @Test
    public void testCompressorCreationAndFallback() {
        Map<String, String> params = new HashMap<>();
        params.put(AbstractCompressionProvider.FALLBACK_TO_DEFAULT_PROVIDER, "false");
        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(()->  registry.getServiceProvider(new ParameterizedClass(TestCompressionProvider.class.getName(), params)))
        .withMessageContaining("Failed to initialize compression provider");
    }
}

