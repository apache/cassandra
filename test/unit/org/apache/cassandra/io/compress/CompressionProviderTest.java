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

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.ConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;


public class CompressionProviderTest
{
    @BeforeClass
    public static void setup()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Test
    public void testDefaultProvider()
    {
        AbstractCompressionProvider provider = DatabaseDescriptor.getCompressionProvider();
        assertThat(provider.getProviderSimpleName()).isEqualTo("DefaultCompressionProvider");
    }

    @Test
    public void testDefaultProviderWithNullParameters()
    {
        Config config = DatabaseDescriptor.getRawConfig();
        config.compression_provider.class_name = "DefaultCompressionProvider";
        config.compression_provider.parameters = null;
        DatabaseDescriptor.applyCompressionProvider();
        AbstractCompressionProvider provider = DatabaseDescriptor.getCompressionProvider();
        assertThat(provider.getProviderSimpleName()).isEqualTo("DefaultCompressionProvider");
        assertThat(provider.getProperties()).isNotNull()
                                                  .isNotEmpty()
                                                  .hasSize(1)
                                                  .containsKeys("fallback_to_default_provider")
                                                  .containsValues("true");

    }

    @Test
    public void testUnhealthyProviderWithFallback() throws Exception
    {
        Config config = DatabaseDescriptor.getRawConfig();
        config.compression_provider.class_name = "MockCompressionProvider";
        config.compression_provider.parameters = Collections.singletonMap("fallback_to_default_provider", "true");
        DatabaseDescriptor.applyCompressionProvider();
        AbstractCompressionProvider provider = DatabaseDescriptor.getCompressionProvider();
        assertThat(provider.getProviderSimpleName()).isEqualTo("DefaultCompressionProvider");
        assertThat(provider.getProperties()).isNotNull()
                                            .isNotEmpty()
                                            .hasSize(1)
                                            .containsKeys("fallback_to_default_provider")
                                            .containsValues("true");
    }

    @Test
    public void testUnhealthyProviderWithoutFallback()
    {
        Config config = DatabaseDescriptor.getRawConfig();
        config.compression_provider.class_name = "MockCompressionProvider";
        config.compression_provider.parameters = Collections.singletonMap("fallback_to_default_provider", "false");
        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(()-> DatabaseDescriptor.applyCompressionProvider())
        .withMessageContaining("Failed to initialize compression provider");
    }

    @Test
    public void testHealthyProviderWithFaultyCompressorWithFallback() throws Exception
    {
        AbstractCompressionProvider provider = spy(new MockCompressionProvider(Collections.singletonMap("fallback_to_default_provider", "true")));
        doReturn(true).when(provider).isHealthy();
        ICompressor baseCompressor =  NoopCompressor.create(Collections.emptyMap());
        ICompressor providerCompressor =provider.getCompressor(baseCompressor,null);
        assertThat(providerCompressor.getClass().getSimpleName()).isEqualTo(baseCompressor.getClass().getSimpleName());
    }

    @Test
    public void testHealthyProviderWithFaultyCompressorWithoutFallback() throws Exception
    {
        AbstractCompressionProvider provider = spy(new MockCompressionProvider(Collections.singletonMap("fallback_to_default_provider", "false")));

        doReturn(true).when(provider).isHealthy();
        ICompressor baseCompressor =  NoopCompressor.create(Collections.emptyMap());
        assertThatExceptionOfType(IllegalStateException.class)
            .isThrownBy(()-> provider.getCompressor(baseCompressor,null))
            .withMessageContaining("Failed to create compressor")
            .withMessageContaining("Fallback to default provider is disabled.");
    }
}
