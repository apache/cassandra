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

import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


public class CompressorRegistryTest
{
    private CompressorRegistry registry;
    private Config originalConfig;

    @Before
    public void setUp() {
        originalConfig = DatabaseDescriptor.loadConfig();
        registry = null;
    }

    @After
    public void tearDown() {
        registry = null;
    }

    @Test
    public void testRegisterAndGetInbuiltCompressor()
    {
        //Make sure default provider is registered and is returned when an in-built compressor is requested
        DatabaseDescriptor.setConfig(originalConfig);
        DatabaseDescriptor.applyCompressionProvider();
        registry = CompressorRegistry.instance;
        AbstractCompressionProvider provider = registry.getProvider("LZ4Compressor");
        assertThat(provider).isNotNull();
        assertThat(provider.getProviderSimpleName()).isEqualTo("DefaultCompressionProvider");
        assertThat(registry.getCompressorTypeFullName("LZ4Compressor")).isEqualTo("org.apache.cassandra.io.compress.LZ4Compressor");
        assertThat(registry.getCompressorTypeSimpleName("LZ4Compressor")).isEqualTo("LZ4Compressor");
    }

    @Test
    public void testRegisterServiceProvider() throws Exception
    {
        //Register a custom provider and ensure it is returned when requested
        Config config = originalConfig;
        Map<String, String> params = new HashMap<>();
        params.put(CompressorRegistry.FALLBACK_TO_DEFAULT_PROVIDER, "true");
        config.compression_provider_options.put("NoopCompressor", new ParameterizedClass("TestCompressionProvider", params));
        DatabaseDescriptor.setConfig(config);

        AbstractCompressionProvider mockProvider = mock(AbstractCompressionProvider.class);
        when(mockProvider.isHealthy()).thenReturn(true);
        when(mockProvider.getProviderSimpleName()).thenReturn("TestCompressionProvider");
        when(mockProvider.getProviderName()).thenReturn("TestCompressionProvider");

        try (MockedStatic<FBUtilities> fbUtilsMock = mockStatic(FBUtilities.class))
        {
            fbUtilsMock.when(() -> FBUtilities.newCompressionProvider("TestCompressionProvider"))
                       .thenReturn(mockProvider);
            DatabaseDescriptor.applyCompressionProvider();
            registry = CompressorRegistry.instance;
            AbstractCompressionProvider provider = registry.getProvider("NoopCompressor");
            assertThat(provider).isNotNull();
            assertThat(provider.isHealthy()).isTrue();
            assertThat(provider.getProviderSimpleName()).isEqualTo("TestCompressionProvider");
        }
    }

    @Test
    public void testUnhealthyProviderWithFallback() throws Exception
    {
        //Try registering a custom provider that is unhealthy and ensure the registry falls back to the default provider
        Map<String, String> params = new HashMap<>();
        params.put(CompressorRegistry.FALLBACK_TO_DEFAULT_PROVIDER, "true");

        AbstractCompressionProvider mockProvider = mock(AbstractCompressionProvider.class);
        when(mockProvider.isHealthy()).thenReturn(false);
        try (MockedStatic<FBUtilities> fbUtilsMock = mockStatic(FBUtilities.class))
        {
            fbUtilsMock.when(() -> FBUtilities.newCompressionProvider("TestCompressionProvider"))
                       .thenReturn(mockProvider);

            registry = CompressorRegistry.instance;
            AbstractCompressionProvider provider = registry.getServiceProvider(new ParameterizedClass("TestCompressionProvider", params));
            assertThat(provider).isNotNull();
            assertThat(provider.getProviderSimpleName()).isEqualTo("DefaultCompressionProvider");
        }
    }

    @Test
    public void testUnhealthyProviderWithoutFallback() throws Exception
    {
        //Try registering a custom provider that is unhealthy and ensure the registry throws an exception when fallback is disabled
        Config config = originalConfig;
        Map<String, String> params = new HashMap<>();
        params.put(CompressorRegistry.FALLBACK_TO_DEFAULT_PROVIDER, "false");
        DatabaseDescriptor.setConfig(config);

        AbstractCompressionProvider mockProvider = mock(AbstractCompressionProvider.class);
        when(mockProvider.isHealthy()).thenReturn(false);
        try (MockedStatic<FBUtilities> fbUtilsMock = mockStatic(FBUtilities.class))
        {
            fbUtilsMock.when(() -> FBUtilities.newCompressionProvider("TestCompressionProvider"))
                       .thenReturn(mockProvider);
            registry = CompressorRegistry.instance;
            assertThatExceptionOfType(ConfigurationException.class)
            .isThrownBy(() -> registry.getServiceProvider(new ParameterizedClass("TestCompressionProvider", params)))
            .withMessageContaining("Failed to initialize compression provider");
        }
    }

    @Test
    public void testCustomCompressor()
    {
        // This tests a custom compressor, here provider is not applicable
        Config config = originalConfig;
        Map<String, String> params = new HashMap<>();
        config.compression_provider_options.put("TestCompressor", new ParameterizedClass("TestCompressionProvider", params));
        DatabaseDescriptor.setConfig(config);
        DatabaseDescriptor.applyCompressionProvider();
        registry = CompressorRegistry.instance;
        AbstractCompressionProvider provider = spy(registry.getProvider("TestCompressor"));
        assertThat(provider).isNotNull();
        assertThat(provider.getProviderSimpleName()).isEqualTo("DefaultCompressionProvider");
    }
}
