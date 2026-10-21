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

import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.CompressionProviderHelper.SerializedTestCompressor;
import org.apache.cassandra.utils.CompressionProviderHelper.TestCompressionProvider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatExceptionOfType;

public class CompressorRegistryTest
{
    private Config config;

    @Before
    public void setUp()
    {
        config = DatabaseDescriptor.loadConfig();
    }

    @After
    public void tearDown()
    {
        CompressorRegistry.instance.reset();
        config.compressor_providers.clear();
        DatabaseDescriptor.setConfig(config);
    }

    @Test
    public void testRegisterAndGetInbuiltCompressor()
    {
        DatabaseDescriptor.setConfig(config);
        DatabaseDescriptor.applyCompressorProviders();

        for (CompressorRegistry.CompressorType type : CompressorRegistry.CompressorType.values())
        {
            AbstractCompressionProvider provider = CompressorRegistry.instance.getProvider(type.compressorClass());
            assertThat(provider).isNotNull();
            assertThat(provider).isEqualTo(CompressorRegistry.DEFAULT_COMPRESSION_PROVIDER);
        }
    }

    @Test
    public void testRegisterServiceProvider() throws Exception
    {
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.FALSE.toString());
        config.compressor_providers.put(NoopCompressor.class.getSimpleName(), new ParameterizedClass(TestCompressionProvider.class.getName(), params));

        DatabaseDescriptor.setConfig(config);
        DatabaseDescriptor.applyCompressorProviders();

        AbstractCompressionProvider provider = CompressorRegistry.instance.getProvider(NoopCompressor.class);
        assertThat(provider).isNotNull();
        assertThat(provider.isHealthy()).isTrue();
        assertThat(provider.getClass()).isEqualTo(TestCompressionProvider.class);
    }

    @Test
    public void testProviderConfigKeyVariants()
    {
        // The registry resolves a compressor type to its plugin via three accepted key forms in
        // compressor_providers: fully qualified class name, simple class name, and the
        // CompressorType abbreviation. Verify each form independently routes to the plugin.
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.FALSE.toString());
        String[] keys = {
        NoopCompressor.class.getName(),                                  // FQCN
        NoopCompressor.class.getSimpleName(),                            // simple name
        CompressorRegistry.CompressorType.NOOP.abbreviation              // abbreviation
        };

        for (String key : keys)
        {
            CompressorRegistry.instance.reset();
            config.compressor_providers.clear();
            config.compressor_providers.put(key, new ParameterizedClass(TestCompressionProvider.class.getName(), params));

            DatabaseDescriptor.setConfig(config);
            DatabaseDescriptor.applyCompressorProviders();

            AbstractCompressionProvider provider = CompressorRegistry.instance.getProvider(NoopCompressor.class);
            assertThat(provider).as("provider resolved via key '%s'", key).isNotNull();
            assertThat(provider.getClass()).as("provider class resolved via key '%s'", key)
                                           .isEqualTo(TestCompressionProvider.class);
        }
    }

    @Test
    public void testUnhealthyProviderWithFallback()
    {
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.FALSE.toString(),
                                            TestCompressionProvider.FAIL_HEALTH, "true");
        ParameterizedClass parameterizedClass = new ParameterizedClass(TestCompressionProvider.class.getName(), params);

        AbstractCompressionProvider provider = CompressorRegistry.instance.resolveProvider(parameterizedClass);
        assertThat(provider).isNotNull();
        assertThat(provider.getClass()).isEqualTo(DefaultCompressionProvider.class);
    }

    @Test
    public void testUnhealthyProviderWithoutFallback()
    {
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString(),
                                            TestCompressionProvider.FAIL_HEALTH, "true");
        ParameterizedClass parameterizedClass = new ParameterizedClass(TestCompressionProvider.class.getName(), params);

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(() -> CompressorRegistry.instance.resolveProvider(parameterizedClass))
        .withMessageContaining("Failed to initialize compression provider " + parameterizedClass);
    }

    @Test
    public void testProviderHealthCheckExceptionWithFallback()
    {
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.FALSE.toString(),
                                            TestCompressionProvider.FAIL_HEALTH_EXCEPTION, "true");
        ParameterizedClass parameterizedClass = new ParameterizedClass(TestCompressionProvider.class.getName(), params);

        AbstractCompressionProvider provider = CompressorRegistry.instance.resolveProvider(parameterizedClass);
        assertThat(provider).isNotNull();
        assertThat(provider.getClass()).isEqualTo(DefaultCompressionProvider.class);
    }

    @Test
    public void testProviderHealthCheckExceptionWithoutFallbackThrows()
    {
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString(),
                                            TestCompressionProvider.FAIL_HEALTH_EXCEPTION, "true");
        ParameterizedClass parameterizedClass = new ParameterizedClass(TestCompressionProvider.class.getName(), params);

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(() -> CompressorRegistry.instance.resolveProvider(parameterizedClass))
        .withMessageContaining("Failed to initialize compression provider " + parameterizedClass);
    }

    @Test
    public void testProviderInstantiationFailureFallsBackToDefault()
    {
        // FBUtilities.newCompressionProvider throws ConfigurationException for an unknown class —
        // this exercises the catch block in resolveProvider, distinct from the isHealthy()==false
        // path. With fallback enabled, the default provider must be returned and the original
        // exception's cause chain should be available in the warn log (not just its message).
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.FALSE.toString());
        ParameterizedClass parameterizedClass = new ParameterizedClass("org.apache.cassandra.does.not.Exist", params);

        AbstractCompressionProvider provider = CompressorRegistry.instance.resolveProvider(parameterizedClass);
        assertThat(provider).isEqualTo(CompressorRegistry.DEFAULT_COMPRESSION_PROVIDER);
    }

    @Test
    public void testNullParametersDoesNotNPE()
    {
        // ParameterizedClass.parameters is nullable: SnakeYAML uses the no-arg constructor and
        // leaves the field null when the yaml entry has only 'class_name:' (no 'parameters:'
        // block). resolveProvider must tolerate this rather than NPE on getOrDefault.
        ParameterizedClass pc = new ParameterizedClass();
        pc.class_name = "org.apache.cassandra.does.not.Exist";
        pc.parameters = new HashMap<>();
        pc.parameters.put(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.FALSE.toString());

        AbstractCompressionProvider provider = CompressorRegistry.instance.resolveProvider(pc);
        assertThat(provider).isEqualTo(CompressorRegistry.DEFAULT_COMPRESSION_PROVIDER);
    }

    @Test
    public void testMissingClassNameThrows()
    {
        // A compressor_providers entry with no 'class_name' is a config mistake and must fail
        // loudly — silently substituting the default would mask typos like 'clas_name:'.
        ParameterizedClass parameterizedClass = new ParameterizedClass(null,
                                                                       Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.FALSE.toString()));

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(() -> CompressorRegistry.instance.resolveProvider(parameterizedClass))
        .withMessageContaining("missing required 'class_name'");
    }

    @Test
    public void testProviderInstantiationFailureWithoutFallbackThrows()
    {
        // Same catch block as above, but with fallback disabled — the resolver must surface a
        // ConfigurationException rather than silently use the default.
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString());
        ParameterizedClass parameterizedClass = new ParameterizedClass("org.apache.cassandra.does.not.Exist", params);

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(() -> CompressorRegistry.instance.resolveProvider(parameterizedClass))
        .withMessageContaining("Failed to initialize compression provider " + parameterizedClass);
    }

    @Test
    public void testInitFailureWithFallback()
    {
        // An exception during provider.init() will be treated the same as an unhealthy provider:
        // if fallback is enabled, the default provider will be used
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.FALSE.toString(),
                                            TestCompressionProvider.FAIL_INIT, "true");
        ParameterizedClass parameterizedClass = new ParameterizedClass(TestCompressionProvider.class.getName(), params);

        AbstractCompressionProvider provider = CompressorRegistry.instance.resolveProvider(parameterizedClass);
        assertThat(provider).isNotNull();
        assertThat(provider.getClass()).isEqualTo(DefaultCompressionProvider.class);
    }

    @Test
    public void testInitFailureWithoutFallbackThrows()
    {
        // If provider.init() fails and if fallback is disabled, an exception will be thrown
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString(),
                                            TestCompressionProvider.FAIL_INIT, "true");
        ParameterizedClass parameterizedClass = new ParameterizedClass(TestCompressionProvider.class.getName(), params);

        assertThatExceptionOfType(ConfigurationException.class)
        .isThrownBy(() -> CompressorRegistry.instance.resolveProvider(parameterizedClass))
        .withMessageContaining("Failed to initialize compression provider " + parameterizedClass);
    }

    @Test
    public void testCreateCompressorFailureWithFallback()
    {
        // This tests a scenario when provider fails to get a compressor,
        // if fallback is enabled, the registry should still return a compressor from the default provider instead of failing the request.
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.FALSE.toString(),
                                            TestCompressionProvider.FAIL_CREATE, "true");
        config.compressor_providers.put(SnappyCompressor.class.getSimpleName(), new ParameterizedClass(TestCompressionProvider.class.getName(), params));

        DatabaseDescriptor.setConfig(config);
        DatabaseDescriptor.applyCompressorProviders();
        ICompressor compressor = CompressorRegistry.instance.getCompressor(SnappyCompressor.class, Map.of());
        assertThat(compressor).isInstanceOf(SnappyCompressor.class);
    }

    @Test
    public void testCreateCompressorFailureWithoutFallbackThrows()
    {
        // if fallback is disabled and provider fails to get a compressor, the registry should throw an exception
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString(),
                                            TestCompressionProvider.FAIL_CREATE, "true");
        ParameterizedClass parameterizedClass = new ParameterizedClass(TestCompressionProvider.class.getName(), params);
        config.compressor_providers.put(SnappyCompressor.class.getSimpleName(), parameterizedClass);

        DatabaseDescriptor.setConfig(config);
        DatabaseDescriptor.applyCompressorProviders();

        assertThatExceptionOfType(IllegalStateException.class)
        .isThrownBy(() -> CompressorRegistry.instance.getCompressor(SnappyCompressor.class, Map.of()))
        .withMessageContaining("compressor instantiation failed");
    }

    @Test
    public void testCustomProviderAndCompressorWithSerializedAs() throws Exception
    {
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.FALSE.toString());
        config.compressor_providers.put(SnappyCompressor.class.getSimpleName(), new ParameterizedClass(TestCompressionProvider.class.getName(), params));

        DatabaseDescriptor.setConfig(config);
        DatabaseDescriptor.applyCompressorProviders();

        AbstractCompressionProvider provider = CompressorRegistry.instance.getProvider(SnappyCompressor.class);
        assertThat(provider).isNotNull();
        assertThat(provider.isHealthy()).isTrue();
        ICompressor compressor = CompressorRegistry.instance.getCompressor(SnappyCompressor.class, Map.of());
        assertThat(compressor.getClass()).isEqualTo(SerializedTestCompressor.class);
    }

    @Test
    public void testCustomProviderAndCompressorWithoutSerializedAs() throws Exception
    {
        Map<String, String> params = Map.of(AbstractCompressionProvider.FAIL_ON_MISSING_PROVIDER, Boolean.TRUE.toString(),
                                            TestCompressionProvider.FAIL_SERIALIZED_AS, "true");
        config.compressor_providers.put(SnappyCompressor.class.getSimpleName(), new ParameterizedClass(TestCompressionProvider.class.getName(), params));

        DatabaseDescriptor.setConfig(config);
        DatabaseDescriptor.applyCompressorProviders();

        AbstractCompressionProvider provider = CompressorRegistry.instance.getProvider(SnappyCompressor.class);
        assertThat(provider).isNotNull();
        assertThat(provider.isHealthy()).isTrue();
        assertThatExceptionOfType(RuntimeException.class)
        .isThrownBy(() -> CompressorRegistry.instance.getCompressor(SnappyCompressor.class, Map.of()))
        .withMessageContaining("You need to override serializedAs() method of your custom compressor and return " +
                               "base compressor class it is the substitute for.");
    }

}
