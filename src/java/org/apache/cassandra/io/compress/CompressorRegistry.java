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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.io.compress.AbstractCompressionProvider.FALLBACK_TO_DEFAULT_PROVIDER;

/**
 * CompressorRegistry manages the registration and retrieval of compression providers.
 * Compression providers supply compressor implementations used for compressing data.
 */
public final class CompressorRegistry
{
    private static final Logger logger = LoggerFactory.getLogger(CompressorRegistry.class);

    @VisibleForTesting
    public static final AbstractCompressionProvider DEFAULT_COMPRESSION_PROVIDER = new DefaultCompressionProvider();

    /**
     * Map of compressor classes to their registered providers.
     */
    private static final Map<Class<?>, AbstractCompressionProvider> compressionProviders = new HashMap<>();

    /**
     * Singleton instance of the registry.
     */
    public static final CompressorRegistry instance = new CompressorRegistry();

    private CompressorRegistry()
    {
    }

    @VisibleForTesting
    public void reset()
    {
        compressionProviders.clear();
    }

    /**
     * Enum representing in-built supported compressor types and their abbreviations.
     */
    public enum CompressorType
    {
        DEFLATE(DeflateCompressor.class, "deflate"),
        LZ4(LZ4Compressor.class, "lz4"),
        NOOP(NoopCompressor.class, "noop"),
        SNAPPY(SnappyCompressor.class, "snappy"),
        ZSTD(ZstdCompressor.class, "zstd"),
        ZSTD_DICTIONARY(ZstdDictionaryCompressor.class, "zstd_dictionary");

        public final Class<?> compressorClass;
        public final String abbreviation;

        CompressorType(Class<?> compressorClass, String abbreviation)
        {
            this.compressorClass = compressorClass;
            this.abbreviation = abbreviation;
        }
    }

    /**
     * Returns the compression provider for the given compressor class. If the class is not one of
     * the built-in {@link CompressorType} entries (e.g. a 3rd-party / user-supplied compressor not
     * configured in {@code compressor_providers}), returns {@link #DEFAULT_COMPRESSION_PROVIDER},
     * which reflectively invokes the class's static {@code create(Map)} factory.
     *
     * @param compressorClass class of the compressor
     * @return the compression provider instance, never null
     */
    public AbstractCompressionProvider getProvider(Class<?> compressorClass)
    {
        AbstractCompressionProvider provider = compressionProviders.get(compressorClass);
        return provider == null ? DEFAULT_COMPRESSION_PROVIDER : provider;
    }

    /**
     * Populates the registry with compression providers specified in the configuration.
     * Should be called during initialization to ensure providers are registered and available for use.
     * If a provider fails to initialize and fallback is enabled, the default provider is used.
     *
     * @param providerOptions map of compressor names to their configuration
     * @throws ConfigurationException if a provider fails to initialize and fallback is not enabled
     */
    public void registerProviders(Map<String, ParameterizedClass> providerOptions)
    {
        if (providerOptions == null)
            return;

        Set<String> validKeys = new HashSet<>();
        for (CompressorType type : CompressorType.values())
        {
            validKeys.add(type.compressorClass.getName());
            validKeys.add(type.compressorClass.getSimpleName());
            validKeys.add(type.abbreviation);
        }
        for (String key : providerOptions.keySet())
        {
            if (!validKeys.contains(key))
                throw new ConfigurationException("Unknown compressor key '" + key + "' in compressor_providers. " +
                                                 "Expected a built-in compressor's fully qualified class name, simple class name, " +
                                                 "or abbreviation: " + validKeys);
        }

        for (CompressorType type : CompressorType.values())
        {
            ParameterizedClass providerConfig = findProviderConfig(providerOptions, type);
            if (providerConfig == null)
            {
                compressionProviders.put(type.compressorClass, DEFAULT_COMPRESSION_PROVIDER);
            }
            else
            {
                AbstractCompressionProvider provider = resolveProvider(providerConfig);
                compressionProviders.put(type.compressorClass, provider);
                logger.info("Adding '{}' provider for '{}'", provider.getClass().getName(), type.compressorClass.getName());
            }
        }
    }

    private ParameterizedClass findProviderConfig(Map<String, ParameterizedClass> providerOptions, CompressorType type)
    {
        ParameterizedClass parameterizedClass = providerOptions.get(type.compressorClass.getName());

        if (parameterizedClass == null)
            parameterizedClass = providerOptions.get(type.compressorClass.getSimpleName());

        if (parameterizedClass == null)
            parameterizedClass = providerOptions.get(type.abbreviation);

        return parameterizedClass;
    }

    /**
     * Returns a compression provider with configuration specified in the config file.
     * If the provider fails to initialize or is not healthy, will attempt to fall back to the default provider
     * if enabled in the configuration.
     *
     * @param providerConfig the configuration for the provider
     * @return the compression provider instance
     * @throws ConfigurationException if both the specified and fallback providers fail to initialize
     */
    AbstractCompressionProvider resolveProvider(ParameterizedClass providerConfig)
    {
        if (providerConfig.class_name == null)
            throw new ConfigurationException("compressor_providers entry is missing required 'class_name': " + providerConfig);

        Map<String, String> p = providerConfig.parameters == null ? Collections.emptyMap() : providerConfig.parameters;

        try
        {
            AbstractCompressionProvider compressionProvider = FBUtilities.newCompressionProvider(providerConfig.class_name);

            // Strip the registry-reserved key before handing parameters to the plugin, so it sees only its own configuration.
            Map<String, String> pluginParameters = new HashMap<>(p);
            pluginParameters.remove(FALLBACK_TO_DEFAULT_PROVIDER);

            compressionProvider.init(pluginParameters);

            if (compressionProvider.isHealthy())
            {
                return compressionProvider;
            }
            else
            {
                logger.warn("Compression provider {} is not healthy, attempting fallback.", providerConfig.class_name);
            }
        }
        catch (Exception e)
        {
            logger.warn("Failed to initialize specified compression provider {}. Will attempt fallback to default if enabled.",
                        providerConfig.class_name,
                        e);
        }

        boolean fallbackToDefault = Boolean.parseBoolean(p.getOrDefault(FALLBACK_TO_DEFAULT_PROVIDER, Boolean.TRUE.toString()));

        if (fallbackToDefault)
            return DEFAULT_COMPRESSION_PROVIDER;

        throw new ConfigurationException("Failed to initialize compression provider " + providerConfig);
    }
}
