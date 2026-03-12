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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * CompressorRegistry manages the registration and retrieval of compression providers.
 * Compression providers supply compressor implementations used for compressing data.
 */
public class CompressorRegistry
{
    private static final Logger logger = LoggerFactory.getLogger(CompressorRegistry.class);
    /** The fully qualified class name of the default compression provider. */
    private static final String DEFAULT_PROVIDER_NAME = DefaultCompressionProvider.class.getName();
    /** Configuration key for enabling fallback to the default provider. */
    public static final String FALLBACK_TO_DEFAULT_PROVIDER = "fallback_to_default_provider";
    /** Singleton instance of the registry. */
    public static final CompressorRegistry instance = new CompressorRegistry();

    /**
     * Enum representing in-built supported compressor types and their abbreviations.
     */
    private enum CompressorType
    {
        DEFLATE("DeflateCompressor","deflate"),
        LZ4("LZ4Compressor","lz4"),
        NOOP("NoopCompressor", "noop"),
        SNAPPY("SnappyCompressor","snappy"),
        ZSTD("ZstdCompressor","zstd"),
        ZSTD_DICTIONARY("ZstdDictionaryCompressor","zstd_dictionary");

        private final String compressorName;
        private final String abbreviation;

        CompressorType(String compressorName, String abbreviation)
        {
            this.compressorName = compressorName;
            this.abbreviation = abbreviation;
        }
        /**
         * @return the class name of the compressor type
         */
        public String getCompressorName()
        {
            return this.compressorName;
        }
        /**
         * @return the abbreviation of the compressor type
         */
        public String getAbbreviation()
        {
            return this.abbreviation;
        }
    }

    /** Map of compressor names to their registered providers. */
    private final Map<String, AbstractCompressionProvider> compressionProviders = new ConcurrentHashMap<>();

    /** Map of service provider compressor name to the fully qualified name of in-built compressor
     * for which it is providing the service.
     * For in-built compressors the map entries would look something like this
     * LZ4Compressor -> org.apache.cassandra.io.compress.LZ4Compressor
     *
     * If a service provider, TestProvider, is available, for LZ4Compressor
     * entry would be something like this
     * TestProvider -> org.apache.cassandra.io.compress.LZ4Compressor
     */
    private final Map<String, String> compressorClassNames = new ConcurrentHashMap<>();

    /**
     * Maps a provider compressor name to an in-built compressor name.
     * @param providerCompressorName the name of the compressor from the service provider
     * @param baseCompressorName the name of the in-built compressor that the service provider is providing the service for
     */
    public void mapProviderInstanceToCompressor(String providerCompressorName, String baseCompressorName)
    {
        compressorClassNames.put(providerCompressorName, baseCompressorName);
    }

    /**
     * Returns the fully qualified class name for the given compressor name.
     * Ensures backward compatibility by adding default mappings if needed.
     * @param name the compressor name
     * @return the fully qualified class name
     */
    public String getCompressorTypeFullName(String name)
    {
        String typeName = compressorClassNames.get(name);
        // Sometimes CompressionParams constructor is called directly without calling createCompressor
        // and in that case the compressorClassNames will not have the mapping for the compressor name.
        // In those cases we will assume that the compressor is one of the in-built compressors and create a default mapping
        if(typeName == null)
        {
            typeName = "org.apache.cassandra.io.compress." + name;
            compressorClassNames.put(name, typeName);
        }
        return typeName;
    }

    /**
     * Returns the simple class name of the compressor mapped to the service provider compressor
     * @param name the compressor name
     * @return the simple class name
     */
    public String getCompressorTypeSimpleName(String name)
    {
        String typeName = getCompressorTypeFullName(name);
        int lastDotPos = typeName.lastIndexOf('.');
        return lastDotPos >= 0 ? typeName.substring(lastDotPos + 1) : typeName;
    }

    /**
     * Returns the compression provider for the given compressor name.
     * If not found, attempts to create and register a default provider.
     * @param name simple class name of the compressor
     * @return the compression provider instance
     */
    public AbstractCompressionProvider getProvider(String name)
    {
        AbstractCompressionProvider provider = compressionProviders.get(name);
        if(provider == null)
        {
            provider = new DefaultCompressionProvider();
            register(name, provider);
        }
        return provider;
    }

    /**
     * Registers a compression provider for the given compressor name.
     * The fully qualified name of the compressor class is also saved for use in metadata and other places.
     * @param name the name of the compressor
     * @param provider the compression provider instance to register
     */
    private void register(String name, AbstractCompressionProvider provider)
    {
        compressionProviders.put(name, provider);

        String algorithmClassName = name;
        if (!name.contains("."))
            algorithmClassName = "org.apache.cassandra.io.compress." + name;
        compressorClassNames.put(name, algorithmClassName);
    }

    /**
     * Populates the registry with compression providers specified in the configuration.
     * Should be called during initialization to ensure providers are registered and available for use.
     * If a provider fails to initialize and fallback is enabled, the default provider is used.
     *
     * @param providerOptions map of compressor names to their configuration
     * @throws ConfigurationException if a provider fails to initialize and fallback is not enabled
     */
    public void registerServices(Map<String, ParameterizedClass> providerOptions)
    {
        if (providerOptions == null) return;
        for(CompressorType type : CompressorType.values())
        {
            String algorithmName = type.getCompressorName();
            ParameterizedClass providerConfig = providerOptions.get(algorithmName);
            if (providerConfig != null)
            {
                try
                {
                    AbstractCompressionProvider provider = getServiceProvider(providerConfig);
                    if (provider != null)
                    {
                        register(algorithmName, provider);
                        logger.info("Adding '{}' for '{}'", provider.getProviderName(), algorithmName);
                    }
                }
                catch (Exception e)
                {
                    logger.warn("Failed to load service for '{}'", algorithmName);
                }
            }
            else
            {
                AbstractCompressionProvider provider = new DefaultCompressionProvider();
                register(algorithmName, provider);
            }
        }
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
    AbstractCompressionProvider getServiceProvider(ParameterizedClass providerConfig)
    {
        AbstractCompressionProvider compressionProvider;
        String className;
        className = (providerConfig.class_name != null) ? providerConfig.class_name : DEFAULT_PROVIDER_NAME;
        try
        {
            compressionProvider = FBUtilities.newCompressionProvider(className);
            if(compressionProvider.isHealthy())
            {
                return compressionProvider;
            }
            else
            {
                logger.warn("Compression provider {} is not healthy, attempting fallback", className);
            }
        }
        catch (Exception e)
        {
            logger.warn(String.format(
            "Failed to initialize specified compression provider %s: %s. Will attempt fallback to default if enabled.",
            providerConfig.class_name,
            e.getMessage()
            ));

        }
        String fallbackToDefault = providerConfig.parameters.getOrDefault(FALLBACK_TO_DEFAULT_PROVIDER, "true");

        if("true".equals(fallbackToDefault))
        {
            try
            {
                return new DefaultCompressionProvider();
            }
            catch (Exception e)
            {
                throw new ConfigurationException(String.format(
                "Failed to initialize both specified compression provider %s and default fallback: %s",
                providerConfig.class_name,
                e.getMessage()
                ));
            }
        }
        else
        {
            throw new ConfigurationException(String.format("Failed to initialize compression provider %s", className));
        }
    }
}
