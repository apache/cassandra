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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.ParameterizedClass;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.FBUtilities;

/**
 * CompressorRegistry manages the registration and retrieval of compression providers
 * which provide access to compressor implementations used for compressing data.
 *
 * This class provides methods to register and retrieve compressorion providers by name of algorithm
 */
public class CompressorRegistry
{
    private static final Logger logger = LoggerFactory.getLogger(CompressorRegistry.class);

    public static final CompressorRegistry instance = new CompressorRegistry();
    private static final String DEFAULT_PROVIDER_NAME = DefaultCompressionProvider.class.getName() ;
    private static final Map<String, String> DEFAULT_PARAMS = Collections.emptyMap();

    private final Map<String, AbstractCompressionProvider> compressionProviders = new ConcurrentHashMap<>();

    /**
     * Returns the compression provider for the given name.
     * @param name the name of the compressor
     * @return the compression provider instance, if not found will attempt to create and return a default provider
     */
    public AbstractCompressionProvider get(String name)
    {
        if(compressionProviders.get(name) == null)
        {
            AbstractCompressionProvider  provider = new DefaultCompressionProvider(DEFAULT_PARAMS);
            register(name, provider);
            return provider;
        }
        return compressionProviders.get(name);
    }

    /**
     * Registers a compression provider for the given compressor name.
     * Fully qualified name of the compressor class will be saved in the provider for use in metadata and
     * other places where compressor name is used.
     * @param name the name of the compressor
     * @param provider the compressor provider instance to register
     */
    private void register(String name, AbstractCompressionProvider provider)
    {
        String algorithmClassName = name;
        if (!name.contains("."))
            algorithmClassName = "org.apache.cassandra.io.compress." + name;
        provider.setAlgorithmName(algorithmClassName);
        compressionProviders.put(name, provider);

    }

    /**
     * Populates the compressionProvider hashmap with providers specified in the configuration.
     * Should be called during initialization to ensure providers
     * specified in the configuration are registered and available for use.
     *
     * @throws ConfigurationException if a provider specified in the configuration fails to initialize and fallback is not enabled or also fails to initialize
     */
    public void registerServices()
    {
        Map<String, ParameterizedClass> providerOptions = DatabaseDescriptor.getCompressionProviderConfigurations();
        if (providerOptions == null) return;
        providerOptions.forEach((algorithmName, providerConfig) -> {
            AbstractCompressionProvider  provider = getServiceProvider(providerConfig);
            try
            {
                if (provider != null)
                {
                    register(algorithmName, provider);
                    logger.info("Adding '{}' for '{}'", provider.getProviderName(), algorithmName);
                }
            }
            catch (Exception e)
            {
                logger.warn("Failed to load service '{}' for '{}'", provider.getProviderName(), algorithmName);
            }
            });
    }

    /**
     * Returns a compressionProvider with configuration specified in the config file.
     * If provider fails to initialize or is not healthy, will attempt to fallback to default provider
     * if enabled in the configuration
     */
    AbstractCompressionProvider getServiceProvider(ParameterizedClass providerConfig)
    {
        AbstractCompressionProvider compressionProvider;
        String className;
        Map<String, String> params;
        className = (providerConfig.class_name != null) ? providerConfig.class_name : DEFAULT_PROVIDER_NAME;
        params = (providerConfig.parameters != null) ? new HashMap<>(providerConfig.parameters): DEFAULT_PARAMS;
        params.putIfAbsent(AbstractCompressionProvider.FALLBACK_TO_DEFAULT_PROVIDER, "true");
        try
        {
            compressionProvider = FBUtilities.newCompressionProvider(className, params);
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
        String fallbackToDefault = params.get(AbstractCompressionProvider.FALLBACK_TO_DEFAULT_PROVIDER);
        if("true".equals(fallbackToDefault))
        {
            try
            {

                return new DefaultCompressionProvider(params);
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
