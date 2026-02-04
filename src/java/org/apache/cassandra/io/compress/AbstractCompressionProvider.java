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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract base class for compression providers.
 * Provides common functionality for loading and managing compression plugins
 * with configurable fallback behavior.
 */
public abstract class AbstractCompressionProvider
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractCompressionProvider.class);
    public static final String FALLBACK_TO_DEFAULT_PROVIDER = "fallback_to_default_provider";

    protected final boolean fallbackToDefaultProvider;
    private final Map<String, String> properties;

    public AbstractCompressionProvider(Map<String, String> args)
    {
        this.properties = args == null ? new HashMap<>() : args;
        this.fallbackToDefaultProvider = Boolean.parseBoolean(this.properties.getOrDefault(FALLBACK_TO_DEFAULT_PROVIDER, "true"));
    }

    /**
     * Returns an unmodifiable view of the configuration properties.
     *
     * @return Immutable map of configuration properties
     */
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(properties);
    }

    /**
     * Returns the fully qualified class name of the compression provider.
     *
     * @return Fully qualified name of the provider
     */
    public abstract String getProviderName();

    /**
     * Returns the simple class name of the compression provider.
     * 
     * @return Class name of the provider
     */
    public abstract String getProviderSimpleName();
   
    /**
     * Checks if this compression provider is in a healthy state and ready to use.
     *
     * <p>This method should perform any necessary health checks, such as verifying
     * that required native libraries are loaded, able to do compress/decompress successfully, etc.</p>
     *
     * @return true if the provider is healthy and ready to use, false otherwise
     * @throws Exception if an error occurs during the health check
     */
    public abstract boolean isHealthy() throws Exception;

    /**
     * Creates a new compressor instance with the given compression parameters.
     * 
     * <p>This method is called when a new compressor is needed. The implementation
     * should create and configure a compressor based on the provided options.</p>
     * 
     * @param options Configuration options for the compressor. May be null or empty.
     * @return A new ICompressor instance
     * @throws IllegalStateException if the compressor cannot be created
     */
    public abstract ICompressor createCompressor(Map<String, String> options) throws IllegalStateException;

    /**
     * Returns the name of the compressor supported by this provider.
     *
     * <p>This should match the simple class name of the compressor that this
     * provider creates (e.g., "DeflateCompressor", "ZstdCompressor").</p>
     *
     * @return The name of the supported compressor, or empty string if none
     */
    public abstract String getSupportedCompressorName();

    /**
     * Gets an appropriate compressor, either from the provider specified in the yaml file or the base compressor.
     * 
     * <p>This method implements the main logic for compressor selection:</p>
     *   If this is the default provider, return the base compressor
     *   If this is plugin provider and it supports the base compressor type, try to create our compressor
     *   If creation fails and fallback is enabled, return the base compressor
     *   If creation fails and fallback is disabled, throw an exception
     * 
     * @param baseCompressor The fallback compressor to use if this provider fails
     * @param options Configuration options for compressor creation
     * @return An ICompressor instance, either newly created or the base compressor
     * @throws IllegalStateException if compressor creation fails and fallback is disabled
     */
    public ICompressor getCompressor(ICompressor baseCompressor, Map<String, String> options)
    {
        if (getProviderName().equals(DefaultCompressionProvider.class.getName()))
            return baseCompressor;
        try
        {
            if ((!getSupportedCompressorName().isEmpty()) && baseCompressor.getClass().getSimpleName().equals(getSupportedCompressorName()))
                return createCompressor(options);
        }
        catch (IllegalStateException e)
        {
		    logger.warn("{} failed to create compressor: {}", getProviderName(), e.getMessage());
        }
        if (fallbackToDefaultProvider)
            return baseCompressor;
        throw new IllegalStateException(String.format(
            "Failed to create compressor for provider %s. Fallback to default provider is disabled.",
            getProviderSimpleName()));
    }
}
