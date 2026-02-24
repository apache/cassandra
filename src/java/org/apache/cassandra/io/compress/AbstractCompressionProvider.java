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
    private String algorithmName;

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
    public abstract ICompressor createCompressor(Class<?> compressorClass, Map<String, String> options) throws IllegalStateException;

    /**
     * Returns the fully qualified class name of the compressor algorithm.
     * CompressorRegistry holds mapping between compressor name and provider, and compressor name is stored in the provider
     * for reverse lookup, to figure out the in-built compressor the provider supports when a provider is available
     *
     * @return the compressor algorithm class name, or null if not set
     */
    public String getAlgorithmName()
    {
        return algorithmName;
    }

    /**
     * Returns the simple class name of the compressor algorithm.
     *
     * @return the simple class name of the compressor algorithm, or null if not found
     */
    public String getAlgorithmSimpleName()
    {
        try
        {
            Class<?> klass = Class.forName(algorithmName);
            return klass.getSimpleName();
        }
        catch (ClassNotFoundException e)
        {
            e.printStackTrace();
        }
        //It shouldn't get here since algorithmName should be set to a valid compressor class, but return null just in case
        return null;
    }

    /**
     * Sets the fully qualified class name of the compressor algorithm.
     *
     * @param name the fully qualified class name to set
     */
    public void setAlgorithmName(String name)
    {
        algorithmName = name;
    }
}
